"""Deterministic synthetic FMCG data for tests, demos and benchmarks.

Everything here is generated from a seed. None of it is real company data: the
parent/child overlap, name noise and error rates are injected on purpose so the
pipeline's quality gates and entity matching have something known to find.
"""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

STEMS_A = [
    "Zen", "Prime", "Sprint", "Peak", "Vita", "Pure", "Iron", "Core", "Swift", "Nova",
    "Terra", "Aero", "Pulse", "Flex", "Bolt", "Apex", "Kinetic", "Urban", "Alpha", "Green",
]
STEMS_B = [
    "athlete", "fuel", "x", "labs", "grain", "harvest", "stride", "forge", "leaf", "wave",
    "field", "craft", "spark", "root", "mill", "trail", "peak", "bowl", "farm", "lane",
]
SECTORS = [
    "Nutrition", "Foods", "Sports Foods", "Wellness", "Dairy", "Snacks", "Traders", "Stores",
    "Mart", "Retail", "Health", "Provisions",
]
LEGAL_SUFFIXES = ["Pvt Ltd", "Private Limited", "Pvt. Ltd.", "Ltd", "LLP", "& Co"]
CITIES = ["Bengaluru", "Hyderabad", "New Delhi", "Mumbai", "Chennai", "Pune", "Kolkata", "Ahmedabad"]
CHILD_CITIES = CITIES[:3]
CITY_TYPOS = {
    "Bengaluru": ["Bengaluruu", "Bengalore", "Bangalore"],
    "Hyderabad": ["Hyderabadd", "Hyderbad"],
    "New Delhi": ["NewDelhi", "NewDheli", "NewDelhee"],
}

PRODUCT_KINDS = [
    ("Protein Bar", "Protein Bars", "Nutrition Bars"),
    ("Energy Bar", "Energy Bars", "Nutrition Bars"),
    ("Granola", "Granola & Cereals", "Breakfast Foods"),
    ("Muesli", "Granola & Cereals", "Breakfast Foods"),
    ("Greek Yogurt", "Recovery Dairy", "Dairy & Recovery"),
    ("Recovery Shake", "Recovery Dairy", "Dairy & Recovery"),
    ("Trail Mix", "Healthy Snacks", "Healthy Snacks"),
    ("Oat Cookies", "Healthy Snacks", "Healthy Snacks"),
    ("Electrolyte Mix", "Electrolyte Mix", "Hydration & Electrolytes"),
]
FLAVORS = {
    "Chocolate": "Choco", "Vanilla": "Van", "Berry": "Bery", "Mango": "Mngo", "Peanut": "Pnut",
    "Coffee": "Cofee", "Mint": "Mnt", "Almond": "Almnd", "Honey": "Hny", "Cranberry": "Cranbry",
}

ORDER_DATE_FORMATS = ("%A, %B %d, %Y", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d")
ORDER_FIELDS = ["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"]


@dataclass
class Scenario:
    parent_customers: list[dict] = field(default_factory=list)
    child_customers: list[dict] = field(default_factory=list)
    customer_truth: list[dict] = field(default_factory=list)
    parent_products: list[dict] = field(default_factory=list)
    child_products: list[dict] = field(default_factory=list)
    product_truth: list[dict] = field(default_factory=list)
    gross_price: list[dict] = field(default_factory=list)
    orders: list[dict] = field(default_factory=list)
    parent_fact: list[dict] = field(default_factory=list)  # monthly grain, like gold.fact_orders


def _typo(word: str, rng: random.Random) -> str:
    if len(word) < 4:
        return word
    i = rng.randrange(1, len(word) - 1)
    operation = rng.choice(("delete", "swap", "double"))
    if operation == "delete":
        return word[:i] + word[i + 1:]
    if operation == "swap":
        return word[:i] + word[i + 1] + word[i] + word[i + 2:]
    return word[:i] + word[i] + word[i:]


def _render_customer(entity: dict, style: str) -> str:
    a, b = entity["stems"]
    brand = {"joined": f"{a}{b}", "spaced": f"{a} {b}", "hyphen": f"{a}-{b}"}[style]
    return f"{brand.title()} {entity['sector']}"


def _vary_customer_name(entity: dict, rng: random.Random) -> str:
    """Render the same real-world customer the way a different company's CRM might."""
    a, b = entity["stems"]
    brand = {"joined": f"{a}{b}", "spaced": f"{a} {b}", "hyphen": f"{a}-{b}"}[
        rng.choice(["joined", "spaced", "hyphen"])
    ].title()
    sector, prefix, suffix, joiner, casing = entity["sector"], "", "", " ", None
    operations = rng.sample(
        ["typo", "case", "suffix", "reorder", "drop_sector", "the", "ampersand"],
        k=rng.choice([1, 1, 2]),
    )
    parts_first = "brand"
    for operation in operations:
        if operation == "typo":
            brand = _typo(brand, rng)
        elif operation == "suffix":
            suffix = f" {rng.choice(LEGAL_SUFFIXES)}"
        elif operation == "reorder":
            parts_first = "sector"
        elif operation == "drop_sector":
            sector = ""
        elif operation == "the":
            prefix = "The "
        elif operation == "ampersand":
            joiner = " & "
        elif operation == "case":
            casing = rng.choice([str.upper, str.lower])
    parts = [brand, sector] if parts_first == "brand" else [sector, brand]
    name = prefix + joiner.join(part for part in parts if part) + suffix
    return casing(name) if casing else name


def _new_customer_entities(rng: random.Random, count: int) -> list[dict]:
    seen: set[tuple] = set()
    entities: list[dict] = []
    while len(entities) < count:
        entity = {
            "stems": (rng.choice(STEMS_A), rng.choice(STEMS_B)),
            "sector": rng.choice(SECTORS),
            "city": rng.choice(CITIES),
        }
        key = (entity["stems"], entity["sector"])
        if key not in seen:
            seen.add(key)
            entities.append(entity)
    return entities


def _build_customers(rng: random.Random, scenario: Scenario, parents: int, children: int, overlap: float) -> None:
    entities = _new_customer_entities(rng, parents)
    for number, entity in enumerate(entities):
        suffix = f" {rng.choice(LEGAL_SUFFIXES)}" if rng.random() < 0.3 else ""
        name = _render_customer(entity, "joined") + suffix
        scenario.parent_customers.append({
            "customer_code": str(70000 + number),
            "customer": f"{name}-{entity['city']}",
            "market": "India",
            "platform": rng.choice(["Brick & Mortar", "E-Commerce"]),
            "channel": rng.choice(["Retailer", "Direct", "Acquisition"]),
        })

    overlapping = int(children * overlap)
    hard_negatives = max(1, int(children * 0.1))
    # The acquired startup only operates in CHILD_CITIES, so shared customers live there too.
    footprint = [i for i, entity in enumerate(entities) if entity["city"] in CHILD_CITIES]
    twins = rng.sample(footprint, min(overlapping, len(footprint)))
    child_id = 789400
    for index in twins:
        entity = entities[index]
        # Real duplicates share a city; sometimes one CRM is stale (10%) or misspells it (15%).
        city = rng.choice(CHILD_CITIES) if rng.random() < 0.10 else entity["city"]
        city = rng.choice(CITY_TYPOS[city]) if rng.random() < 0.15 else city
        scenario.child_customers.append(
            {"customer_id": str(child_id), "customer_name": _vary_customer_name(entity, rng), "city": city}
        )
        scenario.customer_truth.append(
            {"child_id": str(child_id), "parent_code": str(70000 + index), "kind": "variant"}
        )
        child_id += 1
    for _ in range(hard_negatives):
        # Same brand, different business: a near-name that must NOT be merged.
        base = entities[rng.randrange(parents)]
        other_sector = rng.choice([s for s in SECTORS if s != base["sector"]])
        impostor = {**base, "sector": other_sector}
        scenario.child_customers.append(
            {"customer_id": str(child_id), "customer_name": _render_customer(impostor, "joined"),
             "city": rng.choice(CHILD_CITIES)}
        )
        scenario.customer_truth.append({"child_id": str(child_id), "parent_code": None, "kind": "hard_negative"})
        child_id += 1
    for entity in _new_customer_entities(rng, children - overlapping - hard_negatives):
        if any(entity["stems"] == e["stems"] and entity["sector"] == e["sector"] for e in entities):
            continue
        scenario.child_customers.append(
            {"customer_id": str(child_id), "customer_name": _render_customer(entity, "joined"),
             "city": rng.choice(CHILD_CITIES)}
        )
        scenario.customer_truth.append({"child_id": str(child_id), "parent_code": None, "kind": "unique"})
        child_id += 1
    rng.shuffle(scenario.child_customers)


def _build_products(rng: random.Random, scenario: Scenario, overlap: float) -> None:
    catalogue = [
        {"kind": kind, "category": category, "division": division, "flavor": flavor}
        for kind, category, division in PRODUCT_KINDS
        for flavor in FLAVORS
    ]
    rng.shuffle(catalogue)
    parent_items, child_only = catalogue[:60], catalogue[60:75]
    for item in parent_items:
        scenario.parent_products.append({
            "product_code": f"P{len(scenario.parent_products):04d}",
            "division": item["division"],
            "category": item["category"],
            "product": f"{item['kind']} ({item['flavor']})",
            "variant": item["flavor"],
        })
    shared = rng.sample(range(len(parent_items)), int(len(parent_items) * overlap * 0.6))
    child_id = 25891000
    for index in shared:
        item = parent_items[index]
        flavor = FLAVORS[item["flavor"]] if rng.random() < 0.5 else item["flavor"]
        kind = item["kind"].replace("Protein", "Protien") if rng.random() < 0.3 else item["kind"]
        name = rng.choice([
            f"{kind} ({flavor})",
            f"{kind} - {flavor}",
            f"{flavor} {kind}",
            f"{kind} ({flavor}) {rng.choice(['40g', '60g', '250g'])}",
        ])
        scenario.child_products.append(
            {"product_id": str(child_id), "product_name": rng.choice([name, name.upper(), name.lower()]),
             "category": rng.choice([item["category"], item["category"].lower()])}
        )
        scenario.product_truth.append(
            {"child_id": str(child_id), "parent_code": scenario.parent_products[index]["product_code"],
             "kind": "variant"}
        )
        child_id += 1
    for item in child_only:
        scenario.child_products.append(
            {"product_id": str(child_id), "product_name": f"{item['kind']} ({item['flavor']}) Family Pack",
             "category": item["category"]}
        )
        scenario.product_truth.append({"child_id": str(child_id), "parent_code": None, "kind": "unique"})
        child_id += 1


def _build_parent_facts(rng: random.Random, scenario: Scenario, start: date, months: int = 5) -> None:
    """Monthly parent-company sales per customer and product, in the shared fact table's shape."""
    product_codes = [p["product_code"] for p in scenario.parent_products]
    for customer in scenario.parent_customers:
        for product_code in rng.sample(product_codes, 5):
            for offset in range(months):
                month = date(start.year + (start.month - 1 + offset) // 12, (start.month - 1 + offset) % 12 + 1, 1)
                scenario.parent_fact.append({
                    "date": month.isoformat(),
                    "product_code": product_code,
                    "customer_code": customer["customer_code"],
                    "sold_quantity": float(rng.randint(10, 400)),
                })


def _build_prices(rng: random.Random, scenario: Scenario, months: int = 24) -> None:
    for product in scenario.child_products:
        price = rng.randint(30, 400)
        for offset in range(months):
            month = date(2024 + offset // 12, offset % 12 + 1, 1)
            price = max(10, price + rng.randint(-4, 8))
            value: str = str(price)
            roll = rng.random()
            if roll < 0.03:
                value = f"-{price}"
            elif roll < 0.06:
                value = "N/A"
            scenario.gross_price.append({
                "product_id": product["product_id"],
                "month": month.strftime(rng.choice(["%Y/%m/%d", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"])),
                "gross_price": value,
            })


def _build_orders(
    rng: random.Random,
    scenario: Scenario,
    days: int,
    rows_per_day: int,
    start: date,
    duplicate_rate: float,
    invalid_customer_rate: float,
    invalid_qty_rate: float,
) -> None:
    customer_ids = [row["customer_id"] for row in scenario.child_customers]
    product_ids = [row["product_id"] for row in scenario.child_products]
    sequence = 0
    for offset in range(days):
        day = start + timedelta(days=offset)
        daily: list[dict] = []
        for _ in range(rows_per_day):
            sequence += 1
            row = {
                "order_id": f"ORD{day:%y%m%d}{sequence:07d}",
                "order_placement_date": day.strftime(rng.choice(ORDER_DATE_FORMATS)),
                "customer_id": rng.choice(customer_ids),
                "product_id": rng.choice(product_ids),
                "order_qty": str(float(rng.randint(1, 120))),
            }
            if rng.random() < invalid_customer_rate:
                row["customer_id"] = rng.choice(["INVALID", "", "CUST-" + str(rng.randint(10, 99))])
            if rng.random() < invalid_qty_rate:
                row["order_qty"] = rng.choice(["", "0", "-5", "N/A"])
            daily.append(row)
        daily.extend(dict(row) for row in rng.sample(daily, int(len(daily) * duplicate_rate)))
        rng.shuffle(daily)
        scenario.orders.append({"day": day, "rows": daily})


def build_scenario(
    seed: int = 7,
    *,
    parent_customers: int = 250,
    child_customers: int = 60,
    overlap: float = 0.6,
    days: int = 151,
    rows_per_day: int = 320,
    start: date = date(2025, 7, 1),
    duplicate_rate: float = 0.07,
    invalid_customer_rate: float = 0.10,
    invalid_qty_rate: float = 0.05,
) -> Scenario:
    rng = random.Random(seed)
    scenario = Scenario()
    _build_customers(rng, scenario, parent_customers, child_customers, overlap)
    _build_products(rng, scenario, overlap)
    _build_parent_facts(rng, scenario, start)
    _build_prices(rng, scenario)
    _build_orders(rng, scenario, days, rows_per_day, start, duplicate_rate, invalid_customer_rate, invalid_qty_rate)
    return scenario


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_landing_zone(scenario: Scenario, root: str | Path) -> Path:
    """Lay the child company out like the S3 / Volume landing zone the notebooks read."""
    root = Path(root)
    _write_csv(root / "customers/customers.csv", ["customer_id", "customer_name", "city"], scenario.child_customers)
    _write_csv(root / "products/products.csv", ["product_id", "product_name", "category"], scenario.child_products)
    _write_csv(root / "gross_price/gross_price.csv", ["product_id", "month", "gross_price"], scenario.gross_price)
    for batch in scenario.orders:
        _write_csv(root / f"orders/landing/orders_{batch['day']:%Y%m%d}.csv", ORDER_FIELDS, batch["rows"])
    (root / "orders/processed").mkdir(parents=True, exist_ok=True)
    _write_csv(
        root / "parent_customers/parent_customers.csv",
        ["customer_code", "customer", "market", "platform", "channel"],
        scenario.parent_customers,
    )
    _write_csv(
        root / "parent_products/parent_products.csv",
        ["product_code", "division", "category", "product", "variant"],
        scenario.parent_products,
    )
    _write_csv(
        root / "parent_fact_orders/parent_fact_orders.csv",
        ["date", "product_code", "customer_code", "sold_quantity"],
        scenario.parent_fact,
    )
    return root


def write_ground_truth(scenario: Scenario, root: str | Path) -> None:
    root = Path(root)
    _write_csv(root / "customer_matches.csv", ["child_id", "parent_code", "kind"], scenario.customer_truth)
    _write_csv(root / "product_matches.csv", ["child_id", "parent_code", "kind"], scenario.product_truth)


def order_messages(
    scenario: Scenario, *, seed: int = 0, malformed_rate: float = 0.01, extra_field_rate: float = 0.05
) -> list[tuple[str, str]]:
    """The scenario's orders as Kafka ``(key, JSON value)`` pairs, in day order.

    The key is the order id, so every message about one order lands in one partition in order.
    A small share of messages is made unparseable (garbage or truncated) and another share gains a
    field the contract does not know, so the streaming path meets both.
    """
    rng = random.Random(seed)
    messages: list[tuple[str, str]] = []
    for batch in scenario.orders:
        for row in batch["rows"]:
            value = {k: row[k] for k in ORDER_FIELDS}
            if rng.random() < extra_field_rate:
                value["promo_code"] = rng.choice(["SUMMER", "WELCOME", "BULK10"])
            text = json.dumps(value)
            roll = rng.random()
            if roll < malformed_rate / 2:
                text = "not json at all"
            elif roll < malformed_rate:
                text = text[: len(text) // 2]  # a truncated message
            messages.append((row["order_id"], text))
    return messages
