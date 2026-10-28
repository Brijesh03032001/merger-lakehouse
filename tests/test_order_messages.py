import json

from local_lakehouse.synthetic import ORDER_FIELDS, build_scenario, order_messages


def _scenario():
    return build_scenario(seed=3, days=4, rows_per_day=200)


def test_messages_are_keyed_by_order_id_so_one_order_stays_in_one_partition_in_order():
    for key, value in order_messages(_scenario(), seed=1, malformed_rate=0, extra_field_rate=0):
        assert json.loads(value)["order_id"] == key


def test_clean_messages_carry_exactly_the_five_contract_fields():
    for _, value in order_messages(_scenario(), seed=1, malformed_rate=0, extra_field_rate=0):
        assert set(json.loads(value)) == set(ORDER_FIELDS)


def test_a_share_of_messages_is_unparseable_and_a_share_has_an_unknown_field():
    messages = order_messages(_scenario(), seed=1, malformed_rate=0.05, extra_field_rate=0.1)
    broken = 0
    extra = 0
    for _, value in messages:
        try:
            if "promo_code" in json.loads(value):
                extra += 1
        except ValueError:
            broken += 1
    assert 0.02 < broken / len(messages) < 0.09
    assert 0.05 < extra / len(messages) < 0.15


def test_generation_is_deterministic():
    scenario = _scenario()
    assert order_messages(scenario, seed=5) == order_messages(scenario, seed=5)
    assert order_messages(scenario, seed=5) != order_messages(scenario, seed=6)
