from data_quality.scd2_reference import merge_history


TRACKED = ("name", "city")


def test_initial_load_creates_current_versions():
    history = merge_history(
        [],
        [{"customer_id": "101", "name": "Acme", "city": "Phoenix"}],
        key="customer_id",
        tracked=TRACKED,
        effective_at="2026-01-01T00:00:00Z",
    )
    assert len(history) == 1
    assert history[0]["is_current"] is True
    assert history[0]["effective_to"] is None


def test_unchanged_replay_is_idempotent():
    source = [{"customer_id": "101", "name": "Acme", "city": "Phoenix"}]
    first = merge_history(
        [], source, key="customer_id", tracked=TRACKED, effective_at="2026-01-01T00:00:00Z"
    )
    replay = merge_history(
        first, source, key="customer_id", tracked=TRACKED, effective_at="2026-02-01T00:00:00Z"
    )
    assert replay == first


def test_change_expires_old_version_and_inserts_new_current_version():
    first = merge_history(
        [],
        [{"customer_id": "101", "name": "Acme", "city": "Phoenix"}],
        key="customer_id",
        tracked=TRACKED,
        effective_at="2026-01-01T00:00:00Z",
    )
    changed = merge_history(
        first,
        [{"customer_id": "101", "name": "Acme", "city": "Tempe"}],
        key="customer_id",
        tracked=TRACKED,
        effective_at="2026-02-01T00:00:00Z",
    )
    assert len(changed) == 2
    assert changed[0]["is_current"] is False
    assert changed[0]["effective_to"] == "2026-02-01T00:00:00Z"
    assert changed[1]["city"] == "Tempe"
    assert changed[1]["is_current"] is True

