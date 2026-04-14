from archive_vault.sync_state import load_sync_state, update_cursor


def test_update_cursor_roundtrips(tmp_vault):
    update_cursor(tmp_vault, "linkedin", {"page": 2})
    assert load_sync_state(tmp_vault)["linkedin"]["page"] == 2
