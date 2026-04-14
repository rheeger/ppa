import json

from archive_vault.config import PPAConfig, load_config, save_config


def test_load_config_returns_defaults(tmp_vault):
    assert load_config(tmp_vault) == PPAConfig()


def test_load_config_ignores_unknown_keys(tmp_vault):
    (tmp_vault / "_meta" / "ppa-config.json").write_text(
        json.dumps({"merge_threshold": 95, "unknown": "ignored"}),
        encoding="utf-8",
    )
    config = load_config(tmp_vault)
    assert config.merge_threshold == 95
    assert not hasattr(config, "unknown")


def test_save_config_roundtrips(tmp_vault):
    config = PPAConfig(
        merge_threshold=92,
        conflict_threshold=70,
        imessage_thread_body_sha_cache_enabled=False,
        gmail_thread_body_sha_cache_enabled=False,
        calendar_event_body_sha_cache_enabled=False,
        otter_transcript_body_sha_cache_enabled=False,
    )
    save_config(tmp_vault, config)
    loaded = load_config(tmp_vault)
    assert loaded.merge_threshold == 92
    assert loaded.conflict_threshold == 70
    assert loaded.imessage_thread_body_sha_cache_enabled is False
    assert loaded.gmail_thread_body_sha_cache_enabled is False
    assert loaded.calendar_event_body_sha_cache_enabled is False
    assert loaded.otter_transcript_body_sha_cache_enabled is False
