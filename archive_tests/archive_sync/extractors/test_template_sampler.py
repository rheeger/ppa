"""Tests for template-sampler CLI."""

from __future__ import annotations

from archive_cli.commands.template_sampler import run_template_sampler, run_template_sampler_batch
from archive_sync.extractors.preprocessing import clean_email_body
from archive_tests.archive_sync.extractors.conftest import write_email_to_vault


def test_sampler_creates_per_year_directories(extractor_vault, sample_email_card, tmp_path):
    fm, body = sample_email_card(
        "hfa-email-message-sy",
        "a@doordash.com",
        "receipt here",
        "<p>Hello</p>",
        sent_at="2021-03-01T12:00:00-08:00",
    )
    write_email_to_vault(extractor_vault, "Email/sy.md", fm, body)
    out = tmp_path / "samples"
    run_template_sampler(
        vault_path=extractor_vault,
        domain="doordash.com",
        category="receipt",
        per_year=2,
        out_dir=str(out),
    )
    assert (out / "2021").is_dir()


def test_sampler_writes_raw_and_clean_and_meta(extractor_vault, sample_email_card, tmp_path):
    fm, body = sample_email_card(
        "hfa-email-message-uid1",
        "a@doordash.com",
        "receipt order",
        "<html><body>Hi</body></html>",
        sent_at="2022-05-01T12:00:00-08:00",
    )
    write_email_to_vault(extractor_vault, "Email/u1.md", fm, body)
    out = tmp_path / "samples"
    run_template_sampler(
        vault_path=extractor_vault,
        domain="doordash.com",
        category="",
        per_year=3,
        out_dir=str(out),
    )
    ydir = out / "2022"
    assert (ydir / "hfa-email-message-uid1.raw.txt").is_file()
    assert (ydir / "hfa-email-message-uid1.clean.txt").is_file()
    assert (ydir / "hfa-email-message-uid1.meta.json").is_file()


def test_sampler_clean_matches_preprocessing(extractor_vault, sample_email_card, tmp_path):
    raw = "<p>Test line</p>"
    fm, body = sample_email_card(
        "hfa-email-message-uid2",
        "a@doordash.com",
        "subj",
        raw,
        sent_at="2022-05-02T12:00:00-08:00",
    )
    write_email_to_vault(extractor_vault, "Email/u2.md", fm, body)
    out = tmp_path / "samples"
    run_template_sampler(
        vault_path=extractor_vault,
        domain="doordash.com",
        category="",
        per_year=3,
        out_dir=str(out),
    )
    clean_path = out / "2022" / "hfa-email-message-uid2.clean.txt"
    raw_path = out / "2022" / "hfa-email-message-uid2.raw.txt"
    assert clean_path.read_text(encoding="utf-8") == clean_email_body(raw_path.read_text(encoding="utf-8"))


def test_sampler_filters_by_category(extractor_vault, sample_email_card, tmp_path):
    fm1, b1 = sample_email_card(
        "hfa-email-message-c1",
        "a@doordash.com",
        "receipt for you",
        "x",
        sent_at="2020-01-01T12:00:00-08:00",
    )
    fm2, b2 = sample_email_card(
        "hfa-email-message-c2",
        "a@doordash.com",
        "random subject",
        "x",
        sent_at="2020-02-01T12:00:00-08:00",
    )
    write_email_to_vault(extractor_vault, "Email/c1.md", fm1, b1)
    write_email_to_vault(extractor_vault, "Email/c2.md", fm2, b2)
    out = tmp_path / "samples"
    run_template_sampler(
        vault_path=extractor_vault,
        domain="doordash.com",
        category="receipt",
        per_year=5,
        out_dir=str(out),
    )
    assert (out / "2020" / "hfa-email-message-c1.meta.json").is_file()
    assert not (out / "2020" / "hfa-email-message-c2.meta.json").exists()


def test_sampler_respects_per_year_limit(extractor_vault, sample_email_card, tmp_path):
    for i in range(5):
        fm, body = sample_email_card(
            f"hfa-email-message-py{i}",
            "a@doordash.com",
            "receipt",
            "x",
            sent_at=f"2019-{i+1:02d}-01T12:00:00-08:00",
        )
        write_email_to_vault(extractor_vault, f"Email/py{i}.md", fm, body)
    out = tmp_path / "samples"
    run_template_sampler(
        vault_path=extractor_vault,
        domain="doordash.com",
        category="receipt",
        per_year=2,
        out_dir=str(out),
    )
    ydir = out / "2019"
    metas = list(ydir.glob("*.meta.json"))
    assert len(metas) <= 2


def test_sampler_batch_one_walk_two_domains(extractor_vault, sample_email_card, tmp_path):
    """Batch mode matches multiple jobs in a single vault iteration."""
    fm1, b1 = sample_email_card(
        "hfa-email-message-dd",
        "a@doordash.com",
        "your order today",
        "dd",
        sent_at="2021-01-01T12:00:00-08:00",
    )
    fm2, b2 = sample_email_card(
        "hfa-email-message-ub",
        "rides@uber.com",
        "your trip receipt",
        "ub",
        sent_at="2021-02-01T12:00:00-08:00",
    )
    write_email_to_vault(extractor_vault, "Email/dd.md", fm1, b1)
    write_email_to_vault(extractor_vault, "Email/ub.md", fm2, b2)
    out_dd = tmp_path / "dd"
    out_ub = tmp_path / "ub"
    r = run_template_sampler_batch(
        vault_path=extractor_vault,
        jobs=[
            {"name": "doordash", "domain": "doordash.com", "category": "order", "out_dir": str(out_dd)},
            {"name": "uber", "domain": "uber.com", "category": "trip", "out_dir": str(out_ub)},
        ],
        per_year=3,
    )
    assert r["scanned"] >= 2
    assert (out_dd / "2021" / "hfa-email-message-dd.meta.json").is_file()
    assert (out_ub / "2021" / "hfa-email-message-ub.meta.json").is_file()
    assert "doordash" in r["jobs"] and "uber" in r["jobs"]
