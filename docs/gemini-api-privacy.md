

# Gemini API — data retention and privacy for enrichment

Phase 2.75 supports Gemini as an extraction provider (`--provider gemini`). This doc covers
what data goes to Google and how retention works.

## What gets sent

Each extraction call sends:

- **System prompt** (~300 tokens) — card schema + extraction rules
- **Email thread body** — full text of 1–5 messages (sender, subject, body content)
- **No images, no attachments** — text only

This includes personal addresses, financial amounts, names, and confirmation codes
from your email receipts.

## Gemini API data retention (as of April 2026)

| Tier          | Training on your data?                        | Retention                       | Human review?             |
| ------------- | --------------------------------------------- | ------------------------------- | ------------------------- |
| **Free tier** | **Yes** — used to improve Google products     | 55 days (abuse monitoring)      | Yes, possible             |
| **Paid tier** | **No** — not used for training                | 55 days (abuse monitoring only) | Only if flagged for abuse |
| **Vertex AI** | **No** + zero-data-retention option available | Configurable (can be 0)         | No by default             |

Source: [Gemini API Terms](https://ai.google.dev/gemini-api/terms), [Abuse Monitoring](https://ai.google.dev/gemini-api/docs/usage-policies), [Vertex AI ZDR](https://docs.cloud.google.com/vertex-ai/generative-ai/docs/vertex-ai-zero-data-retention)

## Practical implications for this archive

- These are **your own Gmail emails** — Google already has them in Gmail's infrastructure.
  The incremental privacy exposure from sending them to Gemini API is minimal.
- **Paid tier** (link a billing account): Google does **not** train on your prompts/responses.
  Data is retained 55 days for abuse monitoring, then deleted.
- **Free tier**: Google **may** use your data for model improvement. Avoid for sensitive content.

## How to control retention

1. **Use paid tier** — link a billing account at [AI Studio](https://aistudio.google.com/).
   Even $0.10 of usage puts you on paid terms (no training, DPA applies).
2. **Vertex AI zero-data-retention** — if you need maximum control, use Vertex AI endpoints
   and request an abuse monitoring exception. Overkill for a personal archive.
3. **No opt-out on free tier** — if you don't link billing, Google can use your data.

## Recommended setup

```bash
# Set your API key (get from https://aistudio.google.com/apikey)
export GEMINI_API_KEY="your-key-here"

# Run enrichment via Gemini (default model: gemini-2.0-flash)
make enrich-emails-gemini

# Or with custom model
PPA_PATH=/path/to/vault .venv/bin/python -m archive_mcp enrich-emails \
  --provider gemini --extract-model gemini-2.5-flash --workers 8
```

## Cost estimate

- **gemini-2.5-flash**: ~$0.15 per 1M input tokens, ~$0.60 per 1M output tokens (pricing varies)
- Typical extraction: ~1,500 input + ~200 output tokens per thread
- 1pct slice (438 threads): ~$0.05
- Full seed (~4,000 threads): ~$0.50
- Free tier has generous quotas (15 RPM on flash) — may be sufficient for small runs
