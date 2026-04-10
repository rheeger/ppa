"""Known transactional sender domains and noise patterns for LLM pre-filtering.

This is the single place to add new services / providers. The enrichment runner
uses these to classify threads *before* any LLM call:

- **TRANSACTIONAL_DOMAINS** — threads from these domains skip triage and go straight
  to extraction (fast-track). Organized by card type so routing is free.
- **NOISE_DOMAINS** — always skip (no LLM triage needed).
- **NOISE_SENDER_PATTERNS** — regex on from_email; auto-compiled.
- **NOISE_SUBJECT_PATTERNS** — regex on subject line; auto-compiled.

To add a new service: drop its domain(s) into the right ``TRANSACTIONAL_DOMAINS``
category. To add a noise source: add a domain or pattern. No code changes needed.
"""

from __future__ import annotations

import re
from functools import lru_cache

from ppa_google_auth import INTERNAL_DOMAINS

# ---------------------------------------------------------------------------
# Transactional sender domains → card_types fast-track
# ---------------------------------------------------------------------------
# Key: lowercase email domain (after @).
# Value: list of card_types this sender is known for.
# Threads from these domains skip LLM triage — go straight to extraction.
# Add liberally; extraction validates via Pydantic so false positives are safe.

TRANSACTIONAL_DOMAINS: dict[str, list[str]] = {
    # ── Meal delivery / food ──────────────────────────────────────────────
    "uber.com": ["meal_order", "ride"],
    "ubereats.com": ["meal_order"],
    "doordash.com": ["meal_order"],
    "messages.doordash.com": ["meal_order"],
    "grubhub.com": ["meal_order"],
    "mail.grubhub.com": ["meal_order"],
    "postmates.com": ["meal_order"],
    "caviar.com": ["meal_order"],
    "seamless.com": ["meal_order"],
    "eat.chownow.com": ["meal_order"],
    "toasttab.com": ["meal_order"],
    "trycaviar.com": ["meal_order"],
    "slice.com": ["meal_order"],
    # ── Grocery ───────────────────────────────────────────────────────────
    "instacart.com": ["grocery_order"],
    "instacartemail.com": ["grocery_order"],
    "shipt.com": ["grocery_order"],
    "freshdirect.com": ["grocery_order"],
    "mail.freshdirect.com": ["grocery_order"],
    "thrivemarket.com": ["grocery_order"],
    "imperfectfoods.com": ["grocery_order"],
    "hungryroot.com": ["grocery_order"],
    "gopuff.com": ["grocery_order"],
    "mail.gopuff.com": ["grocery_order"],
    "walmart.com": ["grocery_order", "purchase"],
    # ── Rides / rideshare ─────────────────────────────────────────────────
    "lyft.com": ["ride"],
    "lyftmail.com": ["ride"],
    "via.com": ["ride"],
    "ridevia.com": ["ride"],
    # ── Flights / airlines ────────────────────────────────────────────────
    "united.com": ["flight"],
    "aa.com": ["flight"],
    "delta.com": ["flight"],
    "e.delta.com": ["flight"],
    "southwest.com": ["flight"],
    "luv.southwest.com": ["flight"],
    "jetblue.com": ["flight"],
    "alaskaair.com": ["flight"],
    "spiritairlines.com": ["flight"],
    "frontier.com": ["flight"],
    "hawaiianairlines.com": ["flight"],
    "britishairways.com": ["flight"],
    "email.britishairways.com": ["flight"],
    "airfrance.fr": ["flight"],
    "klm.com": ["flight"],
    "lufthansa.com": ["flight"],
    "emirates.com": ["flight"],
    "qatarairways.com": ["flight"],
    "singaporeair.com": ["flight"],
    "cathaypacific.com": ["flight"],
    "aircanada.com": ["flight"],
    "virginatlantic.com": ["flight"],
    "email.virginatlantic.com": ["flight"],
    "norwegianair.com": ["flight"],
    "ryanair.com": ["flight"],
    "easyjet.com": ["flight"],
    "volaris.com": ["flight"],
    "wizzair.com": ["flight"],
    # Additional airlines from missed cards
    "aircanada.ca": ["flight"],
    "email.aircanada.com": ["flight"],
    "e.aa.com": ["flight"],
    "news.delta.com": ["flight"],
    "e.southwest.com": ["flight"],
    "email.alaskaair.com": ["flight"],
    "email.jetblue.com": ["flight"],
    "notifications.google.com": ["flight"],  # Google Flights
    # Booking aggregators
    "booking.com": ["accommodation", "flight"],
    "hotels.com": ["accommodation"],
    "expedia.com": ["flight", "accommodation", "car_rental"],
    "e.expedia.com": ["flight", "accommodation", "car_rental"],
    "kayak.com": ["flight", "accommodation"],
    "priceline.com": ["flight", "accommodation", "car_rental"],
    # google.com removed — too broad (Google Alerts, account setup, etc.)
    "hopper.com": ["flight", "accommodation"],
    "laderatrav.com": ["flight", "accommodation"],
    "ladera.travel": ["flight", "accommodation"],
    # ── Accommodation ─────────────────────────────────────────────────────
    "airbnb.com": ["accommodation"],
    "guest.airbnb.com": ["accommodation"],
    "vrbo.com": ["accommodation"],
    "homeaway.com": ["accommodation"],
    "marriott.com": ["accommodation"],
    "e.marriott.com": ["accommodation"],
    "hilton.com": ["accommodation"],
    "e.hilton.com": ["accommodation"],
    "hyatt.com": ["accommodation"],
    "ihg.com": ["accommodation"],
    "wyndhamhotels.com": ["accommodation"],
    "choicehotels.com": ["accommodation"],
    "fourseasons.com": ["accommodation"],
    "starwoodhotels.com": ["accommodation"],
    "radissonhotels.com": ["accommodation"],
    "bestwestern.com": ["accommodation"],
    "accor.com": ["accommodation"],
    "hoteltonight.com": ["accommodation"],
    # ── Car rental ────────────────────────────────────────────────────────
    "nationalcar.com": ["car_rental"],
    "emeraldclub.com": ["car_rental"],
    "hertz.com": ["car_rental"],
    "enterprise.com": ["car_rental"],
    "avis.com": ["car_rental"],
    "budget.com": ["car_rental"],
    "sixt.com": ["car_rental"],
    "turo.com": ["car_rental"],
    "zipcar.com": ["car_rental"],
    "getaround.com": ["car_rental"],
    # ── Shipping / delivery ───────────────────────────────────────────────
    "ups.com": ["shipment"],
    "fedex.com": ["shipment"],
    "usps.com": ["shipment"],
    "dhl.com": ["shipment"],
    "ontrac.com": ["shipment"],
    "lasership.com": ["shipment"],
    "amazon.com": ["shipment", "purchase"],
    "ship-confirm.amazon.com": ["shipment"],
    "auto-confirm.amazon.com": ["purchase"],
    "order-update.amazon.com": ["shipment", "purchase"],
    "shipment-tracking.amazon.com": ["shipment"],
    # ── E-commerce / purchase ─────────────────────────────────────────────
    "gc.apple.com": ["purchase"],
    "email.apple.com": ["purchase"],
    "bestbuy.com": ["purchase"],
    "target.com": ["purchase"],
    "costco.com": ["purchase"],
    "ebay.com": ["purchase"],
    "etsy.com": ["purchase"],
    "transaction.etsy.com": ["purchase"],
    "shopify.com": ["purchase"],
    "squareup.com": ["purchase"],
    "stripe.com": ["purchase"],
    # venmo/paypal/zelle: send both receipts AND P2P/alerts — classify decides
    # "paypal.com": moved to classify
    # "venmo.com": moved to classify
    # "zelle.com": moved to classify
    # ── Subscriptions / SaaS ──────────────────────────────────────────────
    "netflix.com": ["subscription"],
    "spotify.com": ["subscription"],
    "hulu.com": ["subscription"],
    "disneyplus.com": ["subscription"],
    "max.com": ["subscription"],
    "paramountplus.com": ["subscription"],
    "peacocktv.com": ["subscription"],
    "1password.com": ["subscription"],
    # ── Events / tickets ──────────────────────────────────────────────────
    "ticketmaster.com": ["event_ticket"],
    "livenation.com": ["event_ticket"],
    "axs.com": ["event_ticket"],
    "seatgeek.com": ["event_ticket"],
    "stubhub.com": ["event_ticket"],
    "eventbrite.com": ["event_ticket"],
    "dice.fm": ["event_ticket"],
    # ── Payroll ───────────────────────────────────────────────────────────
    "gusto.com": ["payroll"],
    "adp.com": ["payroll"],
    "rippling.com": ["payroll"],
    "justworks.com": ["payroll"],
    "paychex.com": ["payroll"],
    "quickbooks.intuit.com": ["payroll"],
    # ── Financial / crypto ────────────────────────────────────────────────
    # americanexpress: sends both purchase alerts AND card-not-present alerts — classify decides
    # "americanexpress.com": moved to classify
    # "welcome.americanexpress.com": moved to classify
    "brex.com": ["purchase"],
    "coinbase.com": ["purchase"],
    "contact.coinbase.com": ["purchase"],
    "kraken.com": ["purchase"],
    "gemini.com": ["purchase"],
    "plaid.com": ["subscription"],
    # ── Cloud / SaaS billing ─────────────────────────────────────────────
    "cloud.google.com": ["purchase", "subscription"],
    "noreply-cloud@google.com": ["purchase", "subscription"],
    "domains.google": ["purchase"],
    "aws.amazon.com": ["purchase", "subscription"],
    "heroku.com": ["subscription"],
    "vercel.com": ["subscription"],
    "netlify.com": ["subscription"],
    "digitalocean.com": ["subscription"],
    "slack.com": ["subscription"],
    "notion.so": ["subscription"],
    "zoom.us": ["subscription"],
    "dropbox.com": ["subscription"],
    # github.com removed — mostly repo notifications; billing goes through classify
    "adobe.com": ["subscription"],
    "mail.anthropic.com": ["subscription"],
    "anthropic.com": ["subscription"],
    "infura.io": ["subscription"],
    # ── Insurance ─────────────────────────────────────────────────────────
    "lemonade.com": ["purchase"],
    "nextinsurance.com": ["subscription"],
    "progressive.com": ["subscription"],
    "geico.com": ["subscription"],
    "newfront.com": ["subscription"],
    # ── Meal kits / prepared food ─────────────────────────────────────────
    "blueapron.com": ["meal_order"],
    "hellofresh.com": ["meal_order"],
    "factor75.com": ["meal_order"],
    "sunbasket.com": ["meal_order"],
    "dailyharvest.com": ["meal_order"],
    # ── More travel ───────────────────────────────────────────────────────
    "tripadvisor.com": ["accommodation"],
    "hostelworld.com": ["accommodation"],
    "agoda.com": ["accommodation"],
    "amtrak.com": ["flight"],
    "wanderu.com": ["flight"],
    "flixbus.com": ["flight"],
    "greyhound.com": ["flight"],
    # ── Telecom / utilities ───────────────────────────────────────────────
    "t-mobile.com": ["subscription"],
    "verizon.com": ["subscription"],
    "att.com": ["subscription"],
    "xfinity.com": ["subscription"],
    "spectrum.com": ["subscription"],
    # ── Media / content subscriptions ───────────────────────────────────
    "stratechery.com": ["subscription"],
    "nytimes.com": ["subscription"],
    "washingtonpost.com": ["subscription"],
    "theinformation.com": ["subscription"],
    "substack.com": ["subscription"],
    "superhuman.com": ["subscription"],
    "medium.com": ["subscription"],
    # ── Fitness / wellness ────────────────────────────────────────────────
    "onepeloton.com": ["subscription"],
    "mail.op.onepeloton.com": ["subscription"],
    "classpass.com": ["subscription"],
    "mindbody.io": ["subscription"],
}

# ---------------------------------------------------------------------------
# Personal / freemail domains — person-to-person, never a transactional *sender*.
# Threads where ALL senders are personal-only → skip (no LLM triage).
# ---------------------------------------------------------------------------

PERSONAL_DOMAINS: frozenset[str] = frozenset(
    {
        "gmail.com",
        "yahoo.com",
        "yahoo.co.uk",
        "hotmail.com",
        "outlook.com",
        "live.com",
        "msn.com",
        "aol.com",
        "icloud.com",
        "me.com",
        "mac.com",
        "protonmail.com",
        "proton.me",
        "fastmail.com",
        "hey.com",
        "zoho.com",
        "yandex.com",
        "mail.com",
        "gmx.com",
        "gmx.net",
        "sbcglobal.net",
        "comcast.net",
        "verizon.net",
        "att.net",
        "cox.net",
        "earthlink.net",
        "charter.net",
        "bellsouth.net",
        "optonline.net",
    }
)

# ---------------------------------------------------------------------------
# Known-noise domains — always skip, no LLM triage needed
# ---------------------------------------------------------------------------

NOISE_DOMAINS: frozenset[str] = frozenset(
    {
        # Social media notifications
        "facebookmail.com",
        "linkedin.com",
        "twitter.com",
        "x.com",
        "instagram.com",
        "pinterest.com",
        "reddit.com",
        "tiktok.com",
        "nextdoor.com",
        # Account / auth
        "accounts.google.com",
        "account.live.com",
        "id.apple.com",
        # Newsletter / marketing platforms
        "mailchimp.com",
        "mail.mailchimp.com",
        "sendgrid.net",
        "mandrillapp.com",
        "constantcontact.com",
        "hubspot.com",
        "marketo.com",
        "pardot.com",
        "sailthru.com",
        "braze.com",
        "intercom.io",
        "intercom-mail.com",
        "drip.com",
        "convertkit.com",
        "substack.com",
        "beehiiv.com",
        "buttondown.email",
        # Job boards
        "indeed.com",
        "glassdoor.com",
        # Surveys / feedback
        "surveymonkey.com",
        "typeform.com",
        # CI / dev notifications (not transactional receipts)
        "circleci.com",
        "github.com",
        "gitlab.com",
        "bitbucket.org",
        "app.circleci.com",
        # Community / lists
        "googlegroups.com",
        "groups.google.com",
        "lists.wnyc.org",
        # Misc notification-only
        "figma.com",
        "loom.com",
        # Newsletter delivery platforms (not the publications themselves)
        "substackmail.com",
        # Broad corporate domains where personal email != receipts
        "apple.com",
        "google.com",
        "microsoft.com",
        # Banking alerts (balance updates, not purchases)
        "wellsfargo.com",
        "notify.wellsfargo.com",
        "chase.com",
        "citi.com",
        "capitalone.com",
        "discover.com",
        "usbank.com",
        "ally.com",
        # americanexpress moved to transactional
        # Finance / budgeting notifications
        "mint.com",
        # Real estate listings (not purchases)
        "redfin.com",
        "zillow.com",
        "opendoor.com",
        # Document signing (often not the transaction itself)
        "docusign.net",
        "docusign.com",
        "hellosign.com",
        "statefarm.com",
        # Dev tools (CI notifications only — billing domains are transactional)
        "gitlab.com",
        "bitbucket.org",
    }
)

# ---------------------------------------------------------------------------
# Pattern-based noise filters (compiled lazily)
# ---------------------------------------------------------------------------

NOISE_SENDER_REGEXES: list[str] = [
    r"^no-?reply@",
    r"^noreply@",
    r"^mailer-daemon@",
    r"^postmaster@",
    r"^notifications@github\.com$",
    r"@bounce\.",
    r"@bounces\.",
    r"@.*\.list-manage\.com$",
    r"@.*\.campaign-archive\.com$",
]

NOISE_SUBJECT_REGEXES: list[str] = [
    r"(?i)^(re:\s*)*unsubscribe",
    r"(?i)^(re:\s*)*out of office",
    r"(?i)^(re:\s*)*automatic reply",
    r"(?i)delivery status notification",
    r"(?i)^(re:\s*)*vacation.*auto",
]

# Subject patterns that indicate marketing/promo even from transactional domains.
# A thread is downgraded from fast_track to skip if ALL subjects match these.
MARKETING_SUBJECT_REGEXES: list[str] = [
    r"(?i)save \d+%",
    r"(?i)\d+%\s*off",
    r"(?i)limited.time",
    r"(?i)exclusive\s+(offer|deal|sale)",
    r"(?i)deals?\s+(of the|we think|you)",
    r"(?i)(flash|clearance)\s+sale",
    r"(?i)bestsellers?\s+in\b",
    r"(?i)recommended\s+for\s+you",
    r"(?i)new\s+arrivals",
    r"(?i)shop\s+now",
    r"(?i)free\s+shipping",
    r"(?i)don.t\s+miss",
    r"(?i)last\s+chance",
    r"(?i)act\s+now",
    r"(?i)today\s+only",
    r"(?i)view\s+in\s+(browser|email)",
    r"(?i)newsletter",
    r"(?i)weekly\s+(digest|roundup|update)",
    r"(?i)inspiration\s+for",
    r"(?i)explore\s+(our|new|top)",
    r"(?i)trending\s+(now|this)",
    r"(?i)top\s+picks",
    r"(?i)gift\s+(guide|ideas)",
    r"(?i)mileageplus\s+offer",
    r"(?i)bonus\s+miles",
    r"(?i)promo(tion)?",
    r"(?i)rate\s+your\s+(purchase|experience|order|driver|ride)",
    r"(?i)review\s+your",
    r"(?i)survey",
    r"(?i)how\s+was\s+your",
    r"(?i)watch\s*list",
    r"(?i)wants\s+to\s+get\s+you",
    r"(?i)your\s+trip\s+is\s+around\s+the\s+corner",
    r"(?i)check\s+in\s+now\s+for\s+your",
    r"(?i)today.s\s+headlines",
    r"(?i)^FW:\s",
    r"(?i)^Fwd:\s",
    r"(?i)you\s+have\s+a\s+bill\s+coming",
    r"(?i)balance\s+update",
    r"(?i)payment\s+due\s+on",
    r"(?i)payment\s+posted",
    r"(?i)account\s+(balance|update)",
    r"(?i)you\s+have\s+an\s+interview",
    r"(?i)google\s+alert",
    r"(?i)removed\s+a\s+card",
    r"(?i)new\s+device\s+(confirm|verif)",
    r"(?i)send\s+emails\s+from",
    r"(?i)verify\s+your\s+(email|account|identity)",
    r"(?i)password\s+(reset|changed|updated)",
    r"(?i)sign.in\s+(attempt|alert|notification)",
    r"(?i)two.factor|2fa|mfa\s+code",
    r"(?i)security\s+alert",
    r"(?i)login\s+(attempt|notification)",
    r"^\[.+\]",  # GitHub repo notifications: "[repo-name] ..."
]


@lru_cache(maxsize=1)
def _compiled_noise_sender() -> list[re.Pattern[str]]:
    return [re.compile(p) for p in NOISE_SENDER_REGEXES]


@lru_cache(maxsize=1)
def _compiled_noise_subject() -> list[re.Pattern[str]]:
    return [re.compile(p) for p in NOISE_SUBJECT_REGEXES]


@lru_cache(maxsize=1)
def _compiled_marketing_subject() -> list[re.Pattern[str]]:
    return [re.compile(p) for p in MARKETING_SUBJECT_REGEXES]


def _is_marketing_subject(subject: str) -> bool:
    return any(p.search(subject) for p in _compiled_marketing_subject())


def _email_domain(from_email: str) -> str:
    fe = (from_email or "").strip().lower()
    if "@" not in fe:
        return ""
    return fe.rsplit("@", 1)[-1]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_thread_prefilter(
    from_emails: list[str],
    subjects: list[str],
    *,
    user_domains: frozenset[str] | None = None,
) -> tuple[str, list[str]]:
    """Classify a thread before any LLM call.

    Returns ``(decision, card_types)`` where *decision* is one of:

    - ``"fast_track"`` — known transactional sender; ``card_types`` populated
    - ``"skip"`` — known noise / personal / work mail; ``card_types`` empty
    - ``"triage"`` — unknown; needs LLM triage; ``card_types`` empty

    ``user_domains`` — org/work domains treated like personal for the
    all-personal-senders heuristic (coworker threads). Defaults to
    ``INTERNAL_DOMAINS`` from ``ppa_google_auth`` when omitted.
    """

    work_domains: frozenset[str] = (
        user_domains if user_domains is not None else frozenset(INTERNAL_DOMAINS)
    )

    merged_types: list[str] = []
    has_transactional = False
    has_noise = False
    has_personal_only = True  # assume personal until proven otherwise

    for fe in from_emails:
        dom = _email_domain(fe)
        fe_lower = fe.strip().lower()

        if dom in NOISE_DOMAINS:
            has_noise = True
            continue
        if any(p.search(fe_lower) for p in _compiled_noise_sender()):
            has_noise = True
            continue

        types = TRANSACTIONAL_DOMAINS.get(dom)
        if types:
            has_transactional = True
            has_personal_only = False
            for t in types:
                if t not in merged_types:
                    merged_types.append(t)
            continue

        if dom in PERSONAL_DOMAINS or dom in work_domains:
            continue

        has_personal_only = False

    if not has_transactional:
        for subj in subjects:
            if any(p.search(subj) for p in _compiled_noise_subject()):
                has_noise = True
                break

    if has_transactional:
        if subjects and all(_is_marketing_subject(s) for s in subjects if s):
            return "skip", []
        return "fast_track", merged_types

    if has_noise:
        return "skip", []

    if has_personal_only and from_emails:
        return "skip", []

    return "triage", []


def known_transactional_domains() -> dict[str, list[str]]:
    """Return a copy (for tests / inspection)."""
    return dict(TRANSACTIONAL_DOMAINS)


def known_noise_domains() -> frozenset[str]:
    return NOISE_DOMAINS
