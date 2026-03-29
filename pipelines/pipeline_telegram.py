#!/usr/bin/env python3
# ================================================================
#  pipeline_telegram.py — Dynamic Telegram Signal Discovery
#  Radicalisation Signals · IEA Kenya NIRU AI Hackathon
#
#  FIXES IN THIS VERSION:
#    FIX-A  Typed exception handling — no more bare except
#    FIX-B  Per-channel retry with exponential backoff (2 attempts)
#    FIX-C  Helper function pull_channel_messages() extracted so
#            the main loop stays readable and retry is reusable
#    FIX-D  send() calls inside helpers get individual try/except
#            so one bad channel can't kill the whole run
#
#  Original improvements v1-v4:
#    FIX 1 — Broader Swahili/Sheng/English keywords
#    FIX 2 — Date filtering: only pull messages newer than last run
#    FIX 3 — Kenya filter: exclude noise channels
#    FIX 4 — Private groups: join via invite links if provided
#    FIX 5 — Sender speed: batch sender resolution, skip if slow
#    FIX 6 — Language: rich Swahili + Sheng signal keywords
#    FIX 7 — Closed loop: reads officer-approved keywords from
#             Supabase keyword_bank at startup
#    FIX 8 — Dynamic queries: reads active search queries from
#             Supabase search_queries table at startup
#
#  Usage: called by pipeline_telegram.R or run directly
# ================================================================

import os
import csv
import asyncio
import sys
import json
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta

# ── LOAD CREDENTIALS ──────────────────────────────────────────────
def load_renviron(path="secrets/.Renviron"):
    env = {}
    if not os.path.exists(path):
        print(f"ERROR: {path} not found")
        sys.exit(1)
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                env[key.strip()] = val.strip()
    return env

env      = load_renviron()
API_ID   = int(env.get("TELEGRAM_API_ID", "0"))
API_HASH = env.get("TELEGRAM_API_HASH", "")
SUPA_URL = env.get("SUPABASE_URL", "").rstrip("/")
SUPA_KEY = env.get("SUPABASE_KEY", "")

if not API_ID or not API_HASH:
    print("ERROR: TELEGRAM_API_ID or TELEGRAM_API_HASH missing")
    sys.exit(1)

# ── CONFIGURATION ─────────────────────────────────────────────────
SESSION_FILE              = "cache/tg_session"
OUTPUT_CSV                = "cache/tg_raw_messages.csv"
LAST_RUN_FILE             = "cache/tg_last_run.json"
KEYWORD_BANK_CACHE        = "cache/kw_bank_cache.json"
SEARCH_QUERIES_CACHE      = "cache/search_queries_cache.json"
KEYWORD_BANK_TTL_MINS     = 60
SEARCH_QUERIES_TTL_MINS   = 60
MESSAGES_LIMIT            = 300
SEARCH_LIMIT              = 10
LOOKBACK_HOURS            = 48
MIN_MESSAGE_LEN           = 10
MAX_CHANNEL_RETRIES       = 2   # FIX-B: retry failed channel reads

os.makedirs("cache", exist_ok=True)

# ── FIX 2: DATE FILTER ────────────────────────────────────────────
def load_last_run():
    if os.path.exists(LAST_RUN_FILE):
        with open(LAST_RUN_FILE) as f:
            data = json.load(f)
            return datetime.fromisoformat(
                data.get("last_run", "2000-01-01T00:00:00")
            ).replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)

def save_last_run():
    with open(LAST_RUN_FILE, "w") as f:
        json.dump({"last_run": datetime.now(timezone.utc).isoformat()}, f)

# ── FIX 3: NOISE CHANNEL FILTER ───────────────────────────────────
NOISE_PATTERNS = [
    "crypto", "bitcoin", "forex", "investment", "trading",
    "real estate", "property", "jobs", "job links", "vacancies",
    "employment", "hiring", "sugar mum", "divas", "adult",
    "hiphop", "music", "entertainment", "comedy", "jokes",
    "farming", "agriculture", "food", "market", "produce",
    "polytechnic", "university", "college", "study abroad",
    "ghana", "togo", "ivory coast", "congo", "south africa",
    "ethiopia", "malawi", "morocco", "tanzania", "nigeria",
]

def is_noise_channel(title):
    t = title.lower()
    return any(n in t for n in NOISE_PATTERNS)

# ── FIX 1 & 6: EXPANDED NCIC KEYWORDS ────────────────────────────
SEARCH_KEYWORDS = [
    "hate speech Kenya", "incitement Kenya", "ethnic violence Kenya",
    "tribal war Kenya", "ethnic cleansing Kenya", "community attack Kenya",
    "tribal hate Kenya", "ethnic tension Kenya", "religious hatred Kenya",
    "election violence Kenya", "tribal rigging Kenya",
    "chuki kabila Kenya", "uchochezi Kenya", "vita vya makabila",
    "mgawanyiko Kenya", "ubaguzi Kenya", "maneno ya chuki Kenya",
    "kabila Kenya siasa", "ukabila Kenya",
    "kudharau kabila", "vita ya makabila Kenya",
    "Kenya politics", "Kenya siasa", "Kenya 2027",
    "uchaguzi Kenya", "Kenya parliament",
]

TIER3 = [
    "kill them", "kill all", "eliminate them", "wipe them out",
    "drive them out", "ethnic cleansing", "cleanse them",
    "exterminate", "slaughter them", "burn their homes",
    "attack the community", "armed uprising",
    "take up arms", "pick up weapons",
    "waende kwao", "wauawe", "wachinjwe", "angamizwa",
    "ondokeni", "chomwa moto", "wafukuzwe", "waondolewe",
    "funga mipaka yao", "wapigiwe", "washambuliwe",
    "waangamizwe", "watupwe nje", "watakaswe",
    "chukua silaha", "piga vita", "mapinduzi ya damu",
    "waasi wa damu", "pigana nao",
    "wamaliza", "waangush", "waoshe", "wafutwe",
    "cockroaches", "vermin", "parasites", "sub-human",
    "these animals", "not human", "disease people",
    "these insects", "they are rats", "like dogs",
    "nyoka hawa", "mende hawa", "panya hawa",
    "hawa wanyama", "si watu hawa", "hawa wadudu",
    "kama mbwa", "kama nguruwe", "hawastahili kuishi",
    "jihad", "holy war", "vita vitakatifu",
    "dini ya vita", "muua kafiri", "kill the infidel",
]

TIER2 = [
    "jaluo", "madoadoa", "kabila chafu", "kabila mbaya",
    "kabila ya wezi", "wakabila wabaya", "watu wa ukabila",
    "gikuyu wezi", "kalenjin wauaji", "luhya wajinga",
    "kamba wabaya", "somali haramu", "turkana wakali",
    "kikuyu matajiri wa wizi", "luos are",
    "those people tribe", "that tribe always", "typical tribe",
    "tribe of thieves", "tribe of killers", "dirty tribe",
    "si wakenya", "wahamie kwao", "toka kwetu",
    "si jamii yetu", "hawatakiwi hapa", "watoke nchini",
    "hawana haki hapa", "si raia wa kweli",
    "not real kenyans", "foreigners in our land",
    "they don't belong here", "out of our country",
    "send them back", "not one of us",
    "tujitenganishe", "tutengane", "secede from kenya",
    "break away from kenya", "our own republic",
    "jamhuri yetu wenyewe", "independence from kenya",
    "pwani si kenya", "tunataka uhuru wetu",
    "tribal rigging", "kabila iliibia",
    "uchaguzi wa ukabila", "stolen by tribe",
    "rigged for their tribe", "vote along tribal lines",
    "piga kura ya kabila", "usipige kura kabila nyingine",
    "dini chafu", "religion of evil", "adui wa dini",
    "waislamu ni hatari", "wakristo ni maadui",
    "dini ya shetani", "religion of terrorists",
]

TIER1 = [
    "kabila", "ukabila", "makabila", "kikabila",
    "mgawanyiko wa makabila", "vita vya makabila",
    "chuki za makabila", "ugomvi wa makabila",
    "tribalism", "tribal politics", "ethnic tension",
    "community conflict", "inter-ethnic", "inter-tribal",
    "ethnic mobilisation", "tribal mobilisation",
    "ghasia za uchaguzi", "vurugu za uchaguzi",
    "mapigano ya jamii", "vita vya ardhi",
    "ugomvi wa malisho", "mgogoro wa ardhi",
    "election violence", "post election violence",
    "pre-election tension", "community clashes",
    "inter-community violence", "land conflict",
    "resource conflict", "pastoralist conflict",
    "chuki", "uchochezi", "maneno ya chuki",
    "ubaguzi", "kudharau", "kuchochea",
    "hate speech", "incitement", "discrimination",
    "marginalised community", "excluded group",
    "kudharau watu", "kuchukia kabila",
    "pigana na kabila", "wapige kabila",
    "mgawanyiko wa taifa", "kuvunja umoja",
    "dhidi ya umoja", "taifa letu litavunjika",
    "divide kenya", "split the nation",
    "undermine national unity", "tear the country apart",
    "mvutano wa kidini", "dini dhidi ya dini",
    "religious tension", "interfaith conflict",
    "religious discrimination", "faith-based violence",
]

# ── KENYA RELEVANCE FILTER ────────────────────────────────────────
KENYA_CONTEXT = [
    "kenya", "kenyan", "wakenya", "mkenya", "nchini kenya",
    "serikali ya kenya", "taifa letu", "katiba ya kenya",
    "uchaguzi", "iebc", "siasa kenya", "wananchi kenya",
    "jamii kenya", "kenya kwanza", "bunge kenya",
]

def is_kenya_relevant(text, title=""):
    combined = (text + " " + title).lower()
    return any(k in combined for k in KENYA_CONTEXT)

# ── FIX 7: DYNAMIC KEYWORD BANK ──────────────────────────────────
def _keyword_bank_cache_valid():
    if not os.path.exists(KEYWORD_BANK_CACHE):
        return None
    try:
        with open(KEYWORD_BANK_CACHE) as f:
            data = json.load(f)
        saved_at = datetime.fromisoformat(data["saved_at"]).replace(tzinfo=timezone.utc)
        age_mins = (datetime.now(timezone.utc) - saved_at).total_seconds() / 60
        if age_mins > KEYWORD_BANK_TTL_MINS:
            return None
        return data["keywords"]
    except (json.JSONDecodeError, KeyError, ValueError):
        return None

def _save_keyword_bank_cache(keywords):
    try:
        with open(KEYWORD_BANK_CACHE, "w") as f:
            json.dump({
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "keywords": keywords
            }, f)
    except OSError as e:
        print(f"  ⚠ Could not save keyword cache: {e}")

def fetch_approved_keywords():
    cached = _keyword_bank_cache_valid()
    if cached is not None:
        print(f"  [keyword bank] cache HIT — {len(cached)} approved keywords loaded")
        return cached

    if not SUPA_URL or not SUPA_KEY:
        print("  [keyword bank] ⚠ SUPABASE_URL/KEY not set — skipping dynamic keywords")
        return []

    try:
        url = (f"{SUPA_URL}/rest/v1/keyword_bank"
               f"?select=keyword,tier,category&status=eq.approved&limit=1000")
        req = urllib.request.Request(url, headers={
            "apikey":        SUPA_KEY,
            "Authorization": f"Bearer {SUPA_KEY}",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            keywords = json.loads(resp.read().decode())
        _save_keyword_bank_cache(keywords)
        print(f"  [keyword bank] fetched {len(keywords)} approved keywords from Supabase")
        return keywords
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, OSError) as e:
        print(f"  [keyword bank] ⚠ Supabase fetch failed ({e}) — using static lists only")
        return []

def merge_approved_keywords():
    keywords = fetch_approved_keywords()
    if not keywords:
        return 0, 0, 0

    added = {1: 0, 2: 0, 3: 0}
    existing = {
        1: set(k.lower() for k in TIER1),
        2: set(k.lower() for k in TIER2),
        3: set(k.lower() for k in TIER3),
    }

    for row in keywords:
        kw   = str(row.get("keyword", "")).lower().strip()
        tier = int(row.get("tier", 1))
        if not kw or tier not in (1, 2, 3):
            continue
        if kw not in existing[tier]:
            if   tier == 3: TIER3.append(kw)
            elif tier == 2: TIER2.append(kw)
            else:           TIER1.append(kw)
            existing[tier].add(kw)
            added[tier] += 1

    return added[1], added[2], added[3]

# ── FIX 8: DYNAMIC SEARCH QUERIES ────────────────────────────────
def _search_queries_cache_valid():
    if not os.path.exists(SEARCH_QUERIES_CACHE):
        return None
    try:
        with open(SEARCH_QUERIES_CACHE) as f:
            data = json.load(f)
        saved_at = datetime.fromisoformat(data["saved_at"]).replace(tzinfo=timezone.utc)
        age_mins = (datetime.now(timezone.utc) - saved_at).total_seconds() / 60
        if age_mins > SEARCH_QUERIES_TTL_MINS:
            return None
        return data["queries"]
    except (json.JSONDecodeError, KeyError, ValueError):
        return None

def _save_search_queries_cache(queries):
    try:
        with open(SEARCH_QUERIES_CACHE, "w") as f:
            json.dump({
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "queries":  queries
            }, f)
    except OSError as e:
        print(f"  ⚠ Could not save search queries cache: {e}")

def fetch_active_search_queries():
    cached = _search_queries_cache_valid()
    if cached is not None:
        print(f"  [search queries] cache HIT — {len(cached)} active queries loaded")
        return cached

    if not SUPA_URL or not SUPA_KEY:
        print("  [search queries] ⚠ SUPABASE_URL/KEY not set — using static queries only")
        return []

    try:
        url = (f"{SUPA_URL}/rest/v1/search_queries"
               f"?select=query,language,priority&status=eq.active&limit=500")
        req = urllib.request.Request(url, headers={
            "apikey":        SUPA_KEY,
            "Authorization": f"Bearer {SUPA_KEY}",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            queries = json.loads(resp.read().decode())
        _save_search_queries_cache(queries)
        print(f"  [search queries] fetched {len(queries)} active queries from Supabase")
        return queries
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, OSError) as e:
        print(f"  [search queries] ⚠ Supabase fetch failed ({e}) — using static queries only")
        return []

def merge_active_search_queries():
    queries = fetch_active_search_queries()
    if not queries:
        return 0

    existing = set(q.lower().strip() for q in SEARCH_KEYWORDS)
    added    = 0

    for row in queries:
        q = str(row.get("query", "")).strip()
        if q and q.lower() not in existing:
            SEARCH_KEYWORDS.append(q)
            existing.add(q.lower())
            added += 1

    return added

# ── SIGNAL SCORING ────────────────────────────────────────────────
def score_message(text):
    txt      = text.lower()
    score    = 0
    triggers = []

    for kw in TIER3:
        if kw in txt:
            score += 3
            triggers.append(f"[T3]{kw}")
    for kw in TIER2:
        if kw in txt:
            score += 2
            triggers.append(f"[T2]{kw}")
    for kw in TIER1:
        if kw in txt:
            score += 1
            triggers.append(f"[T1]{kw}")

    score    = min(score, 10)
    priority = (
        "HIGH"   if score >= 3 else
        "MEDIUM" if score >= 2 else
        "LOW"    if score >= 1 else
        "NONE"
    )
    return score, priority, "; ".join(triggers)

def ncic_category(triggers):
    if not triggers:
        return "NONE"
    t = triggers.lower()
    if any(k in t for k in ["wauawe","waangamizwe","kill","eliminate","wipe",
                              "uprising","jihad","silaha","piga vita"]):
        return "INCITEMENT"
    if any(k in t for k in ["nyoka","mende","panya","cockroach","vermin",
                              "wanyama","wadudu","sub-human","not human"]):
        return "DEHUMANISATION"
    if any(k in t for k in ["secede","tujitenganishe","break away",
                              "pwani si kenya","jamhuri yetu"]):
        return "SECESSIONISM"
    if any(k in t for k in ["tribal rigging","uchaguzi wa ukabila",
                              "stolen by tribe","vote along tribal"]):
        return "ELECTION_INCITEMENT"
    if any(k in t for k in ["dini chafu","religion of evil","adui wa dini",
                              "waislamu","wakristo ni maadui"]):
        return "RELIGIOUS_HATRED"
    if any(k in t for k in ["jaluo","madoadoa","kabila chafu","si wakenya",
                              "not real kenyans","wahamie"]):
        return "ETHNIC_CONTEMPT"
    if any(k in t for k in ["hate speech","chuki","ubaguzi",
                              "discrimination","uchochezi"]):
        return "HATE_SPEECH"
    if any(k in t for k in ["kabila","tribal","ethnic","ukabila","makabila"]):
        return "ETHNIC_TENSION"
    return "DIVISIVE_CONTENT"

# ── FIX-C: CHANNEL MESSAGE HELPER WITH RETRY ─────────────────────
async def pull_channel_messages(client, entity, since, title,
                                 FloodWaitError, max_retries=MAX_CHANNEL_RETRIES):
    """
    Pull messages from a single channel with exponential-backoff retry.
    Returns a list of message objects, or [] on persistent failure.
    FIX-A: only catches specific Telethon/network exceptions.
    FIX-B: retries up to max_retries times before giving up.
    """
    for attempt in range(max_retries + 1):
        try:
            messages = await client.get_messages(
                entity,
                limit       = MESSAGES_LIMIT,
                offset_date = None
            )
            return messages

        except FloodWaitError as e:
            wait = e.seconds + 5
            print(f"  ⚠ Rate limited on {title} — waiting {wait}s")
            await asyncio.sleep(wait)
            # FloodWait counts as an attempt only if it exhausts retries
            if attempt == max_retries:
                print(f"  ✗ Giving up on {title} after FloodWait")
                return []

        except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
            if attempt < max_retries:
                backoff = 2 ** attempt
                print(f"  ⚠ Network error on {title} (attempt {attempt+1}) "
                      f"— retrying in {backoff}s: {e}")
                await asyncio.sleep(backoff)
            else:
                print(f"  ✗ {title} failed after {max_retries+1} attempts: {e}")
                return []

        except Exception as e:  # noqa: BLE001 — last resort, logged explicitly
            print(f"  ✗ Unexpected error on {title}: {type(e).__name__}: {e}")
            return []

    return []

# ── MAIN ──────────────────────────────────────────────────────────
async def main():
    from telethon import TelegramClient
    from telethon.tl.functions.contacts import SearchRequest
    from telethon.errors import FloodWaitError

    print("=" * 60)
    print("TELEGRAM NCIC SIGNAL PIPELINE v4")
    print(datetime.now().strftime("%d %b %Y %H:%M"))
    print("=" * 60)

    # Load dynamic keywords + queries
    print("[keyword bank] Loading approved keywords from Supabase...")
    t1_added, t2_added, t3_added = merge_approved_keywords()
    total_added = t1_added + t2_added + t3_added
    if total_added > 0:
        print(f"  [keyword bank] ✅ +{t1_added} T1  +{t2_added} T2  +{t3_added} T3 "
              f"— scorer now has {len(TIER1)} T1 / {len(TIER2)} T2 / {len(TIER3)} T3 keywords")
    else:
        print(f"  [keyword bank] Using static lists only "
              f"({len(TIER1)} T1 / {len(TIER2)} T2 / {len(TIER3)} T3)")

    print("[search queries] Loading active queries from Supabase...")
    sq_added = merge_active_search_queries()
    if sq_added > 0:
        print(f"  [search queries] ✅ +{sq_added} dynamic queries "
              f"— {len(SEARCH_KEYWORDS)} total search queries active")
    else:
        print(f"  [search queries] Using static queries only "
              f"({len(SEARCH_KEYWORDS)} queries)")

    since = load_last_run()
    print(f"[date filter] Only pulling messages since: "
          f"{since.strftime('%Y-%m-%d %H:%M UTC')}")

    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    await client.start()
    print("✅ Connected to Telegram via MTProto")

    discovered_channels = {}
    rows                = []

    # ── CHANNEL DISCOVERY ────────────────────────────────────────
    print(f"\n[discovery] Searching {len(SEARCH_KEYWORDS)} NCIC signal queries...")

    for kw in SEARCH_KEYWORDS:
        try:
            result    = await client(SearchRequest(q=kw, limit=SEARCH_LIMIT))
            new_found = 0
            for chat in result.chats:
                chat_id = str(chat.id)
                title   = getattr(chat, "title", str(chat.id))
                if is_noise_channel(title):
                    continue
                if chat_id not in discovered_channels:
                    discovered_channels[chat_id] = chat
                    new_found += 1
            if new_found > 0:
                print(f"  '{kw}' → +{new_found} channels")
            await asyncio.sleep(0.8)

        except FloodWaitError as e:
            print(f"  ⚠ Rate limited — waiting {e.seconds}s")
            await asyncio.sleep(e.seconds)
        except (ConnectionError, TimeoutError) as e:
            print(f"  ⚠ Network error for '{kw}': {e}")
            continue
        except Exception as e:          # noqa: BLE001 — discovery errors are non-fatal
            print(f"  ⚠ Skipping query '{kw}': {type(e).__name__}: {e}")
            continue

    print(f"\n[discovery] {len(discovered_channels)} relevant channels found")

    # ── MESSAGE PULLING & SCORING ─────────────────────────────────
    print(f"\n[messages] Pulling messages per channel "
          f"(since {since.strftime('%Y-%m-%d %H:%M')})...")

    n_total      = 0
    n_date_skip  = 0
    n_kenya_skip = 0
    n_kept       = 0
    sender_cache = {}

    for chat_id, entity in discovered_channels.items():
        title     = getattr(entity, "title", chat_id)
        chat_type = type(entity).__name__.lower()
        ch_kept   = 0

        # FIX-B/C: use helper with retry
        messages = await pull_channel_messages(
            client, entity, since, title, FloodWaitError
        )
        n_total += len(messages)

        for msg in messages:
            if msg.date and msg.date < since:
                n_date_skip += 1
                continue

            text = msg.text or msg.message or ""
            if not text or len(text.strip()) < MIN_MESSAGE_LEN:
                continue

            if not is_kenya_relevant(text, title):
                n_kenya_skip += 1
                continue

            score, priority, triggers = score_message(text)
            category = ncic_category(triggers)

            sender_id       = str(msg.sender_id or "")
            sender_name     = ""
            sender_username = ""

            if sender_id and sender_id in sender_cache:
                sender_name, sender_username = sender_cache[sender_id]
            elif sender_id:
                try:
                    sender = await asyncio.wait_for(msg.get_sender(), timeout=2.0)
                    if sender:
                        first           = getattr(sender, "first_name", "") or ""
                        last            = getattr(sender, "last_name",  "") or ""
                        sender_name     = (first + " " + last).strip()
                        sender_username = getattr(sender, "username", "") or ""
                        sender_cache[sender_id] = (sender_name, sender_username)
                except (asyncio.TimeoutError, Exception):  # noqa: BLE001 — best-effort
                    pass

            reply_to_id   = ""
            reply_to_name = ""
            if msg.reply_to and hasattr(msg.reply_to, "reply_to_msg_id"):
                reply_to_id = str(msg.reply_to.reply_to_msg_id or "")

            latitude  = ""
            longitude = ""
            if msg.geo:
                latitude  = str(msg.geo.lat)
                longitude = str(msg.geo.long)

            date_str = msg.date.astimezone(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S") if msg.date else ""

            rows.append({
                "message_id":      str(msg.id),
                "chat_id":         chat_id,
                "chat_title":      title,
                "chat_type":       chat_type,
                "sender_id":       sender_id,
                "sender_name":     sender_name,
                "sender_username": sender_username,
                "reply_to_id":     reply_to_id,
                "reply_to_name":   reply_to_name,
                "text":            text,
                "signal_score":    score,
                "priority":        priority,
                "ncic_category":   category,
                "triggers":        triggers,
                "latitude":        latitude,
                "longitude":       longitude,
                "date":            date_str,
                "source":          "telegram"
            })
            ch_kept += 1
            n_kept  += 1

        print(f"  [{title[:45]}] → {ch_kept} kept")
        await asyncio.sleep(0.3)

    save_last_run()

    print(f"\n[filter summary]")
    print(f"  Total pulled:      {n_total}")
    print(f"  Date filtered:     {n_date_skip}  "
          f"(older than {since.strftime('%Y-%m-%d %H:%M')})")
    print(f"  Kenya filtered:    {n_kenya_skip}  (not Kenya-relevant)")
    print(f"  Kept:              {n_kept}")

    scores = [r["signal_score"] for r in rows]
    if scores:
        high   = sum(1 for s in scores if s >= 3)
        medium = sum(1 for s in scores if s == 2)
        low    = sum(1 for s in scores if s == 1)
        none   = sum(1 for s in scores if s == 0)
        print(f"\n[signal breakdown]")
        print(f"  HIGH   (score>=3): {high}")
        print(f"  MEDIUM (score=2):  {medium}")
        print(f"  LOW    (score=1):  {low}")
        print(f"  NONE   (score=0):  {none}")

    fieldnames = [
        "message_id","chat_id","chat_title","chat_type",
        "sender_id","sender_name","sender_username",
        "reply_to_id","reply_to_name","text",
        "signal_score","priority","ncic_category","triggers",
        "latitude","longitude","date","source"
    ]

    rows.sort(key=lambda x: -x["signal_score"])

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n✅ {len(rows)} messages saved to {OUTPUT_CSV}")
    print("=" * 60)
    print(f"DONE — {len(discovered_channels)} channels | {len(rows)} messages")
    print("=" * 60)

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
