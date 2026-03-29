# ================================================================
#  pipeline_youtube.R — YouTube → Supabase Ingestion Pipeline
#  Radicalisation Signals · IEA Kenya NIRU AI Hackathon
#
#  v1 — Basic comment ingestion, static queries
#  v2 — Full feature parity with pipeline_telegram:
#    FIX 1 — Dynamic queries:   Reads active queries from Supabase
#                               search_queries table at startup
#    FIX 2 — Dynamic keywords:  Reads approved keywords from
#                               keyword_bank, merges into TIER1/2/3
#    FIX 3 — Signal scoring:    TIER1/2/3 keyword scoring on every
#                               comment (score, priority, triggers)
#    FIX 4 — NCIC category:     Maps triggers to NCIC taxonomy
#    FIX 5 — Noise filter:      Rejects non-Kenya channels
#    FIX 6 — Kenya filter:      Tightened — unknown channel needs
#                               title match, not just either
#    FIX 7 — Geo coordinates:   3-tier resolution per comment:
#                               (1) video geotag → (2) text inference
#                               → (3) Kenya centroid fallback
#    FIX 8 — MIN_SIGNAL_SCORE:  Only stores comments with score ≥ 1
#    FIX 9 — Signal summary:    Top preview sorted by signal_score
#
#  CACHING STRATEGY (saves YouTube API quota):
#    cache/yt_video_cache.rds      — scored videos found per query (daily)
#    cache/yt_pulled_videos.rds    — video IDs already pulled comments from
#    cache/yt_hash_cache.rds       — Supabase text hashes (TTL: 60 min)
#    cache/yt_failed_batches/      — failed Supabase inserts for inspection
#    cache/kw_bank_cache.json      — approved keywords (shared with Telegram)
#    cache/search_queries_cache.json — active queries (shared with Telegram)
#
#  QUOTA GUARD:
#    Daily limit: 10,000 units. Script stops at 8,000 (2,000 buffer).
#    Search = 100 units | Stats+Geo = 1/video | Comments = 1/video
#
#  Usage: source("pipeline_youtube.R")
# ================================================================

readRenviron("secrets/.Renviron")
library(httr2)
library(jsonlite)
library(digest)

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

# ── CREDENTIALS ───────────────────────────────────────────────────
yt_key   <- Sys.getenv("YOUTUBE_API_KEY")
supa_url <- Sys.getenv("SUPABASE_URL")
supa_key <- Sys.getenv("SUPABASE_KEY")

if (nchar(yt_key)   == 0) stop("YOUTUBE_API_KEY not found in secrets/.Renviron")
if (nchar(supa_url) == 0) stop("SUPABASE_URL not found in secrets/.Renviron")
if (nchar(supa_key) == 0) stop("SUPABASE_KEY not found in secrets/.Renviron")
message("✅ Credentials loaded")

# ── CONFIGURATION ─────────────────────────────────────────────────
MAX_VIDEOS_PER_QUERY   <- 8L    # videos to score per query
MIN_COMMENTS_TO_PULL   <- 50L   # only pull videos with this many+ comments
COMMENTS_PER_VIDEO     <- 50L   # max comments per video
API_PAUSE_SECS         <- 0.5   # pause between API calls (rate limit safety)
DAILY_QUOTA_LIMIT      <- 8000L # stop before hitting 10,000 unit hard limit
HASH_CACHE_TTL_MINS    <- 60L   # refresh Supabase hash cache after N minutes
MIN_SIGNAL_SCORE       <- 1L    # FIX 8: only store comments with score >= 1
KEYWORD_BANK_TTL_MINS  <- 60L   # FIX 2: keyword bank cache TTL
SEARCH_QUERIES_TTL_MINS <- 60L  # FIX 1: search queries cache TTL

CACHE_DIR             <- "cache"
VIDEO_CACHE_FILE      <- file.path(CACHE_DIR, "yt_video_cache.rds")
PULLED_LOG_FILE       <- file.path(CACHE_DIR, "yt_pulled_videos.rds")
HASH_CACHE_FILE       <- file.path(CACHE_DIR, "yt_hash_cache.rds")
FAILED_BATCH_DIR      <- file.path(CACHE_DIR, "yt_failed_batches")
KEYWORD_BANK_CACHE    <- file.path(CACHE_DIR, "kw_bank_cache.json")
SEARCH_QUERIES_CACHE  <- file.path(CACHE_DIR, "search_queries_cache.json")

# ── STATIC FALLBACK QUERIES ───────────────────────────────────────
# Used only when Supabase search_queries table is unreachable.
# Active queries are loaded dynamically at startup (FIX 1).
# ── STATIC FALLBACK QUERIES ───────────────────────────────────────
# NOTE: Do NOT include individual politician names here.
# Including names (e.g. "Ruto", "Raila") creates selection bias —
# opposition-related content would be over-represented relative to
# pro-government content and vice-versa, which could skew NCIC analysis.
# Content-based and theme-based queries only; dynamic queries from
# the officer dashboard (search_queries table) can be more targeted.
STATIC_QUERIES <- list(
  # ── KENYA ────────────────────────────────────────────────────
  list(q="waende kwao Kenya kabila",            priority="HIGH"),
  # ── EAST AFRICA REGIONAL ────────────────────────────────────
  list(q="Somalia al-shabaab attack Kenya",     priority="HIGH"),
  list(q="Ethiopia Tigray ethnic violence",     priority="HIGH"),
  list(q="Uganda opposition crackdown hate",    priority="HIGH"),
  list(q="Tanzania hate speech ethnic",         priority="HIGH"),
  list(q="Rwanda genocide incitement 2027",     priority="HIGH"),
  list(q="Burundi ethnic violence conflict",    priority="HIGH"),
  list(q="East Africa election violence hate",  priority="MEDIUM"),
  list(q="Horn of Africa ethnic conflict",      priority="MEDIUM"),
  list(q="tribal hate Kenya incitement 2027",   priority="HIGH"),
  list(q="ethnic violence Kenya community",     priority="HIGH"),
  list(q="kabila Kenya siasa conflict",         priority="HIGH"),
  list(q="uchaguzi Kenya rigging tribe 2027",   priority="MEDIUM"),
  list(q="coast Kenya pwani independence",      priority="MEDIUM"),
  list(q="Mt Kenya Rift Valley conflict land",  priority="MEDIUM"),
  list(q="northern Kenya pastoralists attack",  priority="MEDIUM"),
  list(q="Kenya 2027 election incitement hate", priority="LOW"),
  list(q="Kenya parliament corruption tribe",   priority="LOW")
)

# ── FIX 5: NOISE CHANNEL FILTER ──────────────────────────────────
NOISE_PATTERNS <- c(
  "crypto","bitcoin","forex","investment","trading",
  "real estate","property","jobs","job links","vacancies",
  "employment","hiring","sugar mum","divas","adult",
  "hiphop","music","entertainment","comedy","jokes",
  "farming","agriculture","food","market","produce",
  "polytechnic","university","college","study abroad",
  "ghana","togo","ivory coast","congo","south africa",
  "ethiopia","malawi","morocco","tanzania","nigeria"
)

is_noise_channel <- function(title) {
  t <- tolower(title)
  any(sapply(NOISE_PATTERNS, function(n) grepl(n, t, fixed=TRUE)))
}

# ── KNOWN KENYAN CHANNELS ─────────────────────────────────────────
KENYA_CHANNELS <- c(
  "ktn","ntv kenya","citizen tv","k24","capital fm","the star kenya",
  "nairobi","kenya","kisumu","mombasa","nakuru","kbc","standard media",
  "nation","daily nation","tuko","pulselive kenya","kenya news",
  "plug tv","kenya newsline","maisha tv","kenya citizen tv",
  "inooro","kameme","muuga","meru fm","egesa fm","west fm",
  "royal media","ramogi","safari news","kenyans.co.ke","the standard",
  "people daily","business daily","kenya broadcasting"
)

KENYA_TITLE_MARKERS <- c(
  "kenya","nairobi","ruto","raila","uhuru","kenyans","odinga",
  "uchaguzi","kabila","serikali","wakenya","iebc","jubilee",
  "azimio","gachagua","kalonzo","mudavadi","wetangula",
  "kisumu","mombasa","nakuru","eldoret","kiambu","machakos",
  "kenyan parliament","bunge kenya","kenya national assembly"
)

# FIX 6: Unknown channel requires title match (prevents haka false positives)
is_kenya_content <- function(channel, title) {
  ch  <- tolower(channel)
  ttl <- tolower(title)
  channel_match <- any(sapply(KENYA_CHANNELS,      function(k) grepl(k, ch,  fixed=TRUE)))
  title_match   <- any(sapply(KENYA_TITLE_MARKERS, function(m) grepl(m, ttl, fixed=TRUE)))
  if (channel_match) return(TRUE)   # known Kenya channel — trust it
  title_match                       # unknown channel — require title match
}

# ── FIX 7: KENYA GEO LOOKUP TABLE ────────────────────────────────
# County centroids + major towns → lat/long
# Used for text-inference geo resolution
KENYA_GEO <- list(
  # 47 Counties
  list(pattern="nairobi",    lat=-1.2921, lng=36.8219),
  list(pattern="mombasa",    lat=-4.0435, lng=39.6682),
  list(pattern="kisumu",     lat=-0.1022, lng=34.7617),
  list(pattern="nakuru",     lat=-0.3031, lng=36.0800),
  list(pattern="eldoret",    lat=0.5143,  lng=35.2698),
  list(pattern="kiambu",     lat=-1.0312, lng=36.8312),
  list(pattern="machakos",   lat=-1.5177, lng=37.2634),
  list(pattern="meru",       lat=0.0470,  lng=37.6496),
  list(pattern="nyeri",      lat=-0.4167, lng=36.9500),
  list(pattern="kakamega",   lat=0.2827,  lng=34.7519),
  list(pattern="kisii",      lat=-0.6817, lng=34.7660),
  list(pattern="kericho",    lat=-0.3686, lng=35.2863),
  list(pattern="thika",      lat=-1.0332, lng=37.0693),
  list(pattern="garissa",    lat=-0.4532, lng=39.6461),
  list(pattern="kitale",     lat=1.0154,  lng=35.0062),
  list(pattern="malindi",    lat=-3.2138, lng=40.1169),
  list(pattern="lamu",       lat=-2.2694, lng=40.9024),
  list(pattern="wajir",      lat=1.7471,  lng=40.0573),
  list(pattern="mandera",    lat=3.9366,  lng=41.8670),
  list(pattern="marsabit",   lat=2.3284,  lng=37.9897),
  list(pattern="isiolo",     lat=0.3542,  lng=37.5820),
  list(pattern="turkana",    lat=3.7781,  lng=35.5975),
  list(pattern="west pokot", lat=1.2442,  lng=35.1167),
  list(pattern="samburu",    lat=1.2052,  lng=36.9102),
  list(pattern="trans nzoia", lat=1.0569, lng=34.9501),
  list(pattern="uasin gishu", lat=0.5204, lng=35.2699),
  list(pattern="elgeyo marakwet", lat=0.7833, lng=35.5833),
  list(pattern="nandi",      lat=0.1835,  lng=35.1017),
  list(pattern="baringo",    lat=0.8267,  lng=35.9819),
  list(pattern="laikipia",   lat=0.3606,  lng=36.7819),
  list(pattern="nyandarua",  lat=-0.4068, lng=36.5984),
  list(pattern="murang'a",   lat=-0.7833, lng=37.0333),
  list(pattern="kirinyaga",  lat=-0.5596, lng=37.3556),
  list(pattern="embu",       lat=-0.5300, lng=37.4500),
  list(pattern="tharaka nithi", lat=0.3000, lng=38.0500),
  list(pattern="kitui",      lat=-1.3667, lng=38.0167),
  list(pattern="makueni",    lat=-2.2588, lng=37.8944),
  list(pattern="kajiado",    lat=-2.0982, lng=36.7819),
  list(pattern="narok",      lat=-1.0834, lng=35.8719),
  list(pattern="bomet",      lat=-0.7847, lng=35.3417),
  list(pattern="nyamira",    lat=-0.5667, lng=34.9333),
  list(pattern="migori",     lat=-1.0634, lng=34.4731),
  list(pattern="homa bay",   lat=-0.5273, lng=34.4571),
  list(pattern="siaya",      lat=0.0607,  lng=34.2880),
  list(pattern="bungoma",    lat=0.5635,  lng=34.5606),
  list(pattern="busia",      lat=0.4606,  lng=34.1112),
  list(pattern="vihiga",     lat=0.0500,  lng=34.7167),
  list(pattern="kwale",      lat=-4.1740, lng=39.4524),
  list(pattern="kilifi",     lat=-3.6305, lng=39.8499),
  list(pattern="taita taveta", lat=-3.3167, lng=38.3500),
  list(pattern="tana river", lat=-1.5000, lng=39.9167),
  list(pattern="rift valley", lat=0.5204,  lng=35.2699),
  list(pattern="coast",      lat=-3.0,    lng=40.0),
  list(pattern="nyanza",     lat=-0.5,    lng=34.5),
  list(pattern="western",    lat=0.5,     lng=34.7),
  list(pattern="central",    lat=-0.5,    lng=37.0),
  list(pattern="eastern",    lat=0.0,     lng=38.0),
  list(pattern="north eastern", lat=2.0,  lng=40.0),
  list(pattern="pwani",      lat=-3.0,    lng=40.0),
  list(pattern="mt kenya",   lat=-0.1521, lng=37.3084)
)

# FIX 7: Three-tier geo resolution
# FIX-A: geo_confidence column ("high"/"medium"/"low") so the dashboard
#         can visually distinguish precise geotags from centroid fallbacks
#         that should not be treated as accurate location data.
resolve_geo <- function(text, video_lat=NA, video_lng=NA) {

  # Tier 1 — video has a geotag (most precise)
  if (!is.na(video_lat) && !is.na(video_lng) &&
      video_lat != 0 && video_lng != 0) {
    return(list(lat=video_lat, lng=video_lng,
                source="video_geotag", geo_confidence="high"))
  }

  # Tier 2 — scan text for county/town name
  txt <- tolower(text)
  for (loc in KENYA_GEO) {
    if (grepl(loc$pattern, txt, fixed=TRUE)) {
      return(list(lat=loc$lat, lng=loc$lng,
                  source="text_inference", geo_confidence="medium"))
    }
  }

  # Tier 3 — Kenya centroid fallback
  # NOTE: This is a country-level placeholder, NOT a meaningful location.
  # The dashboard should show these with reduced opacity / different marker style.
  list(lat=-0.0236, lng=37.9062,
       source="country_centroid", geo_confidence="low")
}

# ── FIX 2: TIER KEYWORDS ─────────────────────────────────────────
# Static base lists — dynamic approved keywords are merged in at startup

TIER3 <- c(
  "kill them","kill all","eliminate them","wipe them out",
  "drive them out","ethnic cleansing","cleanse them",
  "exterminate","slaughter them","burn their homes",
  "attack the community","armed uprising","take up arms",
  "waende kwao","wauawe","wachinjwe","angamizwa",
  "ondokeni","chomwa moto","wafukuzwe","waondolewe",
  "waangamizwe","watupwe nje","chukua silaha","piga vita",
  "wamaliza","waangush","cockroaches","vermin","parasites",
  "sub-human","these animals","not human","nyoka hawa",
  "mende hawa","panya hawa","hawa wanyama","hawastahili kuishi",
  "jihad","holy war","vita vitakatifu","muua kafiri"
)

TIER2 <- c(
  "jaluo","madoadoa","kabila chafu","kabila mbaya",
  "kabila ya wezi","gikuyu wezi","kalenjin wauaji",
  "luhya wajinga","kamba wabaya","somali haramu",
  "tribe of thieves","tribe of killers","dirty tribe",
  "si wakenya","wahamie kwao","toka kwetu","hawatakiwi hapa",
  "not real kenyans","foreigners in our land","send them back",
  "tujitenganishe","secede from kenya","break away from kenya",
  "pwani si kenya","tribal rigging","kabila iliibia",
  "uchaguzi wa ukabila","stolen by tribe","vote along tribal lines",
  "piga kura ya kabila","dini chafu","religion of evil",
  "waislamu ni hatari","wakristo ni maadui","dini ya shetani"
)

TIER1 <- c(
  "kabila","ukabila","makabila","kikabila","tribalism",
  "tribal politics","ethnic tension","community conflict",
  "inter-ethnic","inter-tribal","ghasia za uchaguzi",
  "vurugu za uchaguzi","election violence","community clashes",
  "land conflict","chuki","uchochezi","maneno ya chuki",
  "ubaguzi","kudharau","hate speech","incitement","discrimination",
  "mgawanyiko wa taifa","divide kenya","undermine national unity",
  "religious tension","interfaith conflict"
)

# ── FIX 2: DYNAMIC KEYWORD BANK ──────────────────────────────────
# Identical pattern to pipeline_telegram.py — reads approved keywords
# from Supabase and merges into TIER1/2/3 at startup.

.kw_cache_valid <- function() {
  if (!file.exists(KEYWORD_BANK_CACHE)) return(NULL)
  tryCatch({
    data <- fromJSON(KEYWORD_BANK_CACHE, flatten=TRUE)
    saved <- as.POSIXct(data$saved_at, tz="UTC")
    if (as.numeric(difftime(Sys.time(), saved, units="mins")) > KEYWORD_BANK_TTL_MINS)
      return(NULL)
    data$keywords
  }, error=function(e) NULL)
}

.fetch_keyword_bank <- function() {
  cached <- .kw_cache_valid()
  if (!is.null(cached)) {
    message(sprintf("  [keyword bank] cache HIT — %d approved keywords", nrow(cached)))
    return(cached)
  }
  tryCatch({
    resp <- request(paste0(supa_url, "/rest/v1/keyword_bank")) |>
      req_headers("apikey"=supa_key, "Authorization"=paste("Bearer", supa_key)) |>
      req_url_query(select="keyword,tier,category", status="eq.approved", limit=1000) |>
      req_error(is_error=\(r) FALSE) |>
      req_perform() |>
      resp_body_string()
    df <- fromJSON(resp, flatten=TRUE)
    write(toJSON(list(
      saved_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
      keywords = df
    ), auto_unbox=TRUE), KEYWORD_BANK_CACHE)
    message(sprintf("  [keyword bank] fetched %d approved keywords from Supabase", nrow(df)))
    df
  }, error=function(e) {
    message(sprintf("  [keyword bank] ⚠ fetch failed (%s) — static lists only", e$message))
    data.frame()
  })
}

merge_keyword_bank <- function() {
  df <- .fetch_keyword_bank()
  if (nrow(df) == 0) return(c(0L, 0L, 0L))
  added <- c(0L, 0L, 0L)
  for (i in seq_len(nrow(df))) {
    kw   <- tolower(trimws(df$keyword[i]))
    tier <- suppressWarnings(as.integer(df$tier[i]))
    if (!nzchar(kw) || is.na(tier) || !tier %in% 1:3) next
    if (tier == 3 && !kw %in% TIER3) { TIER3 <<- c(TIER3, kw); added[3] <- added[3]+1L }
    if (tier == 2 && !kw %in% TIER2) { TIER2 <<- c(TIER2, kw); added[2] <- added[2]+1L }
    if (tier == 1 && !kw %in% TIER1) { TIER1 <<- c(TIER1, kw); added[1] <- added[1]+1L }
  }
  added
}

# ── FIX 1: DYNAMIC SEARCH QUERIES ────────────────────────────────
.sq_cache_valid <- function() {
  if (!file.exists(SEARCH_QUERIES_CACHE)) return(NULL)
  tryCatch({
    data <- fromJSON(SEARCH_QUERIES_CACHE, flatten=TRUE)
    saved <- as.POSIXct(data$saved_at, tz="UTC")
    if (as.numeric(difftime(Sys.time(), saved, units="mins")) > SEARCH_QUERIES_TTL_MINS)
      return(NULL)
    data$queries
  }, error=function(e) NULL)
}

fetch_active_queries <- function() {
  cached <- .sq_cache_valid()
  if (!is.null(cached) && nrow(cached) > 0) {
    message(sprintf("  [search queries] cache HIT — %d active queries", nrow(cached)))
    return(cached)
  }
  tryCatch({
    resp <- request(paste0(supa_url, "/rest/v1/search_queries")) |>
      req_headers("apikey"=supa_key, "Authorization"=paste("Bearer", supa_key)) |>
      req_url_query(select="query,priority", status="eq.active", limit=500) |>
      req_error(is_error=\(r) FALSE) |>
      req_perform() |>
      resp_body_string()
    df <- fromJSON(resp, flatten=TRUE)
    message(sprintf("  [search queries] fetched %d active queries from Supabase", nrow(df)))
    df
  }, error=function(e) {
    message(sprintf("  [search queries] ⚠ fetch failed (%s) — static queries only", e$message))
    data.frame()
  })
}

build_query_list <- function() {
  df <- fetch_active_queries()
  if (nrow(df) == 0) {
    message("  [search queries] Using static fallback queries")
    return(STATIC_QUERIES)
  }
  static_qs <- sapply(STATIC_QUERIES, function(x) tolower(trimws(x$q)))
  dynamic   <- lapply(seq_len(nrow(df)), function(i) {
    list(q=df$query[i], priority=df$priority[i] %||% "MEDIUM")
  })
  # Merge: keep all static + add dynamic ones not already present
  existing  <- static_qs
  new_items <- Filter(function(x) !tolower(trimws(x$q)) %in% existing, dynamic)
  merged    <- c(STATIC_QUERIES, new_items)
  message(sprintf("  [search queries] %d total (%d static + %d dynamic)",
                  length(merged), length(STATIC_QUERIES), length(new_items)))
  merged
}

# ── FIX 3 & 4: SIGNAL SCORING ────────────────────────────────────
score_comment <- function(text) {
  txt      <- tolower(text)
  score    <- 0L
  triggers <- character()
  for (kw in TIER3) if (grepl(kw, txt, fixed=TRUE)) {
    score <- score + 3L; triggers <- c(triggers, paste0("[T3]", kw))
  }
  for (kw in TIER2) if (grepl(kw, txt, fixed=TRUE)) {
    score <- score + 2L; triggers <- c(triggers, paste0("[T2]", kw))
  }
  for (kw in TIER1) if (grepl(kw, txt, fixed=TRUE)) {
    score <- score + 1L; triggers <- c(triggers, paste0("[T1]", kw))
  }
  score    <- min(score, 10L)
  priority <- if (score >= 3) "HIGH" else if (score >= 2) "MEDIUM" else
              if (score >= 1) "LOW"  else "NONE"
  list(score=score, priority=priority, triggers=paste(triggers, collapse="; "))
}

ncic_category <- function(triggers) {
  if (!nzchar(triggers)) return("NONE")
  t <- tolower(triggers)
  if (any(sapply(c("wauawe","waangamizwe","kill","eliminate","wipe","uprising",
                   "jihad","silaha","piga vita"), function(k) grepl(k, t, fixed=TRUE))))
    return("INCITEMENT")
  if (any(sapply(c("nyoka","mende","panya","cockroach","vermin","wanyama",
                   "wadudu","sub-human","not human"), function(k) grepl(k, t, fixed=TRUE))))
    return("DEHUMANISATION")
  if (any(sapply(c("secede","tujitenganishe","break away","pwani si kenya","jamhuri yetu"),
                 function(k) grepl(k, t, fixed=TRUE))))
    return("SECESSIONISM")
  if (any(sapply(c("tribal rigging","uchaguzi wa ukabila","stolen by tribe","vote along tribal"),
                 function(k) grepl(k, t, fixed=TRUE))))
    return("ELECTION_INCITEMENT")
  if (any(sapply(c("dini chafu","religion of evil","adui wa dini","waislamu","wakristo ni maadui"),
                 function(k) grepl(k, t, fixed=TRUE))))
    return("RELIGIOUS_HATRED")
  if (any(sapply(c("jaluo","madoadoa","kabila chafu","si wakenya","not real kenyans","wahamie"),
                 function(k) grepl(k, t, fixed=TRUE))))
    return("ETHNIC_CONTEMPT")
  if (any(sapply(c("hate speech","chuki","ubaguzi","discrimination","uchochezi"),
                 function(k) grepl(k, t, fixed=TRUE))))
    return("HATE_SPEECH")
  if (any(sapply(c("kabila","tribal","ethnic","ukabila","makabila"),
                 function(k) grepl(k, t, fixed=TRUE))))
    return("ETHNIC_TENSION")
  "DIVISIVE_CONTENT"
}

# ── QUOTA GUARD ───────────────────────────────────────────────────
units_used <- 0L

charge_quota <- function(units, label="") {
  units_used <<- units_used + as.integer(units)
  if (units_used >= DAILY_QUOTA_LIMIT)
    stop(sprintf(
      "Quota guard: %d/%d units used%s — stopping to stay under daily limit",
      units_used, DAILY_QUOTA_LIMIT,
      if (nchar(label) > 0) paste0(" (", label, ")") else ""
    ))
}

# ── CACHE SETUP ───────────────────────────────────────────────────
if (!dir.exists(CACHE_DIR))        dir.create(CACHE_DIR, recursive=TRUE)
if (!dir.exists(FAILED_BATCH_DIR)) dir.create(FAILED_BATCH_DIR, recursive=TRUE)

# ── VIDEO CACHE ───────────────────────────────────────────────────
load_video_cache <- function() {
  if (!file.exists(VIDEO_CACHE_FILE)) {
    return(data.frame(
      video_id=character(), title=character(), channel=character(),
      comment_count=integer(), view_count=integer(), like_count=integer(),
      signal_score=numeric(), video_lat=numeric(), video_lng=numeric(),
      query=character(), cached_at=character(),
      stringsAsFactors=FALSE))
  }
  cache <- readRDS(VIDEO_CACHE_FILE)
  # Forward-compatibility: add geo columns if loaded from old schema
  if (!"video_lat" %in% names(cache)) cache$video_lat <- NA_real_
  if (!"video_lng" %in% names(cache)) cache$video_lng <- NA_real_
  cache
}

# Pass existing cache in to avoid double file reads
save_video_cache <- function(new_videos, query_label, existing_cache) {
  new_videos$query     <- query_label
  new_videos$cached_at <- format(Sys.Date(), "%Y-%m-%d")
  combined <- rbind(existing_cache, new_videos)
  combined <- combined[!duplicated(combined$video_id), ]
  saveRDS(combined, VIDEO_CACHE_FILE)
  combined  # return updated cache for in-memory use
}

# ── PULLED VIDEO LOG ──────────────────────────────────────────────
load_pulled_log <- function() {
  if (file.exists(PULLED_LOG_FILE)) readRDS(PULLED_LOG_FILE)
  else data.frame(
    video_id=character(), pulled_at=character(),
    n_comments=integer(), stringsAsFactors=FALSE)
}

# Save the entire log at once (called once at end, not per video)
save_pulled_log <- function(log) {
  saveRDS(log, PULLED_LOG_FILE)
}

# Update log in memory — caller saves when done
update_pulled_log <- function(log, video_id, n_comments) {
  ts <- format(Sys.time(), "%Y-%m-%d %H:%M")
  if (video_id %in% log$video_id) {
    log$pulled_at[log$video_id  == video_id] <- ts
    log$n_comments[log$video_id == video_id] <- n_comments
  } else {
    log <- rbind(log, data.frame(
      video_id=video_id, pulled_at=ts,
      n_comments=n_comments, stringsAsFactors=FALSE))
  }
  log
}

# ── SUPABASE HASH CACHE ───────────────────────────────────────────
load_hash_cache <- function() {
  if (!file.exists(HASH_CACHE_FILE)) return(NULL)
  cache <- readRDS(HASH_CACHE_FILE)
  age   <- as.numeric(difftime(Sys.time(),
                                as.POSIXct(cache$saved_at), units="mins"))
  if (age > HASH_CACHE_TTL_MINS) {
    message(sprintf("[hash cache] Expired (%.0f min old) — refreshing", age))
    return(NULL)
  }
  message(sprintf("[hash cache] HIT — %d hashes (%.0f min old)",
                  length(cache$hashes), age))
  cache$hashes
}

save_hash_cache <- function(hashes) {
  saveRDS(list(hashes=hashes,
               saved_at=format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
          HASH_CACHE_FILE)
}

append_hash_cache <- function(new_hashes) {
  if (!file.exists(HASH_CACHE_FILE)) return(invisible(NULL))
  cache    <- readRDS(HASH_CACHE_FILE)
  combined <- unique(c(cache$hashes, new_hashes))
  saveRDS(list(hashes=combined, saved_at=cache$saved_at), HASH_CACHE_FILE)
}

# ── CACHE STATUS REPORT ───────────────────────────────────────────
print_cache_status <- function() {
  v  <- load_video_cache()
  p  <- load_pulled_log()
  h  <- if (file.exists(HASH_CACHE_FILE))
          readRDS(HASH_CACHE_FILE)
        else list(hashes=character(), saved_at="never")
  message(sprintf(
    "[cache] %d videos known | %d pulled | %d hashes (saved: %s)",
    nrow(v), nrow(p), length(h$hashes), h$saved_at))
  if (nrow(p) > 0) {
    message("[cache] Last 5 pulled:")
    recent <- tail(p[order(p$pulled_at), ], 5)
    for (i in seq_len(nrow(recent)))
      message(sprintf("        %-22s | %3d comments | %s",
                      recent$video_id[i], recent$n_comments[i],
                      recent$pulled_at[i]))
  }
}

# ── SUPABASE HELPERS ──────────────────────────────────────────────
supa_insert <- function(table, rows_df) {
  request(paste0(supa_url, "/rest/v1/", table)) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key),
      "Content-Type"  = "application/json",
      "Prefer"        = "return=minimal"
    ) |>
    req_body_json(jsonlite::fromJSON(jsonlite::toJSON(rows_df, na="null"))) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
}

# Paginated fetch — handles >1,000 rows correctly
supa_get_existing_hashes <- function(table) {
  all_hashes <- character(0)
  offset     <- 0L
  page_size  <- 1000L

  repeat {
    resp <- request(paste0(supa_url, "/rest/v1/", table)) |>
      req_headers(
        "apikey"        = supa_key,
        "Authorization" = paste("Bearer", supa_key),
        "Range-Unit"    = "items",
        "Range"         = sprintf("%d-%d", offset, offset + page_size - 1L)
      ) |>
      req_url_query(select="text_hash") |>
      req_error(is_error = \(r) FALSE) |>
      req_perform() |>
      resp_body_json(simplifyVector=TRUE)

    if (!is.data.frame(resp) || nrow(resp) == 0) break
    all_hashes <- c(all_hashes, resp$text_hash)
    if (nrow(resp) < page_size) break
    offset <- offset + page_size
  }
  all_hashes
}

supa_count <- function(table) {
  resp <- request(paste0(supa_url, "/rest/v1/", table)) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key),
      "Prefer"        = "count=exact"
    ) |>
    req_url_query(select="id", limit=1) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
  cr <- tryCatch(resp_header(resp, "content-range"), error=function(e) "0/0")
  as.integer(gsub(".*/", "", cr %||% "0")) %||% 0L
}

# ── YOUTUBE: SCORE VIDEOS ─────────────────────────────────────────
# Returns scored video data frame. Uses daily video cache if available.
# FIX 7: Fetches recordingDetails for video geotag (lat/long).
score_videos <- function(query, max_videos=MAX_VIDEOS_PER_QUERY,
                         video_cache) {
  today <- format(Sys.Date(), "%Y-%m-%d")

  # Cache hit — same query already searched today
  cached <- video_cache[video_cache$query == query &
                         video_cache$cached_at == today, ]
  if (nrow(cached) > 0) {
    message(sprintf("  [cache HIT] %d videos (no API call)", nrow(cached)))
    return(cached)
  }

  # Cache miss — call search API (100 units)
  message("  [cache MISS] Calling YouTube search API...")
  charge_quota(100, "search")
  Sys.sleep(API_PAUSE_SECS)

  search_resp <- tryCatch(
    request("https://www.googleapis.com/youtube/v3/search") |>
      req_url_query(
        key               = yt_key,
        q                 = query,
        part              = "snippet",
        type              = "video",
        maxResults        = max_videos,
        order             = "relevance",
        regionCode        = "KE",
        relevanceLanguage = "sw"
      ) |>
      req_error(is_error = \(r) FALSE) |>
      req_perform() |>
      resp_body_json(),
    error = function(e) NULL
  )

  if (is.null(search_resp) || is.null(search_resp$items))
    return(data.frame())

  items     <- Filter(function(v) !is.null(v$id$videoId), search_resp$items)
  video_ids <- sapply(items, function(v) v$id$videoId)
  if (length(video_ids) == 0) return(data.frame())

  # Fetch stats + recordingDetails for geo (1 unit per video)
  charge_quota(length(video_ids), "stats+geo")
  Sys.sleep(API_PAUSE_SECS)

  stats_resp <- tryCatch(
    request("https://www.googleapis.com/youtube/v3/videos") |>
      req_url_query(
        key  = yt_key,
        id   = paste(video_ids, collapse=","),
        part = "statistics,snippet,recordingDetails"
      ) |>
      req_error(is_error = \(r) FALSE) |>
      req_perform() |>
      resp_body_json(),
    error = function(e) NULL
  )

  if (is.null(stats_resp) || is.null(stats_resp$items))
    return(data.frame())

  videos <- do.call(rbind, lapply(stats_resp$items, function(v) {
    # Extract video geotag if present
    geo  <- v$recordingDetails$location
    vlat <- if (!is.null(geo$latitude))  as.numeric(geo$latitude)  else NA_real_
    vlng <- if (!is.null(geo$longitude)) as.numeric(geo$longitude) else NA_real_
    data.frame(
      video_id      = v$id %||% "",
      title         = substr(v$snippet$title %||% "", 1, 70),
      channel       = v$snippet$channelTitle %||% "",
      comment_count = as.integer(v$statistics$commentCount %||% 0),
      view_count    = as.integer(v$statistics$viewCount    %||% 0),
      like_count    = as.integer(v$statistics$likeCount    %||% 0),
      video_lat     = vlat,
      video_lng     = vlng,
      stringsAsFactors = FALSE
    )
  }))

  # FIX 5+6: Noise + tightened Kenya filter
  videos <- videos[!sapply(videos$channel, is_noise_channel), ]
  if (nrow(videos) == 0) return(data.frame())

  videos$is_kenya <- mapply(is_kenya_content, videos$channel, videos$title)
  kenya_vids <- videos[videos$is_kenya, ]

  if (nrow(kenya_vids) == 0) {
    message("  [filter] No Kenya videos found — keeping all results")
    kenya_vids <- videos
  } else {
    n_removed <- nrow(videos) - nrow(kenya_vids)
    if (n_removed > 0)
      message(sprintf("  [filter] Removed %d non-Kenya video(s)", n_removed))
  }

  kenya_vids$signal_score <- kenya_vids$comment_count * 0.7 +
                             kenya_vids$view_count    * 0.0001
  kenya_vids[order(-kenya_vids$signal_score), ]
}

# ── YOUTUBE: PULL COMMENTS ────────────────────────────────────────
# Skips videos already in pulled_log.
# FIX 3/4/7/8: Scores each comment, resolves geo, filters by MIN_SIGNAL_SCORE.
pull_comments <- function(scored_videos, pulled_log,
                          min_comments       = MIN_COMMENTS_TO_PULL,
                          comments_per_video = COMMENTS_PER_VIDEO,
                          query_label        = "") {

  worthy <- scored_videos[scored_videos$comment_count >= min_comments, ]
  if (nrow(worthy) == 0) return(list(comments=data.frame(), log=pulled_log))

  # Skip videos already pulled
  new_worthy <- worthy[!worthy$video_id %in% pulled_log$video_id, ]
  n_skipped  <- nrow(worthy) - nrow(new_worthy)

  if (n_skipped > 0)
    message(sprintf("  [cache] Skipping %d already-pulled video(s)", n_skipped))
  if (nrow(new_worthy) == 0) {
    message("  [cache] All videos already pulled — nothing new")
    return(list(comments=data.frame(), log=pulled_log))
  }

  message(sprintf("  Pulling from %d new video(s) [%d/%d qualify]",
                  nrow(new_worthy), nrow(worthy), nrow(scored_videos)))

  all_comments <- list()

  for (i in seq_len(nrow(new_worthy))) {
    vid      <- new_worthy$video_id[i]
    title    <- new_worthy$title[i]
    vlat     <- new_worthy$video_lat[i] %||% NA_real_
    vlng     <- new_worthy$video_lng[i] %||% NA_real_

    charge_quota(1L, sprintf("comments:%s", vid))
    Sys.sleep(API_PAUSE_SECS)

    comm_resp <- tryCatch(
      request("https://www.googleapis.com/youtube/v3/commentThreads") |>
        req_url_query(
          key        = yt_key,
          videoId    = vid,
          part       = "snippet",
          maxResults = comments_per_video,
          order      = "relevance"
        ) |>
        req_error(is_error = \(r) FALSE) |>
        req_perform() |>
        resp_body_json(),
      error = function(e) NULL
    )

    if (is.null(comm_resp) || !is.null(comm_resp$error) ||
        is.null(comm_resp$items)) {
      message(sprintf("  ⚠ Comments disabled: %s", substr(title, 1, 40)))
      pulled_log <- update_pulled_log(pulled_log, vid, 0)
      next
    }

    kept <- 0L
    for (item in comm_resp$items) {
      s    <- item$snippet$topLevelComment$snippet
      text <- trimws(s$textOriginal %||% "")

      if (nchar(text) < 15)         next
      if (!grepl("[a-zA-Z]", text)) next

      # FIX 3 & 4: Score comment + assign NCIC category
      sig      <- score_comment(text)
      category <- ncic_category(sig$triggers)

      # FIX 8: Skip comments with no signal
      if (sig$score < MIN_SIGNAL_SCORE) next

      # FIX 7: Resolve geo — video geotag → text inference → centroid
      geo <- resolve_geo(
        text    = paste(text, title),
        video_lat = if (!is.null(vlat) && !is.na(vlat)) vlat else NA_real_,
        video_lng = if (!is.null(vlng) && !is.na(vlng)) vlng else NA_real_
      )

      all_comments[[length(all_comments)+1]] <- list(
        video_id      = vid,
        video_title   = title,
        channel       = new_worthy$channel[i] %||% "",
        query         = query_label,
        author        = s$authorDisplayName                     %||% "unknown",
        text          = text,
        likes         = as.integer(s$likeCount                  %||% 0),
        replies       = as.integer(item$snippet$totalReplyCount %||% 0),
        published_at  = s$publishedAt                           %||% "",
        text_hash     = digest(tolower(trimws(text)), algo="md5"),
        signal_score  = sig$score,
        priority      = sig$priority,
        ncic_category = category,
        triggers      = sig$triggers,
        latitude      = geo$lat,
        longitude     = geo$lng,
        geo_source    = geo$source,
        geo_confidence= geo$geo_confidence,
        source        = "youtube"
      )
      kept <- kept + 1L
    }

    pulled_log <- update_pulled_log(pulled_log, vid, kept)
    message(sprintf("  [%d/%d] '%s' -> %d signal comments",
                    i, nrow(new_worthy), substr(title, 1, 45), kept))
  }

  comments_df <- if (length(all_comments) == 0)
    data.frame()
  else {
    df <- do.call(rbind, lapply(all_comments, as.data.frame,
                                stringsAsFactors=FALSE))
    df[order(-df$signal_score, -df$replies), ]
  }

  list(comments=comments_df, log=pulled_log)
}

# ── FIX-D: MAIN PIPELINE FUNCTION ────────────────────────────────
run_youtube_pipeline <- function() {

message("\n", strrep("=", 60))
message("YOUTUBE INGESTION PIPELINE v2")
message(format(Sys.time(), "%d %b %Y %H:%M EAT"))
message(strrep("=", 60))

# ── FIX 2: Load approved keywords ────────────────────────────────
message("\n[keyword bank] Loading approved keywords from Supabase...")
kw_added <- merge_keyword_bank()
kw_total <- sum(kw_added)
if (kw_total > 0) {
  message(sprintf("  [keyword bank] ✅ +%d T1  +%d T2  +%d T3 — scorer now has %d T1 / %d T2 / %d T3 keywords",
                  kw_added[1], kw_added[2], kw_added[3],
                  length(TIER1), length(TIER2), length(TIER3)))
} else {
  message(sprintf("  [keyword bank] Using static lists (%d T1 / %d T2 / %d T3)",
                  length(TIER1), length(TIER2), length(TIER3)))
}

# ── FIX 1: Load dynamic search queries ───────────────────────────
message("\n[search queries] Loading active queries from Supabase...")
QUERIES <- build_query_list()

# Load all caches upfront — one read each
print_cache_status()
video_cache <- load_video_cache()
pulled_log  <- load_pulled_log()

message("\n[supabase] Loading existing hashes...")
existing_hashes <- load_hash_cache()
if (is.null(existing_hashes)) {
  message("[supabase] Fetching from Supabase (paginated)...")
  existing_hashes <- supa_get_existing_hashes("pipeline_youtube")
  save_hash_cache(existing_hashes)
  message(sprintf("[supabase] %d hashes fetched and cached", length(existing_hashes)))
} else {
  message("[supabase] Using cached hashes — no Supabase call")
}

message(sprintf("[quota] Starting with %d/%d units used",
                units_used, DAILY_QUOTA_LIMIT))

query_summary  <- data.frame()
total_inserted <- 0L
all_new        <- data.frame()

for (item in QUERIES) {
  q        <- item$q
  priority <- item$priority

  message(sprintf("\n[%s] %s", priority, q))

  # Score videos — uses/updates video_cache in memory
  scored <- tryCatch(
    score_videos(q, video_cache=video_cache),
    error = function(e) {
      if (grepl("Quota guard", e$message)) stop(e)  # re-raise quota errors
      message("  ERROR: ", e$message)
      data.frame()
    }
  )

  if (nrow(scored) == 0) {
    message("  No videos found")
    next
  }

  # Save updated video cache after each query
  video_cache <- save_video_cache(scored, q, video_cache)

  # Pull comments — pass pulled_log in, get updated log back
  result <- tryCatch(
    pull_comments(scored, pulled_log, query_label=q),
    error = function(e) {
      if (grepl("Quota guard", e$message)) stop(e)
      message("  ERROR: ", e$message)
      list(comments=data.frame(), log=pulled_log)
    }
  )

  pulled_log <- result$log     # update in memory
  comments   <- result$comments

  n_pulled <- nrow(comments)

  if (n_pulled == 0) {
    query_summary <- rbind(query_summary, data.frame(
      priority=priority, query=substr(q,1,45),
      videos=nrow(scored), pulled=0, new=0, inserted=0,
      stringsAsFactors=FALSE))
    next
  }

  # Deduplicate against Supabase hashes
  new_comments <- comments[!comments$text_hash %in% existing_hashes, ]
  n_new        <- nrow(new_comments)
  message(sprintf("  %d new / %d pulled (%d duplicates skipped)",
                  n_new, n_pulled, n_pulled - n_new))

  # Insert in batches of 50
  n_inserted <- 0L
  if (n_new > 0) {
    batch_size <- 50L
    for (b in seq_len(ceiling(n_new / batch_size))) {
      start <- (b-1)*batch_size + 1L
      end   <- min(b*batch_size, n_new)
      batch <- new_comments[start:end, ]

      batch$geo_confidence <- NULL  # drop column not in Supabase schema
      resp <- supa_insert("pipeline_youtube", batch)

      if (resp_status(resp) %in% c(200L, 201L, 204L)) {
        n_inserted      <- n_inserted + nrow(batch)
        existing_hashes <- c(existing_hashes, batch$text_hash)
        append_hash_cache(batch$text_hash)
      } else {
        # Save failed batch to disk for inspection
        fail_file <- file.path(FAILED_BATCH_DIR,
          sprintf("failed_%s_b%d.csv", format(Sys.time(),"%Y%m%d_%H%M%S"), b))
        write.csv(batch, fail_file, row.names=FALSE)
        message(sprintf("  ❌ Batch %d failed (status %d) — saved to %s",
                        b, resp_status(resp), fail_file))
      }
      Sys.sleep(0.2)
    }
    total_inserted <- total_inserted + n_inserted
    all_new        <- rbind(all_new, new_comments)
    message(sprintf("  ✅ Inserted %d comments", n_inserted))
  }

  query_summary <- rbind(query_summary, data.frame(
    priority = priority, query = substr(q, 1, 45),
    videos   = nrow(scored), pulled = n_pulled,
    new      = n_new, inserted = n_inserted,
    stringsAsFactors = FALSE
  ))

  Sys.sleep(1)
}

# Save pulled log once at the end (not per video)
save_pulled_log(pulled_log)
message(sprintf("\n[quota] Total units used this run: %d/%d",
                units_used, DAILY_QUOTA_LIMIT))

# ── SUMMARY ───────────────────────────────────────────────────────
message("\n", strrep("=", 60))
message("SUMMARY")
message(strrep("=", 60))

if (nrow(query_summary) > 0) {
  message(sprintf("%-8s %-46s %6s %6s %6s %8s",
                  "Priority","Query","Videos","Pulled","New","Inserted"))
  message(strrep("-", 80))
  for (i in seq_len(nrow(query_summary)))
    message(sprintf("%-8s %-46s %6d %6d %6d %8d",
                    query_summary$priority[i], query_summary$query[i],
                    query_summary$videos[i],   query_summary$pulled[i],
                    query_summary$new[i],      query_summary$inserted[i]))
  message(strrep("-", 80))
  message(sprintf("%-8s %-46s %6d %6d %6d %8d",
                  "TOTAL", "",
                  sum(query_summary$videos), sum(query_summary$pulled),
                  sum(query_summary$new),    total_inserted))
}

final_count <- supa_count("pipeline_youtube")
message(sprintf("\nTotal in Supabase:  %d", final_count))
message(sprintf("Inserted this run:  %d", total_inserted))

# Check for any failed batches
failed_files <- list.files(FAILED_BATCH_DIR, pattern="\\.csv$")
if (length(failed_files) > 0)
  message(sprintf("⚠ %d failed batch(es) in %s — inspect and retry",
                  length(failed_files), FAILED_BATCH_DIR))

# ── CACHE STATUS ──────────────────────────────────────────────────
message("\n[cache status]")
h_info <- if (file.exists(HASH_CACHE_FILE))
  readRDS(HASH_CACHE_FILE) else list(hashes=character(), saved_at="none")
message(sprintf("  Videos known:    %d", nrow(video_cache)))
message(sprintf("  Videos pulled:   %d", nrow(pulled_log)))
message(sprintf("  Comments in DB:  %d", final_count))
message(sprintf("  Hash cache:      %d hashes (saved: %s)",
                length(h_info$hashes), h_info$saved_at))
message(sprintf("  Quota used:      %d/%d units", units_used, DAILY_QUOTA_LIMIT))

# ── TOP SIGNAL PREVIEW ────────────────────────────────────────────
if (nrow(all_new) > 0) {
  message("\n", strrep("=", 60))
  message("TOP 5 HIGHEST SIGNAL COMMENTS THIS RUN")
  message(strrep("=", 60))
  top <- head(all_new[order(-all_new$signal_score, -all_new$replies), ], 5)
  for (i in seq_len(nrow(top)))
    message(sprintf(
      "\n[%d] score:%-3d %-6s | %s | %s\n    geo: %.4f, %.4f (%s)\n    triggers: %s\n    %s",
      i,
      top$signal_score[i],
      top$priority[i],
      top$ncic_category[i],
      top$author[i],
      top$latitude[i]  %||% 0,
      top$longitude[i] %||% 0,
      top$geo_source[i] %||% "",
      substr(top$triggers[i] %||% "", 1, 80),
      substr(top$text[i], 1, 150)
    ))

  # Signal breakdown
  message(sprintf("\n[signal breakdown]"))
  message(sprintf("  HIGH   (score>=3): %d", sum(all_new$signal_score >= 3)))
  message(sprintf("  MEDIUM (score=2):  %d", sum(all_new$signal_score == 2)))
  message(sprintf("  LOW    (score=1):  %d", sum(all_new$signal_score == 1)))

  # Geo source breakdown
  if ("geo_source" %in% names(all_new)) {
    geo_tbl <- table(all_new$geo_source)
    message(sprintf("\n[geo resolution]"))
    for (gs in names(geo_tbl))
      message(sprintf("  %-20s : %d comments", gs, geo_tbl[gs]))
  }
}

message("\n", strrep("=", 60))
message(sprintf("DONE — %d new comments stored", total_inserted))
message(sprintf("Next run will skip %d known videos", nrow(pulled_log)))
message(strrep("=", 60))

  invisible(list(inserted = total_inserted, quota_used = units_used))
} # end run_youtube_pipeline()

# ── AUTO-RUN WHEN SOURCED STANDALONE ─────────────────────────────
if (!exists(".pipeline_sourced_by_app")) {
  run_youtube_pipeline()
}
