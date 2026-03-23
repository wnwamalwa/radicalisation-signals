# ================================================================
#  telegram_pipeline.R — Telegram → Supabase Ingestion Pipeline
#  Radicalisation Signals · IEA Kenya NIRU AI Hackathon
#
#  WORKFLOW:
#    1. Calls telegram_scraper.py (MTProto) to dynamically discover
#       public Telegram channels using NCIC signal keywords
#    2. Python scores messages and saves to cache/tg_raw_messages.csv
#    3. R deduplicates against Supabase and inserts new signals
#
#  CACHING STRATEGY:
#    cache/tg_session          — Telegram login session (login once)
#    cache/tg_raw_messages.csv — raw scored messages from Python
#    cache/tg_channel_cache.rds— discovered channels + signal scores
#    cache/tg_hash_cache.rds   — Supabase text hashes (TTL: 60 min)
#    cache/tg_failed_batches/  — failed Supabase inserts
#
#  Usage: source("telegram_pipeline.R")
# ================================================================

readRenviron("secrets/.Renviron")
library(httr2)
library(jsonlite)
library(digest)
library(readr)

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

# ── CREDENTIALS ───────────────────────────────────────────────────
supa_url <- Sys.getenv("SUPABASE_URL")
supa_key <- Sys.getenv("SUPABASE_KEY")

if (nchar(supa_url) == 0) stop("SUPABASE_URL not found in secrets/.Renviron")
if (nchar(supa_key) == 0) stop("SUPABASE_KEY not found in secrets/.Renviron")
message("✅ Credentials loaded")

# ── CONFIGURATION ─────────────────────────────────────────────────
MIN_SIGNAL_SCORE    <- 1L    # minimum score to store in Supabase
HASH_CACHE_TTL_MINS <- 60L   # refresh Supabase hash cache after N minutes
SCRAPER_SCRIPT      <- "pipeline_telegram.py"
RAW_CSV             <- file.path("cache", "tg_raw_messages.csv")

CACHE_DIR          <- "cache"
CHANNEL_CACHE_FILE <- file.path(CACHE_DIR, "tg_channel_cache.rds")
HASH_CACHE_FILE    <- file.path(CACHE_DIR, "tg_hash_cache.rds")
FAILED_BATCH_DIR   <- file.path(CACHE_DIR, "tg_failed_batches")

if (!dir.exists(CACHE_DIR))        dir.create(CACHE_DIR, recursive = TRUE)
if (!dir.exists(FAILED_BATCH_DIR)) dir.create(FAILED_BATCH_DIR, recursive = TRUE)

# ── CHANNEL CACHE ─────────────────────────────────────────────────
load_channel_cache <- function() {
  if (file.exists(CHANNEL_CACHE_FILE)) readRDS(CHANNEL_CACHE_FILE)
  else data.frame(
    chat_id = character(), chat_title = character(),
    signal_score = numeric(), msg_count = integer(),
    first_seen = character(), last_seen = character(),
    stringsAsFactors = FALSE)
}

save_channel_cache <- function(cache) saveRDS(cache, CHANNEL_CACHE_FILE)

update_channel_score <- function(cache, chat_id, chat_title, msg_score) {
  chat_id <- as.character(chat_id)
  if (chat_id %in% cache$chat_id) {
    idx <- which(cache$chat_id == chat_id)
    cache$signal_score[idx] <- round(cache$signal_score[idx] * 0.8 + msg_score, 2)
    cache$last_seen[idx]    <- format(Sys.time(), "%Y-%m-%d %H:%M")
    cache$msg_count[idx]    <- cache$msg_count[idx] + 1L
  } else {
    cache <- rbind(cache, data.frame(
      chat_id      = chat_id,
      chat_title   = chat_title,
      signal_score = as.numeric(msg_score),
      msg_count    = 1L,
      first_seen   = format(Sys.time(), "%Y-%m-%d %H:%M"),
      last_seen    = format(Sys.time(), "%Y-%m-%d %H:%M"),
      stringsAsFactors = FALSE))
  }
  cache
}

# ── HASH CACHE ────────────────────────────────────────────────────
load_hash_cache <- function() {
  if (!file.exists(HASH_CACHE_FILE)) return(NULL)
  cache <- readRDS(HASH_CACHE_FILE)
  age   <- as.numeric(difftime(Sys.time(), as.POSIXct(cache$saved_at), units = "mins"))
  if (age > HASH_CACHE_TTL_MINS) {
    message(sprintf("[hash cache] Expired (%.0f min old) — refreshing", age))
    return(NULL)
  }
  message(sprintf("[hash cache] HIT — %d hashes (%.0f min old)", length(cache$hashes), age))
  cache$hashes
}

save_hash_cache <- function(hashes) {
  saveRDS(list(hashes = hashes, saved_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
          HASH_CACHE_FILE)
}

append_hash_cache <- function(new_hashes) {
  if (!file.exists(HASH_CACHE_FILE)) return(invisible(NULL))
  cache    <- readRDS(HASH_CACHE_FILE)
  combined <- unique(c(cache$hashes, new_hashes))
  saveRDS(list(hashes = combined, saved_at = cache$saved_at), HASH_CACHE_FILE)
}

# ── SUPABASE HELPERS ──────────────────────────────────────────────
supa_insert <- function(table, rows_df) {
  req <- request(paste0(supa_url, "/rest/v1/", table)) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key),
      "Content-Type"  = "application/json",
      "Prefer"        = "return=minimal"
    ) |>
    req_body_raw(toJSON(rows_df, auto_unbox = TRUE, na = "null"), type = "application/json") |>
    req_method("POST")
  req_perform(req)
}

supa_get_existing_hashes <- function(table) {
  all_hashes <- character()
  offset     <- 0L
  page_size  <- 1000L
  repeat {
    req <- request(paste0(supa_url, "/rest/v1/", table)) |>
      req_headers(
        "apikey"        = supa_key,
        "Authorization" = paste("Bearer", supa_key),
        "Range"         = sprintf("%d-%d", offset, offset + page_size - 1L)
      ) |>
      req_url_query(select = "text_hash")
    resp <- req_perform(req)
    page <- fromJSON(resp_body_string(resp))
    if (length(page) == 0 || nrow(page) == 0) break
    all_hashes <- c(all_hashes, page$text_hash)
    if (nrow(page) < page_size) break
    offset <- offset + page_size
  }
  unique(all_hashes)
}

supa_count <- function(table) {
  req <- request(paste0(supa_url, "/rest/v1/", table)) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key),
      "Prefer"        = "count=exact",
      "Range"         = "0-0"
    ) |>
    req_url_query(select = "id")
  resp  <- req_perform(req)
  cr    <- resp_header(resp, "content-range") %||% "0/0"
  total <- suppressWarnings(as.integer(sub(".*/", "", cr)))
  total %||% 0L
}

# ── MAIN PIPELINE ─────────────────────────────────────────────────
message("\n", strrep("=", 60))
message("TELEGRAM RADICALISATION SIGNAL PIPELINE")
message(format(Sys.time(), "%d %b %Y %H:%M EAT"))
message(strrep("=", 60))

# ── STEP 1: RUN PYTHON SCRAPER ────────────────────────────────────
message("\n[scraper] Launching telegram_scraper.py...")
message("[scraper] Searching Telegram dynamically via NCIC keywords...")
message("[scraper] This may take 2-5 minutes on first run (login required)...")

scraper_status <- system(paste("python3", SCRAPER_SCRIPT), intern = FALSE)

if (scraper_status != 0) {
  stop("❌ telegram_scraper.py failed — check output above for errors")
}

message("[scraper] ✅ Python scraper completed")

# ── STEP 2: LOAD RAW MESSAGES ─────────────────────────────────────
if (!file.exists(RAW_CSV)) {
  stop("❌ cache/tg_raw_messages.csv not found — scraper may have failed")
}

raw <- read_csv(RAW_CSV, show_col_types = FALSE)
message(sprintf("[load] %d raw messages loaded from CSV", nrow(raw)))

if (nrow(raw) == 0) {
  message("⚠ No messages in CSV — nothing to insert")
  message(strrep("=", 60))
  message("DONE")
  message(strrep("=", 60))
  quit(save = "no")
}

# ── STEP 3: FILTER BY MINIMUM SIGNAL SCORE ────────────────────────
raw <- raw[!is.na(raw$signal_score) & raw$signal_score >= MIN_SIGNAL_SCORE, ]
message(sprintf("[filter] %d messages at or above signal threshold (%d)",
                nrow(raw), MIN_SIGNAL_SCORE))

# ── STEP 4: ADD TEXT HASH ─────────────────────────────────────────
raw$text_hash <- sapply(tolower(trimws(raw$text)), digest, algo = "md5")

# ── STEP 5: LOAD SUPABASE HASHES ─────────────────────────────────
message("\n[supabase] Loading existing hashes...")
existing_hashes <- load_hash_cache()
if (is.null(existing_hashes)) {
  message("[supabase] Fetching from Supabase (paginated)...")
  existing_hashes <- supa_get_existing_hashes("pipeline_telegram")
  save_hash_cache(existing_hashes)
  message(sprintf("[supabase] %d hashes fetched and cached", length(existing_hashes)))
} else {
  message("[supabase] Using cached hashes — no Supabase call")
}

# ── STEP 6: DEDUPLICATE ───────────────────────────────────────────
new_messages <- raw[!raw$text_hash %in% existing_hashes, ]
n_dupes      <- nrow(raw) - nrow(new_messages)
message(sprintf("[dedup] %d new | %d duplicates skipped", nrow(new_messages), n_dupes))

# ── STEP 7: UPDATE CHANNEL CACHE ─────────────────────────────────
channel_cache <- load_channel_cache()
for (i in seq_len(nrow(raw))) {
  channel_cache <- update_channel_score(
    channel_cache,
    raw$chat_id[i],
    raw$chat_title[i],
    raw$signal_score[i]
  )
}
save_channel_cache(channel_cache)

# ── STEP 8: INSERT TO SUPABASE ────────────────────────────────────
total_inserted <- 0L

if (nrow(new_messages) > 0) {

  # Sort by signal score descending
  new_messages <- new_messages[order(-new_messages$signal_score), ]

  # Ensure all required columns exist
  new_messages$source <- "telegram"

  batch_size <- 50L
  n_new      <- nrow(new_messages)

  message(sprintf("\n[supabase] Inserting %d new messages in batches of %d...", n_new, batch_size))

  for (b in seq_len(ceiling(n_new / batch_size))) {
    start <- (b - 1) * batch_size + 1L
    end   <- min(b * batch_size, n_new)
    batch <- new_messages[start:end, ]
    resp  <- supa_insert("pipeline_telegram", batch)

    if (resp_status(resp) %in% c(200L, 201L, 204L)) {
      total_inserted  <- total_inserted + nrow(batch)
      existing_hashes <- c(existing_hashes, batch$text_hash)
      append_hash_cache(batch$text_hash)
      message(sprintf("  ✅ Batch %d inserted (%d rows)", b, nrow(batch)))
    } else {
      fail_file <- file.path(FAILED_BATCH_DIR,
        sprintf("failed_%s_b%d.csv", format(Sys.time(), "%Y%m%d_%H%M%S"), b))
      write.csv(batch, fail_file, row.names = FALSE)
      message(sprintf("  ❌ Batch %d failed (status %d) — saved to %s",
                      b, resp_status(resp), fail_file))
    }
    Sys.sleep(0.2)
  }
}

# ── SUMMARY ───────────────────────────────────────────────────────
message("\n", strrep("=", 60))
message("SUMMARY")
message(strrep("=", 60))

final_count <- supa_count("pipeline_telegram")
message(sprintf("Total in Supabase:  %d", final_count))
message(sprintf("Inserted this run:  %d", total_inserted))
message(sprintf("Raw messages found: %d", nrow(raw)))

# Priority breakdown
if (nrow(new_messages) > 0) {
  priority_summary <- as.data.frame(table(new_messages$priority))
  names(priority_summary) <- c("priority", "count")
  message("\nSignal breakdown (inserted):")
  for (i in seq_len(nrow(priority_summary)))
    message(sprintf("  %-8s : %d messages",
                    priority_summary$priority[i], priority_summary$count[i]))
}

# ── TOP SIGNAL CHANNELS ───────────────────────────────────────────
if (nrow(channel_cache) > 0) {
  message("\n", strrep("=", 60))
  message("TOP SIGNAL CHANNELS (cumulative)")
  message(strrep("=", 60))
  top_ch <- head(channel_cache[order(-channel_cache$signal_score), ], 10)
  message(sprintf("%-35s %8s %8s  %s", "Channel", "Score", "Msgs", "Last seen"))
  message(strrep("-", 70))
  for (i in seq_len(nrow(top_ch)))
    message(sprintf("%-35s %8.1f %8d  %s",
                    substr(top_ch$chat_title[i], 1, 35),
                    top_ch$signal_score[i],
                    top_ch$msg_count[i],
                    top_ch$last_seen[i]))
  message("\n💡 High-score channels are automatically prioritised next run")
}

# ── TOP SIGNAL MESSAGES ───────────────────────────────────────────
if (nrow(new_messages) > 0 && total_inserted > 0) {
  message("\n", strrep("=", 60))
  message("TOP 5 HIGHEST SIGNAL MESSAGES THIS RUN")
  message(strrep("=", 60))
  top_msgs <- head(new_messages[order(-new_messages$signal_score), ], 5)
  for (i in seq_len(nrow(top_msgs)))
    message(sprintf(
      "\n[%d] score:%-3d %-6s | %s → %s | chat: %s\n    triggers: %s\n    %s",
      i,
      top_msgs$signal_score[i],
      top_msgs$priority[i],
      top_msgs$sender_name[i] %||% "unknown",
      top_msgs$reply_to_name[i] %||% "N/A",
      top_msgs$chat_title[i],
      substr(top_msgs$triggers[i] %||% "", 1, 80),
      substr(top_msgs$text[i], 1, 150)
    ))
}

# Failed batches check
failed_files <- list.files(FAILED_BATCH_DIR, pattern = "\\.csv$")
if (length(failed_files) > 0)
  message(sprintf("\n⚠ %d failed batch(es) in %s — inspect and retry",
                  length(failed_files), FAILED_BATCH_DIR))

message("\n", strrep("=", 60))
message(sprintf("DONE — %d radicalisation signals stored", total_inserted))
message(strrep("=", 60))
