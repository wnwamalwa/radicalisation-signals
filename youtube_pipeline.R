# ================================================================
#  youtube_pipeline.R — YouTube → Supabase Ingestion Pipeline
#  Radicalisation Signals · IEA Kenya NIRU AI Hackathon
#
#  CACHING STRATEGY (saves YouTube API quota):
#    cache/yt_video_cache.rds   — scored videos found per query (daily)
#    cache/yt_pulled_videos.rds — video IDs already pulled comments from
#    cache/yt_hash_cache.rds    — Supabase text hashes (TTL: 60 min)
#    cache/yt_failed_batches/   — failed Supabase inserts for inspection
#
#  QUOTA GUARD:
#    Daily limit: 10,000 units. Script stops at 8,000 (2,000 buffer).
#    Search = 100 units | Stats = 1/video | Comments = 1/video
#
#  Usage: source("youtube_pipeline.R")
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
message("Credentials loaded")

# ── CONFIGURATION ─────────────────────────────────────────────────
MAX_VIDEOS_PER_QUERY  <- 8L    # videos to score per query
MIN_COMMENTS_TO_PULL  <- 50L   # only pull videos with this many+ comments
COMMENTS_PER_VIDEO    <- 50L   # max comments per video
API_PAUSE_SECS        <- 0.5   # pause between API calls (rate limit safety)
DAILY_QUOTA_LIMIT     <- 8000L # stop before hitting 10,000 unit hard limit
HASH_CACHE_TTL_MINS   <- 60L   # refresh Supabase hash cache after N minutes

CACHE_DIR         <- "cache"
VIDEO_CACHE_FILE  <- file.path(CACHE_DIR, "yt_video_cache.rds")
PULLED_LOG_FILE   <- file.path(CACHE_DIR, "yt_pulled_videos.rds")
HASH_CACHE_FILE   <- file.path(CACHE_DIR, "yt_hash_cache.rds")
FAILED_BATCH_DIR  <- file.path(CACHE_DIR, "yt_failed_batches")

# ── QUERIES — ordered HIGH to LOW expected signal yield ───────────
QUERIES <- list(
  # HIGH — ethnic tensions, expulsion rhetoric
  list(q="waende kwao Kenya kabila",           priority="HIGH"),
  list(q="tribal hate Kenya incitement 2027",  priority="HIGH"),
  list(q="ethnic violence Kenya community",    priority="HIGH"),
  list(q="kabila Kenya siasa conflict",        priority="HIGH"),
  # MEDIUM — election fraud, regional tensions
  list(q="uchaguzi Kenya rigging tribe 2027",  priority="MEDIUM"),
  list(q="coast Kenya pwani independence",     priority="MEDIUM"),
  list(q="Mt Kenya Rift Valley conflict land", priority="MEDIUM"),
  list(q="northern Kenya pastoralists attack", priority="MEDIUM"),
  # LOW — general political critique
  list(q="Ruto resign Kenya 2027",             priority="LOW"),
  list(q="Kenya parliament corruption tribe",  priority="LOW")
)

# ── KNOWN KENYAN CHANNELS ─────────────────────────────────────────
# Used to strictly filter out non-Kenya content.
# A video must match a channel OR a title keyword to be accepted.
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
  "azimio","matiang","gachagua","kalonzo","mudavadi","wetangula",
  "kisumu","mombasa","nakuru","eldoret","kiambu","machakos"
)

# Strict Kenya check — channel OR title must match, not just either
is_kenya_content <- function(channel, title) {
  ch  <- tolower(channel)
  ttl <- tolower(title)
  channel_match <- any(sapply(KENYA_CHANNELS, function(k)
    grepl(k, ch, fixed=TRUE)))
  title_match   <- any(sapply(KENYA_TITLE_MARKERS, function(m)
    grepl(m, ttl, fixed=TRUE)))
  channel_match || title_match
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
  if (file.exists(VIDEO_CACHE_FILE)) readRDS(VIDEO_CACHE_FILE)
  else data.frame(
    video_id=character(), title=character(), channel=character(),
    comment_count=integer(), view_count=integer(), like_count=integer(),
    signal_score=numeric(), query=character(), cached_at=character(),
    stringsAsFactors=FALSE)
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

  # Fetch stats (1 unit per video)
  charge_quota(length(video_ids), "stats")
  Sys.sleep(API_PAUSE_SECS)

  stats_resp <- tryCatch(
    request("https://www.googleapis.com/youtube/v3/videos") |>
      req_url_query(
        key  = yt_key,
        id   = paste(video_ids, collapse=","),
        part = "statistics,snippet"
      ) |>
      req_error(is_error = \(r) FALSE) |>
      req_perform() |>
      resp_body_json(),
    error = function(e) NULL
  )

  if (is.null(stats_resp) || is.null(stats_resp$items))
    return(data.frame())

  videos <- do.call(rbind, lapply(stats_resp$items, function(v) {
    data.frame(
      video_id      = v$id %||% "",
      title         = substr(v$snippet$title %||% "", 1, 70),
      channel       = v$snippet$channelTitle %||% "",
      comment_count = as.integer(v$statistics$commentCount %||% 0),
      view_count    = as.integer(v$statistics$viewCount    %||% 0),
      like_count    = as.integer(v$statistics$likeCount    %||% 0),
      stringsAsFactors = FALSE
    )
  }))

  # Strict Kenya filter — reject non-Kenya videos
  videos$is_kenya <- mapply(is_kenya_content,
                             videos$channel, videos$title)
  kenya_vids <- videos[videos$is_kenya, ]

  # If filter removes everything, keep original (better than zero results)
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
# Updates pulled_log in memory — caller must save_pulled_log() when done.
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
    vid   <- new_worthy$video_id[i]
    title <- new_worthy$title[i]

    # Charge quota before the call
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

      if (nchar(text) < 15)         next  # too short
      if (!grepl("[a-zA-Z]", text)) next  # no Latin characters

      all_comments[[length(all_comments)+1]] <- list(
        video_id     = vid,
        video_title  = title,
        channel      = new_worthy$channel[i] %||% "",
        query        = query_label,
        author       = s$authorDisplayName                    %||% "unknown",
        text         = text,
        likes        = as.integer(s$likeCount                 %||% 0),
        replies      = as.integer(item$snippet$totalReplyCount %||% 0),
        published_at = s$publishedAt                          %||% "",
        text_hash    = digest(tolower(trimws(text)), algo="md5")
      )
      kept <- kept + 1L
    }

    pulled_log <- update_pulled_log(pulled_log, vid, kept)
    message(sprintf("  [%d/%d] '%s' -> %d comments",
                    i, nrow(new_worthy), substr(title, 1, 45), kept))
  }

  comments_df <- if (length(all_comments) == 0)
    data.frame()
  else {
    df <- do.call(rbind, lapply(all_comments, as.data.frame,
                                stringsAsFactors=FALSE))
    df[order(-df$replies, -df$likes), ]
  }

  list(comments=comments_df, log=pulled_log)
}

# ── MAIN PIPELINE ─────────────────────────────────────────────────
message("\n", strrep("=", 60))
message("YOUTUBE INGESTION PIPELINE")
message(format(Sys.time(), "%d %b %Y %H:%M EAT"))
message(strrep("=", 60))

# Load all caches upfront — one read each
print_cache_status()
video_cache <- load_video_cache()
pulled_log  <- load_pulled_log()

message("\n[supabase] Loading existing hashes...")
existing_hashes <- load_hash_cache()
if (is.null(existing_hashes)) {
  message("[supabase] Fetching from Supabase (paginated)...")
  existing_hashes <- supa_get_existing_hashes("rad_signals_google_api")
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

      resp <- supa_insert("rad_signals_google_api", batch)

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

final_count <- supa_count("rad_signals_google_api")
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
  message("TOP 5 MOST REPLIED NEW COMMENTS")
  message(strrep("=", 60))
  top <- head(all_new[order(-all_new$replies, -all_new$likes), ], 5)
  for (i in seq_len(nrow(top)))
    message(sprintf("\n[%d] replies:%-3d likes:%-4d | %s\n    %s",
                    i, top$replies[i], top$likes[i], top$author[i],
                    substr(top$text[i], 1, 150)))
}

message("\n", strrep("=", 60))
message(sprintf("DONE — %d new comments stored", total_inserted))
message(sprintf("Next run will skip %d known videos", nrow(pulled_log)))
message(strrep("=", 60))
