# ================================================================
#  google_test.R — YouTube → Supabase Ingestion Pipeline
#  Radicalisation Signals · IEA Kenya NIRU AI Hackathon
#
#  CACHING STRATEGY (saves YouTube API quota):
#    - cache/yt_video_cache.rds   — scored videos already found
#    - cache/yt_pulled_videos.rds — video IDs already pulled comments from
#    - Supabase text_hash column  — comments already stored
#
#  On each run:
#    1. Skip search API for queries run today (use cached video IDs)
#    2. Skip pull for videos already pulled (use pulled video log)
#    3. Skip insert for comments already in Supabase (use text_hash)
#
#  Usage: source("google_test.R")
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
MAX_VIDEOS_PER_QUERY  <- 8     # videos to score per query
MIN_COMMENTS_TO_PULL  <- 50    # only pull videos with this many+ comments
COMMENTS_PER_VIDEO    <- 50    # max comments per video
API_PAUSE_SECS        <- 0.5   # pause between API calls
CACHE_DIR             <- "cache"
VIDEO_CACHE_FILE      <- file.path(CACHE_DIR, "yt_video_cache.rds")
PULLED_LOG_FILE       <- file.path(CACHE_DIR, "yt_pulled_videos.rds")

# ── QUERIES ───────────────────────────────────────────────────────
QUERIES <- list(
  # HIGH signal — ethnic tensions, expulsion rhetoric
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

# ── LOCAL CACHE HELPERS ───────────────────────────────────────────

# Ensure cache directory exists
if (!dir.exists(CACHE_DIR)) dir.create(CACHE_DIR, recursive=TRUE)

# Load video cache (previously found videos with their stats)
load_video_cache <- function() {
  if (file.exists(VIDEO_CACHE_FILE))
    readRDS(VIDEO_CACHE_FILE)
  else
    data.frame(video_id=character(), title=character(), channel=character(),
               comment_count=integer(), view_count=integer(),
               like_count=integer(), signal_score=numeric(),
               query=character(), cached_at=character(),
               stringsAsFactors=FALSE)
}

# Save new videos into the cache
save_video_cache <- function(new_videos, query_label) {
  existing <- load_video_cache()
  new_videos$query     <- query_label
  new_videos$cached_at <- format(Sys.Date(), "%Y-%m-%d")
  combined <- rbind(existing, new_videos)
  combined <- combined[!duplicated(combined$video_id), ]
  saveRDS(combined, VIDEO_CACHE_FILE)
  combined
}

# Load list of video IDs we already pulled comments from
load_pulled_log <- function() {
  if (file.exists(PULLED_LOG_FILE))
    readRDS(PULLED_LOG_FILE)
  else
    data.frame(video_id=character(), pulled_at=character(),
               n_comments=integer(), stringsAsFactors=FALSE)
}

# Record that we pulled comments from a video
log_pulled_video <- function(video_id, n_comments) {
  log <- load_pulled_log()
  # Update if exists, add if new
  if (video_id %in% log$video_id) {
    log$pulled_at[log$video_id == video_id]  <- format(Sys.time(), "%Y-%m-%d %H:%M")
    log$n_comments[log$video_id == video_id] <- n_comments
  } else {
    log <- rbind(log, data.frame(
      video_id   = video_id,
      pulled_at  = format(Sys.time(), "%Y-%m-%d %H:%M"),
      n_comments = n_comments,
      stringsAsFactors = FALSE
    ))
  }
  saveRDS(log, PULLED_LOG_FILE)
}

# Print cache status
print_cache_status <- function() {
  v_cache  <- load_video_cache()
  p_log    <- load_pulled_log()
  message(sprintf("[cache] %d videos known | %d already pulled",
                  nrow(v_cache), nrow(p_log)))
  if (nrow(p_log) > 0) {
    message("[cache] Last 5 pulled videos:")
    recent <- tail(p_log[order(p_log$pulled_at), ], 5)
    for (i in seq_len(nrow(recent)))
      message(sprintf("        %s | %d comments | %s",
                      recent$video_id[i], recent$n_comments[i], recent$pulled_at[i]))
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

supa_get_existing_hashes <- function(table) {
  resp <- request(paste0(supa_url, "/rest/v1/", table)) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key)
    ) |>
    req_url_query(select="text_hash", limit=50000) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform() |>
    resp_body_json(simplifyVector=TRUE)
  if (is.data.frame(resp) && "text_hash" %in% names(resp))
    resp$text_hash
  else
    character(0)
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

# ── YOUTUBE HELPERS ───────────────────────────────────────────────

KENYA_CHANNELS <- c(
  "ktn","ntv kenya","citizen tv","k24","capital fm","the star kenya",
  "nairobi","kenya","kisumu","mombasa","nakuru","kbc","standard media",
  "nation","daily nation","tuko","pulselive kenya","kenya news",
  "plug tv","kenya newsline","maisha tv","kenya citizen tv",
  "inooro","kameme","muuga","meru fm","egesa fm","west fm",
  "royal media","ramogi","safari news"
)

is_kenya_channel <- function(channel_name) {
  ch <- tolower(channel_name)
  any(sapply(KENYA_CHANNELS, function(k) grepl(k, ch, fixed=TRUE)))
}

# Search YouTube and score videos.
# If videos for this query are already cached today, skip the API call.
score_videos <- function(query, max_videos=MAX_VIDEOS_PER_QUERY) {
  
  # Check cache first — if we searched this query today, reuse results
  v_cache <- load_video_cache()
  today   <- format(Sys.Date(), "%Y-%m-%d")
  
  cached_for_query <- v_cache[v_cache$query == query &
                                v_cache$cached_at == today, ]
  
  if (nrow(cached_for_query) > 0) {
    message(sprintf("  [cache HIT] %d videos from cache (no API call)",
                    nrow(cached_for_query)))
    return(cached_for_query)
  }
  
  # Not cached — call YouTube API (costs 100 + N units)
  message("  [cache MISS] Calling YouTube search API...")
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
  
  if (is.null(search_resp) || is.null(search_resp$items)) return(data.frame())
  
  items     <- Filter(function(v) !is.null(v$id$videoId), search_resp$items)
  video_ids <- sapply(items, function(v) v$id$videoId)
  if (length(video_ids) == 0) return(data.frame())
  
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
  
  if (is.null(stats_resp) || is.null(stats_resp$items)) return(data.frame())
  
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
  
  # Kenya filter
  kenya_markers <- c("kenya","nairobi","ruto","raila","uhuru","kenyans",
                     "uchaguzi","kabila","serikali","wakenya","odinga")
  videos$is_kenya <- sapply(seq_len(nrow(videos)), function(i) {
    is_kenya_channel(videos$channel[i]) ||
      any(sapply(kenya_markers, function(m)
        grepl(m, tolower(videos$title[i]), fixed=TRUE)))
  })
  kenya_vids <- videos[videos$is_kenya, ]
  if (nrow(kenya_vids) == 0) kenya_vids <- videos
  
  kenya_vids$signal_score <- kenya_vids$comment_count * 0.7 +
    kenya_vids$view_count    * 0.0001
  kenya_vids <- kenya_vids[order(-kenya_vids$signal_score), ]
  
  # Save to cache for reuse today
  save_video_cache(kenya_vids, query)
  kenya_vids
}

# Pull comments — skips videos already pulled
pull_comments <- function(scored_videos,
                          min_comments       = MIN_COMMENTS_TO_PULL,
                          comments_per_video = COMMENTS_PER_VIDEO,
                          query_label        = "") {
  
  worthy <- scored_videos[scored_videos$comment_count >= min_comments, ]
  if (nrow(worthy) == 0) return(data.frame())
  
  # Skip videos already pulled
  pulled_log    <- load_pulled_log()
  already_pulled <- pulled_log$video_id
  new_worthy    <- worthy[!worthy$video_id %in% already_pulled, ]
  
  n_skipped <- nrow(worthy) - nrow(new_worthy)
  if (n_skipped > 0)
    message(sprintf("  [cache] Skipping %d already-pulled video(s)", n_skipped))
  
  if (nrow(new_worthy) == 0) {
    message("  [cache] All videos already pulled — nothing new")
    return(data.frame())
  }
  
  message(sprintf("  Pulling from %d new video(s)", nrow(new_worthy)))
  
  all_comments <- list()
  
  for (i in seq_len(nrow(new_worthy))) {
    vid   <- new_worthy$video_id[i]
    title <- new_worthy$title[i]
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
      message(sprintf("  Comments disabled: %s", substr(title, 1, 40)))
      log_pulled_video(vid, 0)   # log even if disabled — don't retry
      next
    }
    
    kept <- 0
    for (item in comm_resp$items) {
      s    <- item$snippet$topLevelComment$snippet
      text <- trimws(s$textOriginal %||% "")
      
      if (nchar(text) < 15)         next
      if (!grepl("[a-zA-Z]", text)) next
      
      all_comments[[length(all_comments)+1]] <- list(
        video_id     = vid,
        video_title  = title,
        channel      = new_worthy$channel[i] %||% "",
        query        = query_label,
        author       = s$authorDisplayName                  %||% "unknown",
        text         = text,
        likes        = as.integer(s$likeCount               %||% 0),
        replies      = as.integer(item$snippet$totalReplyCount %||% 0),
        published_at = s$publishedAt                        %||% "",
        text_hash    = digest(tolower(trimws(text)), algo="md5")
      )
      kept <- kept + 1
    }
    
    # Log this video as pulled so we never pull it again
    log_pulled_video(vid, kept)
    message(sprintf("  [%d/%d] '%s' -> %d comments",
                    i, nrow(new_worthy), substr(title, 1, 45), kept))
  }
  
  if (length(all_comments) == 0) return(data.frame())
  
  df <- do.call(rbind, lapply(all_comments, as.data.frame, stringsAsFactors=FALSE))
  df[order(-df$replies, -df$likes), ]
}

# ── MAIN PIPELINE ─────────────────────────────────────────────────
message("\n", strrep("=", 60))
message("YOUTUBE -> SUPABASE INGESTION PIPELINE")
message(format(Sys.time(), "%d %b %Y %H:%M"))
message(strrep("=", 60))

# Show cache status before starting
print_cache_status()

# Load Supabase hashes once upfront
message("\n[supabase] Fetching existing hashes...")
existing_hashes <- supa_get_existing_hashes("rad_signals_google_api")
message(sprintf("[supabase] %d comments already stored", length(existing_hashes)))

query_summary  <- data.frame()
total_inserted <- 0L
all_new        <- data.frame()

for (item in QUERIES) {
  q        <- item$q
  priority <- item$priority
  
  message(sprintf("\n[%s] %s", priority, q))
  
  scored <- tryCatch(score_videos(q),
                     error=function(e) { message("  ERROR: ", e$message); data.frame() })
  if (nrow(scored) == 0) { message("  No videos found"); next }
  
  comments <- tryCatch(pull_comments(scored, query_label=q),
                       error=function(e) { message("  ERROR: ", e$message); data.frame() })
  if (nrow(comments) == 0) {
    query_summary <- rbind(query_summary, data.frame(
      priority=priority, query=substr(q,1,45),
      videos=nrow(scored), pulled=0, new=0, inserted=0,
      stringsAsFactors=FALSE))
    next
  }
  
  # Deduplicate against Supabase
  new_comments <- comments[!comments$text_hash %in% existing_hashes, ]
  n_new        <- nrow(new_comments)
  message(sprintf("  %d new / %d total (%d duplicates skipped)",
                  n_new, nrow(comments), nrow(comments)-n_new))
  
  # Insert in batches of 50
  n_inserted <- 0L
  if (n_new > 0) {
    batch_size <- 50L
    for (b in seq_len(ceiling(n_new / batch_size))) {
      start <- (b-1)*batch_size + 1
      end   <- min(b*batch_size, n_new)
      batch <- new_comments[start:end, ]
      
      resp <- supa_insert("rad_signals_google_api", batch)
      if (resp_status(resp) %in% c(200L, 201L, 204L)) {
        n_inserted      <- n_inserted + nrow(batch)
        existing_hashes <- c(existing_hashes, batch$text_hash)
      } else {
        message(sprintf("  Batch %d failed (status %d)", b, resp_status(resp)))
      }
      Sys.sleep(0.2)
    }
    total_inserted <- total_inserted + n_inserted
    all_new        <- rbind(all_new, new_comments)
    message(sprintf("  Inserted %d comments", n_inserted))
  }
  
  query_summary <- rbind(query_summary, data.frame(
    priority = priority, query = substr(q, 1, 45),
    videos   = nrow(scored), pulled = nrow(comments),
    new      = n_new, inserted = n_inserted,
    stringsAsFactors = FALSE
  ))
  
  Sys.sleep(1)
}

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
                  "TOTAL","",
                  sum(query_summary$videos), sum(query_summary$pulled),
                  sum(query_summary$new),    total_inserted))
}

final_count <- supa_count("rad_signals_google_api")
message(sprintf("\nTotal in Supabase:  %d", final_count))
message(sprintf("Inserted this run:  %d", total_inserted))

# ── CACHE STATUS AFTER RUN ────────────────────────────────────────
message("\n[cache status after run]")
v_cache  <- load_video_cache()
p_log    <- load_pulled_log()
message(sprintf("  Videos known:    %d", nrow(v_cache)))
message(sprintf("  Videos pulled:   %d", nrow(p_log)))
message(sprintf("  Comments in DB:  %d", final_count))

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
message(sprintf("Next run will skip %d known videos", nrow(p_log)))
message(strrep("=", 60))