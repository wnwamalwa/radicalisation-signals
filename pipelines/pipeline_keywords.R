# ================================================================
#  pipeline_keywords.R — Adaptive Keyword + Query Extraction Pipeline
#  Radicalisation Signals · IEA Kenya NIRU AI Hackathon
#
#  WORKFLOW — PART A: KEYWORD EXTRACTION
#    1. Pull recent messages from Supabase (pipeline_telegram)
#    2. Send batches to GPT for new signal term extraction
#    3. GPT suggests new keywords with tier + category
#    4. New keywords saved to keyword_bank as 'pending'
#    5. Officers review in app.R dashboard
#    6. Approved keywords go live immediately next pipeline run
#
#  WORKFLOW — PART B: SEARCH QUERY ANALYSIS
#    1. Pull current search_queries + their channel performance
#    2. Pull top signal messages from pipeline_telegram
#    3. GPT analyses what queries are working and suggests:
#         - NEW queries to discover more signal channels
#         - PROMOTE existing queries (LOW → MEDIUM → HIGH)
#         - DEACTIVATE queries finding zero signal
#         - TRANSLATE high-performing queries to Swahili/Sheng
#    4. Suggestions saved to search_queries as 'pending'
#    5. Officers approve/reject in app.R dashboard
#    6. Active queries go live next pipeline_telegram.py run
#
#  FIXES IN THIS VERSION:
#    FIX-A  All execution wrapped in run_keyword_pipeline() —
#            safe to source from app.R without side-effects
#    FIX-B  OpenAI calls use req_retry() with exponential backoff
#            (max 3 attempts, handles 429/500/503 automatically)
#    FIX-C  System prompts loaded from prompts/ text files —
#            easy to tune without touching R code
#    FIX-D  Structured file logging to logs/kw_pipeline_YYYYMMDD.log
#    FIX-E  Auto-run guard for standalone vs app usage
#
#  Usage:
#    source("pipelines/pipeline_keywords.R")            # standalone
#    source("pipelines/pipeline_keywords.R"); run_keyword_pipeline()  # from app
# ================================================================

readRenviron("secrets/.Renviron")
library(httr2)
library(jsonlite)

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

# ── CREDENTIALS ───────────────────────────────────────────────────
supa_url   <- Sys.getenv("SUPABASE_URL")
supa_key   <- Sys.getenv("SUPABASE_KEY")
openai_key <- Sys.getenv("OPENAI_API_KEY")

if (nchar(supa_url)   == 0) stop("SUPABASE_URL not found")
if (nchar(supa_key)   == 0) stop("SUPABASE_KEY not found")
if (nchar(openai_key) == 0) stop("OPENAI_API_KEY not found")
message("✅ Credentials loaded")

# ── CONFIGURATION ─────────────────────────────────────────────────
BATCH_SIZE     <- 20L
MAX_BATCHES    <- 5L
GPT_MODEL      <- "gpt-4o-mini"
MIN_CONFIDENCE <- 0.7

PROMPTS_DIR    <- "prompts"
LOG_DIR        <- "logs"

# ── FIX-C: LOAD SYSTEM PROMPTS FROM FILES ────────────────────────
.load_prompt <- function(filename) {
  path <- file.path(PROMPTS_DIR, filename)
  if (!file.exists(path)) {
    stop(sprintf("Prompt file not found: %s\n  Run from project root or check PROMPTS_DIR", path))
  }
  paste(readLines(path, warn = FALSE), collapse = "\n")
}

# Loaded once at source time — fast subsequent calls
KW_EXTRACTION_PROMPT <- .load_prompt("kw_extraction_system.txt")
QUERY_ANALYSIS_PROMPT <- .load_prompt("query_analysis_system.txt")

# ── FIX-D: FILE LOGGER ───────────────────────────────────────────
.log_file <- NULL

.log_init <- function() {
  if (!dir.exists(LOG_DIR)) dir.create(LOG_DIR, recursive = TRUE)
  .log_file <<- file.path(LOG_DIR,
    sprintf("kw_pipeline_%s.log", format(Sys.time(), "%Y%m%d_%H%M%S")))
  cat(sprintf("=== Keyword Pipeline Log — %s ===\n\n",
              format(Sys.time(), "%d %b %Y %H:%M:%S EAT")),
      file = .log_file)
  invisible(.log_file)
}

log_msg <- function(...) {
  msg <- paste0(...)
  message(msg)
  if (!is.null(.log_file))
    cat(msg, "\n", file = .log_file, append = TRUE)
}

# ── SUPABASE HELPERS ──────────────────────────────────────────────
supa_get <- function(table, query_params = list()) {
  req <- request(paste0(supa_url, "/rest/v1/", table)) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key)
    )
  if (length(query_params) > 0)
    req <- req |> req_url_query(!!!query_params)
  resp <- req_perform(req)
  fromJSON(resp_body_string(resp), flatten = TRUE)
}

supa_insert <- function(table, rows_df) {
  req <- request(paste0(supa_url, "/rest/v1/", table)) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key),
      "Content-Type"  = "application/json",
      "Prefer"        = "return=minimal"
    ) |>
    req_body_raw(
      toJSON(rows_df, auto_unbox = TRUE, na = "null"),
      type = "application/json"
    ) |>
    req_method("POST")
  req_perform(req)
}

supa_upsert <- function(table, rows_df, on_conflict = "query") {
  req <- request(paste0(supa_url, "/rest/v1/", table)) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key),
      "Content-Type"  = "application/json",
      "Prefer"        = "return=minimal,resolution=ignore-duplicates"
    ) |>
    req_url_query(on_conflict = on_conflict) |>
    req_body_raw(
      toJSON(rows_df, auto_unbox = TRUE, na = "null"),
      type = "application/json"
    ) |>
    req_method("POST")
  req_perform(req)
}

# ── FIX-B: OPENAI REQUEST BUILDER WITH RETRY ─────────────────────
# req_retry() handles 429 (rate limit), 500, 503 automatically.
# max_tries = 3 means up to 3 attempts with exponential backoff.
.openai_post <- function(body_list) {
  request("https://api.openai.com/v1/chat/completions") |>
    req_headers(
      "Authorization" = paste("Bearer", openai_key),
      "Content-Type"  = "application/json"
    ) |>
    req_body_raw(
      toJSON(body_list, auto_unbox = TRUE),
      type = "application/json"
    ) |>
    req_method("POST") |>
    req_retry(
      max_tries      = 3L,
      retry_on_status = c(429L, 500L, 503L),
      backoff        = \(i) min(2^i * 2, 60)   # 4s, 8s, cap 60s
    ) |>
    req_timeout(120L) |>
    req_perform()
}

# ── DATA FETCHERS ─────────────────────────────────────────────────
fetch_search_queries <- function() {
  log_msg("[supabase] Fetching current search queries...")
  req <- request(paste0(supa_url, "/rest/v1/search_queries")) |>
    req_headers("apikey" = supa_key, "Authorization" = paste("Bearer", supa_key)) |>
    req_url_query(select = "query,status,language,priority,added_by,context")
  resp <- req_perform(req)
  df   <- fromJSON(resp_body_string(resp), flatten = TRUE)
  if (!is.data.frame(df) || nrow(df) == 0) return(data.frame())
  df
}

fetch_channel_performance <- function(top_n = 20L) {
  cache_file <- file.path("cache", "tg_channel_cache.rds")
  if (!file.exists(cache_file)) return(data.frame())
  cache <- readRDS(cache_file)
  if (nrow(cache) == 0) return(data.frame())
  head(cache[order(-cache$signal_score), ], top_n)
}

fetch_top_signal_messages <- function(limit = 50L) {
  req <- request(paste0(supa_url, "/rest/v1/pipeline_telegram")) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key),
      "Range"         = sprintf("0-%d", limit - 1L)
    ) |>
    req_url_query(
      select = "text,signal_score,ncic_category,chat_title",
      order  = "signal_score.desc"
    )
  resp <- req_perform(req)
  df   <- fromJSON(resp_body_string(resp), flatten = TRUE)
  if (!is.data.frame(df) || nrow(df) == 0) return(data.frame())
  df
}

fetch_recent_messages <- function(limit = 100L) {
  log_msg("[supabase] Fetching recent messages for keyword analysis...")
  req <- request(paste0(supa_url, "/rest/v1/pipeline_telegram")) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key),
      "Range"         = sprintf("0-%d", limit - 1L)
    ) |>
    req_url_query(
      select = "text,signal_score,ncic_category,triggers",
      order  = "created_at.desc"
    )
  resp <- req_perform(req)
  fromJSON(resp_body_string(resp), flatten = TRUE)
}

fetch_existing_keywords <- function() {
  log_msg("[supabase] Loading existing keyword bank...")
  req <- request(paste0(supa_url, "/rest/v1/keyword_bank")) |>
    req_headers("apikey" = supa_key, "Authorization" = paste("Bearer", supa_key)) |>
    req_url_query(select = "keyword")
  resp <- req_perform(req)
  df   <- fromJSON(resp_body_string(resp), flatten = TRUE)
  if (nrow(df) == 0) return(character())
  tolower(trimws(df$keyword))
}

# ── GPT KEYWORD EXTRACTION ────────────────────────────────────────
extract_keywords_gpt <- function(messages_text, existing_keywords) {

  user_prompt <- paste0(
    "Existing keywords (do NOT suggest these again):\n",
    paste(existing_keywords, collapse = ", "),
    "\n\n---\n\n",
    "Analyse these Kenyan social media messages and suggest NEW signal keywords:\n\n",
    messages_text,
    "\n\n---\n\n",
    "Return ONLY a JSON array of new keyword suggestions. ",
    "If no new keywords found, return empty array: []"
  )

  resp <- tryCatch(
    .openai_post(list(
      model           = GPT_MODEL,
      messages        = list(
        list(role = "system", content = KW_EXTRACTION_PROMPT),
        list(role = "user",   content = user_prompt)
      ),
      temperature     = 0.2,
      max_tokens      = 1000L,
      response_format = list(type = "json_object")
    )),
    error = function(e) {
      log_msg(sprintf("  ✗ OpenAI request failed after retries: %s", e$message))
      return(NULL)
    }
  )

  if (is.null(resp)) return(data.frame())

  result <- fromJSON(resp_body_string(resp), flatten = TRUE)
  raw    <- result$choices[[1]]$message$content %||% "[]"

  tryCatch({
    parsed <- fromJSON(raw, flatten = TRUE)
    if (is.data.frame(parsed)) return(parsed)
    if (is.list(parsed)) {
      for (val in parsed)
        if (is.data.frame(val)) return(val)
    }
    data.frame()
  }, error = function(e) {
    log_msg(sprintf("  ⚠ GPT parse error: %s", e$message))
    data.frame()
  })
}

# ── GPT SEARCH QUERY ANALYSIS ────────────────────────────────────
analyse_search_queries_gpt <- function(current_queries, channel_perf, top_messages) {

  queries_text <- if (nrow(current_queries) > 0) {
    paste(
      sapply(seq_len(nrow(current_queries)), function(i) {
        sprintf("[%s | %s | %s] %s",
          current_queries$status[i]   %||% "active",
          current_queries$priority[i] %||% "MEDIUM",
          current_queries$language[i] %||% "en",
          current_queries$query[i])
      }),
      collapse = "\n"
    )
  } else { "(none yet)" }

  channels_text <- if (nrow(channel_perf) > 0) {
    paste(
      sapply(seq_len(nrow(channel_perf)), function(i) {
        sprintf("[score:%.1f | msgs:%d] %s",
          channel_perf$signal_score[i],
          channel_perf$msg_count[i],
          channel_perf$chat_title[i])
      }),
      collapse = "\n"
    )
  } else { "(no channel data yet)" }

  messages_text <- if (nrow(top_messages) > 0) {
    paste(
      sapply(seq_len(min(30L, nrow(top_messages))), function(i) {
        sprintf("[score:%d | %s] %s",
          top_messages$signal_score[i],
          top_messages$ncic_category[i] %||% "",
          substr(top_messages$text[i], 1, 200))
      }),
      collapse = "\n\n"
    )
  } else { "(no signal messages yet)" }

  user_prompt <- paste0(
    "CURRENT SEARCH QUERIES (status | priority | language):\n",
    queries_text,
    "\n\n---\n\n",
    "TOP SIGNAL CHANNELS discovered so far (score | messages):\n",
    channels_text,
    "\n\n---\n\n",
    "TOP SIGNAL MESSAGES from recent runs:\n\n",
    messages_text,
    "\n\n---\n\n",
    "Based on the above, suggest improvements to the search query list. ",
    "Focus on finding MORE channels like the high-signal ones above. ",
    "Return ONLY the JSON object with a 'suggestions' array."
  )

  resp <- tryCatch(
    .openai_post(list(
      model           = GPT_MODEL,
      messages        = list(
        list(role = "system", content = QUERY_ANALYSIS_PROMPT),
        list(role = "user",   content = user_prompt)
      ),
      temperature     = 0.3,
      max_tokens      = 1200L,
      response_format = list(type = "json_object")
    )),
    error = function(e) {
      log_msg(sprintf("  ✗ OpenAI request failed after retries: %s", e$message))
      return(NULL)
    }
  )

  if (is.null(resp)) return(data.frame())

  result <- fromJSON(resp_body_string(resp), flatten = TRUE)
  raw    <- result$choices[[1]]$message$content %||% "{}"

  tryCatch({
    parsed <- fromJSON(raw, flatten = TRUE)
    if (is.list(parsed) && "suggestions" %in% names(parsed)) {
      s <- parsed$suggestions
      if (is.data.frame(s)) return(s)
    }
    if (is.data.frame(parsed)) return(parsed)
    data.frame()
  }, error = function(e) {
    log_msg(sprintf("  ⚠ GPT parse error (search queries): %s", e$message))
    data.frame()
  })
}

# ── FIX-A: PART A — KEYWORD EXTRACTION FUNCTION ──────────────────
run_keyword_extraction <- function() {

  log_msg("\n", strrep("=", 60))
  log_msg("PART A — KEYWORD EXTRACTION")
  log_msg(strrep("=", 60))

  messages_df <- fetch_recent_messages(limit = BATCH_SIZE * MAX_BATCHES)
  log_msg(sprintf("[load] %d messages loaded for analysis", nrow(messages_df)))

  if (nrow(messages_df) == 0) {
    log_msg("⚠ No messages found — run pipeline_telegram.R first")
    return(data.frame())
  }

  existing_kws <- fetch_existing_keywords()
  log_msg(sprintf("[keywords] %d existing keywords loaded", length(existing_kws)))

  all_suggestions <- data.frame()
  n_batches       <- min(MAX_BATCHES, ceiling(nrow(messages_df) / BATCH_SIZE))

  log_msg(sprintf("\n[gpt] Processing %d batches of %d messages each...",
                  n_batches, BATCH_SIZE))

  for (b in seq_len(n_batches)) {
    start  <- (b - 1) * BATCH_SIZE + 1L
    end    <- min(b * BATCH_SIZE, nrow(messages_df))
    batch  <- messages_df[start:end, ]

    msgs_text <- paste(
      sapply(seq_len(nrow(batch)), function(i)
        sprintf("[%d] %s", i, substr(batch$text[i], 1, 300))),
      collapse = "\n\n"
    )

    log_msg(sprintf("  Batch %d/%d (%d messages)...", b, n_batches, nrow(batch)))

    suggestions <- tryCatch(
      extract_keywords_gpt(msgs_text, existing_kws),
      error = function(e) {
        log_msg(sprintf("  ❌ Batch %d error: %s", b, e$message))
        data.frame()
      }
    )

    if (nrow(suggestions) > 0) {
      log_msg(sprintf("  ✅ %d new keywords suggested", nrow(suggestions)))
      all_suggestions <- rbind(all_suggestions, suggestions)
    } else {
      log_msg(sprintf("  ℹ No new keywords found in batch %d", b))
    }

    Sys.sleep(1)
  }

  if (nrow(all_suggestions) == 0) {
    log_msg("\n[gpt] No new keywords suggested this run")
    return(data.frame())
  }

  # Filter, deduplicate, validate
  required_cols <- c("keyword","tier","category","language","confidence","context")
  missing <- setdiff(required_cols, names(all_suggestions))
  for (col in missing) all_suggestions[[col]] <- NA

  all_suggestions <- all_suggestions[
    !is.na(all_suggestions$confidence) &
    all_suggestions$confidence >= MIN_CONFIDENCE, ]

  all_suggestions <- all_suggestions[
    !tolower(trimws(all_suggestions$keyword)) %in% existing_kws, ]

  all_suggestions <- all_suggestions[
    !duplicated(tolower(trimws(all_suggestions$keyword))), ]

  log_msg(sprintf("\n[suggestions] %d new keywords after filtering", nrow(all_suggestions)))

  if (nrow(all_suggestions) == 0) return(data.frame())

  to_insert <- data.frame(
    keyword       = tolower(trimws(all_suggestions$keyword)),
    tier          = as.integer(all_suggestions$tier),
    category      = all_suggestions$category,
    language      = all_suggestions$language %||% "en",
    status        = "pending",
    source        = "gpt",
    suggested_by  = GPT_MODEL,
    context       = all_suggestions$context %||% "",
    times_matched = 0L,
    stringsAsFactors = FALSE
  )

  resp <- supa_insert("keyword_bank", to_insert)

  if (resp_status(resp) %in% c(200L, 201L, 204L)) {
    log_msg(sprintf("✅ %d keywords saved to keyword_bank as PENDING", nrow(to_insert)))
  } else {
    log_msg(sprintf("❌ Insert failed: %s", resp_body_string(resp)))
  }

  log_msg("\n[suggested keywords for officer review]")
  log_msg(sprintf("%-30s %5s %-25s %-6s %s",
                  "Keyword", "Tier", "Category", "Lang", "Confidence"))
  log_msg(strrep("-", 80))
  for (i in seq_len(nrow(all_suggestions)))
    log_msg(sprintf("%-30s %5d %-25s %-6s %.2f",
                    substr(all_suggestions$keyword[i], 1, 30),
                    all_suggestions$tier[i],
                    all_suggestions$category[i],
                    all_suggestions$language[i] %||% "en",
                    all_suggestions$confidence[i]))

  invisible(to_insert)
}

# ── PART B — SEARCH QUERY ANALYSIS FUNCTION ──────────────────────
run_query_analysis <- function() {

  log_msg("\n", strrep("=", 60))
  log_msg("PART B — SEARCH QUERY ANALYSIS")
  log_msg(format(Sys.time(), "%d %b %Y %H:%M EAT"))
  log_msg(strrep("=", 60))

  current_queries <- tryCatch(fetch_search_queries(),    error = function(e) data.frame())
  channel_perf    <- tryCatch(fetch_channel_performance(), error = function(e) data.frame())
  top_messages    <- tryCatch(fetch_top_signal_messages(), error = function(e) data.frame())

  log_msg(sprintf("[load] %d current queries | %d channels | %d top messages",
                  nrow(current_queries), nrow(channel_perf), nrow(top_messages)))

  existing_queries <- if (nrow(current_queries) > 0)
    tolower(trimws(current_queries$query))
  else character()

  log_msg("\n[gpt] Analysing search query performance...")
  sq_suggestions <- tryCatch(
    analyse_search_queries_gpt(current_queries, channel_perf, top_messages),
    error = function(e) {
      log_msg(sprintf("  ❌ GPT error: %s", e$message))
      data.frame()
    }
  )

  if (nrow(sq_suggestions) == 0) {
    log_msg("[gpt] No search query suggestions this run")
    return(data.frame())
  }

  # Validate
  required <- c("query","action","language","priority","confidence","context")
  for (col in setdiff(required, names(sq_suggestions)))
    sq_suggestions[[col]] <- NA

  sq_suggestions <- sq_suggestions[
    !is.na(sq_suggestions$confidence) &
    sq_suggestions$confidence >= MIN_CONFIDENCE, ]

  valid_actions  <- c("new", "promote", "deactivate", "translate")
  sq_suggestions <- sq_suggestions[
    sq_suggestions$action %in% valid_actions, ]

  is_new_or_translate <- sq_suggestions$action %in% c("new", "translate")
  already_exists      <- tolower(trimws(sq_suggestions$query)) %in% existing_queries
  sq_suggestions      <- sq_suggestions[!is_new_or_translate | !already_exists, ]

  sq_suggestions <- sq_suggestions[
    !duplicated(tolower(trimws(sq_suggestions$query))), ]

  log_msg(sprintf("[suggestions] %d search query suggestions after filtering",
                  nrow(sq_suggestions)))

  if (nrow(sq_suggestions) == 0) return(data.frame())

  to_insert <- data.frame(
    query        = trimws(sq_suggestions$query),
    status       = "pending",
    language     = sq_suggestions$language  %||% "en",
    priority     = sq_suggestions$priority  %||% "MEDIUM",
    added_by     = GPT_MODEL,
    action       = sq_suggestions$action,
    context      = sq_suggestions$context   %||% "",
    confidence   = round(as.numeric(sq_suggestions$confidence), 2),
    stringsAsFactors = FALSE
  )

  resp <- supa_upsert("search_queries", to_insert, on_conflict = "query")

  if (resp_status(resp) %in% c(200L, 201L, 204L)) {
    log_msg(sprintf("✅ %d query suggestions saved to search_queries as PENDING",
                    nrow(to_insert)))
  } else {
    log_msg(sprintf("❌ Insert failed: %s", resp_body_string(resp)))
  }

  log_msg("\n[suggested search queries for officer review]")
  log_msg(sprintf("%-40s %-12s %-8s %-8s %s",
                  "Query", "Action", "Priority", "Lang", "Confidence"))
  log_msg(strrep("-", 85))
  for (i in seq_len(nrow(sq_suggestions)))
    log_msg(sprintf("%-40s %-12s %-8s %-8s %.2f",
      substr(sq_suggestions$query[i], 1, 40),
      sq_suggestions$action[i]    %||% "",
      sq_suggestions$priority[i]  %||% "",
      sq_suggestions$language[i]  %||% "",
      sq_suggestions$confidence[i]))

  invisible(to_insert)
}

# ── COMBINED ENTRY POINT ──────────────────────────────────────────
run_keyword_pipeline <- function() {

  .log_init()

  log_msg("\n", strrep("=", 60))
  log_msg("ADAPTIVE KEYWORD EXTRACTION PIPELINE")
  log_msg(format(Sys.time(), "%d %b %Y %H:%M EAT"))
  log_msg(strrep("=", 60))

  kw_result <- tryCatch(run_keyword_extraction(), error = function(e) {
    log_msg(sprintf("❌ Keyword extraction failed: %s", e$message))
    data.frame()
  })

  sq_result <- tryCatch(run_query_analysis(), error = function(e) {
    log_msg(sprintf("❌ Query analysis failed: %s", e$message))
    data.frame()
  })

  # Combined summary
  log_msg("\n", strrep("=", 60))
  log_msg("COMBINED SUMMARY")
  log_msg(strrep("=", 60))

  kb <- tryCatch(supa_get("keyword_bank"),   error = function(e) data.frame())
  sq <- tryCatch(supa_get("search_queries"), error = function(e) data.frame())

  if (nrow(kb) > 0) {
    log_msg("\nKeyword Bank:")
    for (s in names(table(kb$status)))
      log_msg(sprintf("  %-10s : %d keywords", s, table(kb$status)[s]))
    log_msg(sprintf("  %-10s : %d keywords", "TOTAL", nrow(kb)))
  }

  if (nrow(sq) > 0) {
    log_msg("\nSearch Queries:")
    for (s in names(table(sq$status)))
      log_msg(sprintf("  %-10s : %d queries", s, table(sq$status)[s]))
    log_msg(sprintf("  %-10s : %d queries", "TOTAL", nrow(sq)))
  }

  log_msg("\n💡 Review all pending items in the app.R officer dashboard")
  log_msg(sprintf("📄 Log written to: %s", .log_file))
  log_msg(strrep("=", 60))

  invisible(list(
    keywords_suggested = nrow(kw_result),
    queries_suggested  = nrow(sq_result),
    log_file           = .log_file
  ))
}

# ── FIX-E: AUTO-RUN WHEN SOURCED STANDALONE ──────────────────────
if (!exists(".pipeline_sourced_by_app")) {
  run_keyword_pipeline()
}
