# ================================================================
#  supabase_helpers.R — Supabase API helpers
#  Radicalisation Signals · IEA Kenya NIRU AI Hackathon
#
#  Drop-in replacement for SQLite db_* functions in app.R.
#  Field mapping:
#    SQLite tweet_text  →  Supabase text
#    SQLite tweet       →  Supabase text  (examples/disagreements)
#
#  Usage: source("supabase_helpers.R")  at top of app.R
#         then replace db_* calls with supa_* calls
# ================================================================

library(httr2)
library(jsonlite)

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

SUPA_URL <- Sys.getenv("SUPABASE_URL")
SUPA_KEY <- Sys.getenv("SUPABASE_KEY")

if (nchar(SUPA_URL) == 0) stop("SUPABASE_URL not set in .Renviron")
if (nchar(SUPA_KEY) == 0) stop("SUPABASE_KEY not set in .Renviron")

# ── LOW-LEVEL HELPERS ─────────────────────────────────────────────

# Standard headers for all requests
.supa_hdrs <- function(prefer = "return=representation") {
  list(
    "apikey"        = SUPA_KEY,
    "Authorization" = paste("Bearer", SUPA_KEY),
    "Content-Type"  = "application/json",
    "Prefer"        = prefer
  )
}

# Build base request
.supa_req <- function(table, prefer = "return=representation") {
  req <- request(paste0(SUPA_URL, "/rest/v1/", table))
  do.call(req_headers, c(list(req), .supa_hdrs(prefer)))
}

# SELECT with optional filters, order, limit, offset
supa_select <- function(table, select = "*", filters = list(),
                        order = NULL, limit = NULL, offset = NULL) {
  params <- list(select = select)
  if (!is.null(order))  params$order  <- order
  if (!is.null(limit))  params$limit  <- as.integer(limit)
  if (!is.null(offset)) params$offset <- as.integer(offset)
  params <- c(params, filters)

  req <- .supa_req(table)
  req <- do.call(req_url_query, c(list(req), params))
  req |>
    req_error(is_error = \(r) FALSE) |>
    req_perform() |>
    resp_body_json(simplifyVector = TRUE)
}

# INSERT — returns inserted rows (or nothing if prefer=return=minimal)
supa_insert <- function(table, data_df,
                        prefer = "return=representation") {
  resp <- .supa_req(table, prefer) |>
    req_body_json(
      jsonlite::fromJSON(jsonlite::toJSON(data_df, na = "null"))
    ) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
  # Only parse body when content is expected
  if (grepl("return=minimal", prefer) ||
      resp_status(resp) == 204L)
    return(invisible(resp))
  tryCatch(
    resp_body_json(resp, simplifyVector = TRUE),
    error = function(e) invisible(resp)
  )
}

# UPSERT — insert or update on conflict
supa_upsert <- function(table, data_df,
                        on_conflict = NULL,
                        prefer = "resolution=merge-duplicates,return=minimal") {
  req <- .supa_req(table, prefer)
  if (!is.null(on_conflict))
    req <- req_url_query(req, on_conflict = on_conflict)
  req |>
    req_body_json(
      jsonlite::fromJSON(jsonlite::toJSON(data_df, na = "null"))
    ) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
}
# PATCH (UPDATE) — eq filter
supa_update <- function(table, data_list, eq_col, eq_val) {
  filter_params <- list(paste0("eq.", eq_val))
  names(filter_params) <- eq_col
  req <- .supa_req(table, "return=minimal") |>
    req_method("PATCH")
  req <- do.call(req_url_query, c(list(req), filter_params))
  req |>
    req_body_json(data_list) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
}

# DELETE — eq filter
supa_delete <- function(table, eq_col, eq_val) {
  filter_params <- list(paste0("eq.", eq_val))
  names(filter_params) <- eq_col
  req <- .supa_req(table, "return=minimal") |>
    req_method("DELETE")
  req <- do.call(req_url_query, c(list(req), filter_params))
  req |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
}

# COUNT — exact row count
# Uses HEAD request with count=exact — most reliable approach
supa_count <- function(table, filters = list()) {
  params <- c(list(select = "*"), filters)
  req <- request(paste0(SUPA_URL, "/rest/v1/", table)) |>
    req_headers(
      "apikey"        = SUPA_KEY,
      "Authorization" = paste("Bearer", SUPA_KEY),
      "Prefer"        = "count=exact"
    ) |>
    req_method("HEAD")
  req <- do.call(req_url_query, c(list(req), params))
  resp <- req |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
  cr <- tryCatch(resp_header(resp, "content-range"),
                 error = function(e) NULL)
  if (is.null(cr) || is.na(cr) || nchar(cr) == 0) return(0L)
  total <- sub(".*/", "", cr)
  as.integer(total) %||% 0L
}

# FETCH ALL — paginates past the 1,000 row default limit
supa_fetch_all <- function(table, select = "*", filters = list(),
                           order = NULL, page_size = 1000L) {
  all_rows  <- NULL
  offset    <- 0L
  page_size <- as.integer(page_size)

  repeat {
    page <- supa_select(table, select = select, filters = filters,
                        order = order,
                        limit = page_size, offset = offset)
    if (!is.data.frame(page) || nrow(page) == 0) break
    all_rows <- if (is.null(all_rows)) page else rbind(all_rows, page)
    if (nrow(page) < page_size) break
    offset <- offset + page_size
  }
  all_rows %||% data.frame()
}

# ── CASES ─────────────────────────────────────────────────────────

# Load all cases — mirrors db_load_cases()
# Note: Supabase uses 'text' field; app.R uses 'tweet_text'
# We rename on load so app.R needs zero changes
supa_load_cases <- function() {
  message("[supabase] Loading cases...")
  df <- supa_fetch_all("cases", order = "timestamp.desc")
  if (!is.data.frame(df) || nrow(df) == 0) {
    message("[supabase] No cases found")
    return(NULL)
  }
  # Rename 'text' → 'tweet_text' so app.R works without changes
  if ("text" %in% names(df) && !"tweet_text" %in% names(df))
    names(df)[names(df) == "text"] <- "tweet_text"

  # Coerce types
  df$ncic_level  <- as.integer(df$ncic_level  %||% 0)
  df$risk_score  <- as.integer(df$risk_score  %||% 0)
  df$conf_num    <- as.integer(df$conf_num    %||% 0)
  df$section_13  <- as.integer(df$section_13  %||% 0)
  df$kw_score    <- as.integer(df$kw_score    %||% 0)
  df$network_score <- as.integer(df$network_score %||% 20)

  # Parse timestamp
  df$timestamp <- tryCatch(
    as.POSIXct(df$timestamp,
               format = "%Y-%m-%dT%H:%M:%OSZ",
               tz = "Africa/Nairobi"),
    error = function(e) as.POSIXct(Sys.time())
  )

  # Backfill risk_level if missing
  df$risk_level <- ifelse(
    is.na(df$risk_level) | df$risk_level == "",
    ifelse(df$risk_score >= 65, "HIGH",
           ifelse(df$risk_score >= 35, "MEDIUM", "LOW")),
    df$risk_level
  )

  message(sprintf("[supabase] Loaded %d cases", nrow(df)))
  df
}

# Save / upsert cases — mirrors db_save_cases()
supa_save_cases <- function(cases_df) {
  df <- cases_df
  if ("tweet_text" %in% names(df) && !"text" %in% names(df))
    names(df)[names(df) == "tweet_text"] <- "text"
  drop_cols <- c("timestamp_chr")
  df <- df[, !names(df) %in% drop_cols, drop=FALSE]
  if ("timestamp" %in% names(df))
    df$timestamp <- format(as.POSIXct(df$timestamp), "%Y-%m-%dT%H:%M:%SZ")

  # Convert to list with explicit NULLs — required by Supabase
  rows_list <- lapply(seq_len(nrow(df)), function(i) {
    row <- as.list(df[i, ])
    lapply(row, function(v) if (length(v)==1 && is.na(v)) NULL else v)
  })

  batch_size <- 100L
  n <- nrow(df)
  for (i in seq(1, n, by=batch_size)) {
    end   <- min(i + batch_size - 1, n)
    batch <- rows_list[i:end]
    request(paste0(SUPA_URL, "/rest/v1/cases")) |>
      req_headers(
        "apikey"        = SUPA_KEY,
        "Authorization" = paste("Bearer", SUPA_KEY),
        "Content-Type"  = "application/json",
        "Prefer"        = "resolution=merge-duplicates,return=minimal"
      ) |>
      req_body_raw(
        jsonlite::toJSON(batch, auto_unbox=TRUE, null="null"),
        type="application/json"
      ) |>
      req_error(is_error = \(r) FALSE) |>
      req_perform()
  }
  message(sprintf("[supabase] Saved %d cases", n))
}

# Update a single case — mirrors db_update_case()
supa_update_case <- function(case_id, fields_list) {
  # Rename tweet_text if present
  if ("tweet_text" %in% names(fields_list)) {
    fields_list[["text"]] <- fields_list[["tweet_text"]]
    fields_list[["tweet_text"]] <- NULL
  }
  supa_update("cases", fields_list, "case_id", case_id)
}

# ── KEYWORD WEIGHTS ───────────────────────────────────────────────

# Load weights — mirrors db_load_weights()
supa_load_weights <- function() {
  rows <- supa_fetch_all("keyword_weights", order = "id.asc")
  if (!is.data.frame(rows) || nrow(rows) == 0) return(NULL)
  setNames(as.list(rows$kw_value), rows$kw_key)
}

# Save weights — mirrors db_save_weights()
supa_save_weights <- function(weights_list, officer = "system") {
  df <- data.frame(
    kw_key     = names(weights_list),
    kw_value   = unlist(weights_list),
    updated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    updated_by = officer,
    stringsAsFactors = FALSE
  )
  # Delete existing and re-insert (simpler than per-key upsert)
  # Using upsert on kw_key
  request(paste0(SUPA_URL, "/rest/v1/keyword_weights")) |>
    req_headers(
      "apikey"        = SUPA_KEY,
      "Authorization" = paste("Bearer", SUPA_KEY),
      "Content-Type"  = "application/json",
      "Prefer"        = "resolution=merge-duplicates,return=minimal"
    ) |>
    req_body_json(
      jsonlite::fromJSON(jsonlite::toJSON(df, na = "null"))
    ) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
}

# ── EXAMPLES (few-shot bank) ──────────────────────────────────────

# Load examples — mirrors load_examples()
supa_load_examples <- function() {
  rows <- supa_fetch_all("examples", order = "ts.desc")
  if (!is.data.frame(rows) || nrow(rows) == 0) return(list())
  # Rename text → tweet for app.R compatibility
  if ("text" %in% names(rows) && !"tweet" %in% names(rows))
    names(rows)[names(rows) == "text"] <- "tweet"
  lapply(seq_len(nrow(rows)), function(i) as.list(rows[i, ]))
}

# Add example — mirrors db_add_example()
supa_add_example <- function(tweet, ncic_level, category,
                             reasoning, outcome, source = "manual") {
  supa_insert("examples", data.frame(
    text       = substr(tweet, 1, 1000),
    ncic_level = as.character(ncic_level),
    category   = category,
    reasoning  = reasoning,
    outcome    = outcome,
    source     = source,
    ts         = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    stringsAsFactors = FALSE
  ))
}

# ── DISAGREEMENTS ─────────────────────────────────────────────────

# Log disagreement — mirrors db_log_disagreement()
supa_log_disagreement <- function(case_id, tweet, gpt_level,
                                  officer_level, officer) {
  supa_insert("disagreements", data.frame(
    ts            = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    case_id       = case_id,
    text          = substr(tweet, 1, 500),
    gpt_level     = as.integer(gpt_level),
    officer_level = as.integer(officer_level),
    officer       = officer,
    stringsAsFactors = FALSE
  ))
}

# Load disagreements
supa_load_disagreements <- function(limit = 500) {
  supa_fetch_all("disagreements", order = "ts.desc")
}

# ── AUDIT LOG ─────────────────────────────────────────────────────

# Write audit entry — mirrors audit()
supa_audit <- function(officer, officer_name, action,
                       case_id = NA, detail = NA, session_id = NA) {
  supa_insert("audit_log", data.frame(
    ts           = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    officer      = officer,
    officer_name = officer_name,
    action       = action,
    case_id      = if (is.na(case_id))    NA_character_ else as.character(case_id),
    detail       = if (is.na(detail))     NA_character_ else substr(as.character(detail), 1, 500),
    session_id   = if (is.na(session_id)) NA_character_ else session_id,
    stringsAsFactors = FALSE
  ), prefer = "return=minimal")
}

# Load audit log — mirrors db_load_audit()
supa_load_audit <- function(limit = 200) {
  supa_select("audit_log", order = "ts.desc", limit = limit)
}

# ── S13 QUEUE ─────────────────────────────────────────────────────

# Enqueue S13 case — mirrors db_s13_enqueue()
supa_s13_enqueue <- function(case_row, officer) {
  # Check not already queued
  existing <- supa_select("s13_queue",
    filters = list(case_id = paste0("eq.", case_row$case_id)),
    limit = 1)
  if (is.data.frame(existing) && nrow(existing) > 0)
    return(invisible(NULL))

  tweet_text <- case_row$tweet_text %||% case_row$text %||% ""

  supa_insert("s13_queue", data.frame(
    case_id      = case_row$case_id,
    county       = case_row$county       %||% "Unknown",
    platform     = case_row$platform     %||% "Unknown",
    text         = substr(tweet_text, 1, 500),
    ncic_level   = as.integer(case_row$ncic_level  %||% 0),
    risk_score   = as.integer(case_row$risk_score  %||% 0),
    target_group = case_row$target_group %||% "",
    validated_by = officer,
    validated_at = format(Sys.time(), "%Y-%m-%d %H:%M"),
    status       = "PENDING",
    created_at   = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    updated_at   = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    stringsAsFactors = FALSE
  ), prefer = "return=minimal")
}

# Update S13 status — mirrors db_s13_update_status()
supa_s13_update_status <- function(id, status,
                                   dci_ref = NA, notes = NA) {
  supa_update("s13_queue",
    list(
      status     = status,
      dci_ref    = if (is.na(dci_ref)) NULL else dci_ref,
      notes      = if (is.na(notes))   NULL else notes,
      updated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
    ),
    "id", id)
}

# Load S13 queue — mirrors db_load_s13_queue()
supa_load_s13_queue <- function() {
  df <- supa_fetch_all("s13_queue", order = "created_at.desc")
  if (!is.data.frame(df) || nrow(df) == 0) return(data.frame())
  # Rename text → tweet_text for app.R compatibility
  if ("text" %in% names(df) && !"tweet_text" %in% names(df))
    names(df)[names(df) == "text"] <- "tweet_text"
  df
}

# Backfill S13 queue from high-severity validated cases
supa_s13_backfill <- function() {
  existing <- supa_select("s13_queue", select = "case_id")
  existing_ids <- if (is.data.frame(existing) && nrow(existing) > 0)
    existing$case_id else character(0)

  high_cases <- supa_fetch_all("cases",
    filters = list(
      ncic_level   = "gte.4",
      validated_by = "not.is.null"
    ))

  if (!is.data.frame(high_cases) || nrow(high_cases) == 0) return(0L)

  new_cases <- high_cases[!high_cases$case_id %in% existing_ids, ]
  if (nrow(new_cases) == 0) return(0L)

  for (i in seq_len(nrow(new_cases))) {
    row <- as.list(new_cases[i, ])
    supa_s13_enqueue(row, row$validated_by %||% "system")
  }
  message(sprintf("[supabase] S13 backfill: %d cases added", nrow(new_cases)))
  nrow(new_cases)
}

# ── OFFICERS ──────────────────────────────────────────────────────

# Verify officer login — mirrors db_verify_officer()
supa_verify_officer <- function(username, password_plain) {
  rows <- supa_select("officers",
    filters = list(
      username = paste0("eq.", username),
      status   = "eq.ACTIVE"
    ), limit = 1)

  if (!is.data.frame(rows) || nrow(rows) == 0) return(NULL)
  officer <- rows[1, ]

  if (!bcrypt::checkpw(password_plain, officer$password_hash))
    return(NULL)

  supa_update("officers",
    list(last_login = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")),
    "username", username)

  officer
}

# Load officer roster — mirrors db_load_officers()
supa_load_officers <- function() {
  supa_select("officers",
    select = "id,username,role,full_name,email,status,last_login,created_at",
    order  = "created_at.asc")
}

# ── API FAILURES ──────────────────────────────────────────────────

# Log API failure — mirrors db_log_api_failure()
supa_log_api_failure <- function(case_id, text, error_msg) {
  supa_insert("api_failures", data.frame(
    ts        = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    case_id   = case_id,
    text      = substr(text, 1, 500),
    error_msg = substr(error_msg, 1, 500),
    resolved  = 0L,
    stringsAsFactors = FALSE
  ), prefer = "return=minimal")
}

# ── CONNECTION TEST ───────────────────────────────────────────────

supa_ping <- function() {
  tryCatch({
    resp <- request(paste0(SUPA_URL, "/rest/v1/cases")) |>
      req_headers(
        "apikey"        = SUPA_KEY,
        "Authorization" = paste("Bearer", SUPA_KEY)
      ) |>
      req_url_query(select = "case_id", limit = 1) |>
      req_error(is_error = \(r) FALSE) |>
      req_perform()
    resp_status(resp) %in% c(200L, 206L)
  }, error = function(e) FALSE)
}

# ── STARTUP ───────────────────────────────────────────────────────
message("[supabase_helpers] Loaded — testing connection...")
if (supa_ping()) {
  n <- supa_count("cases")
  message(sprintf("[supabase_helpers] Connected — %d cases in DB", n))
} else {
  warning("[supabase_helpers] Could not connect — check SUPABASE_URL and SUPABASE_KEY")
}
