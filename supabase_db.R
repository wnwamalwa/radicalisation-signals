# ================================================================
#  supabase_db.R — Complete Supabase Database Layer
#  Radicalisation Signals · IEA Kenya NIRU
#
#  Replaces all SQLite db_* functions with Supabase REST API calls.
#  Drop-in replacement — function signatures unchanged.
#
#  USAGE: source("supabase_db.R") in app.R after readRenviron()
# ================================================================

# ── SUPABASE CONNECTION HELPERS ───────────────────────────────────
.supa_url <- function() Sys.getenv("SUPABASE_URL")
.supa_key <- function() Sys.getenv("SUPABASE_KEY")

.supa_req <- function(table, method="GET") {
  request(paste0(.supa_url(), "/rest/v1/", table)) |>
    req_headers(
      "apikey"        = .supa_key(),
      "Authorization" = paste("Bearer", .supa_key()),
      "Content-Type"  = "application/json",
      "Prefer"        = "return=representation"
    ) |>
    req_method(method) |>
    req_error(is_error = \(r) FALSE)
}

.supa_get <- function(table, query_params=list(), limit=10000L) {
  req <- .supa_req(table, "GET")
  if (length(query_params) > 0)
    req <- do.call(req_url_query, c(list(req), query_params))
  req <- req_url_query(req, limit=limit)
  resp <- req_perform(req)
  if (resp_status(resp) >= 400) return(data.frame())
  result <- tryCatch(
    resp_body_json(resp, simplifyVector=TRUE),
    error = function(e) data.frame()
  )
  if (length(result) == 0) return(data.frame())
  as.data.frame(result)
}

.supa_post <- function(table, data, upsert=FALSE) {
  req <- .supa_req(table, "POST")
  if (upsert)
    req <- req_headers(req, "Prefer"="resolution=merge-duplicates,return=minimal")
  else
    req <- req_headers(req, "Prefer"="return=minimal")
  req <- req_body_raw(req,
    jsonlite::toJSON(data, auto_unbox=TRUE, na="null"),
    type="application/json")
  req_perform(req)
}

.supa_patch <- function(table, filter_col, filter_val, data) {
  req <- .supa_req(table, "PATCH") |>
    req_url_query(setNames(list(paste0("eq.", filter_val)), filter_col)) |>
    req_headers("Prefer"="return=minimal") |>
    req_body_raw(
      jsonlite::toJSON(data, auto_unbox=TRUE, na="null"),
      type="application/json")
  req_perform(req)
}

.supa_delete <- function(table, filter_col, filter_val) {
  req <- .supa_req(table, "DELETE") |>
    req_url_query(setNames(list(paste0("eq.", filter_val)), filter_col))
  req_perform(req)
}

# ── db_init ───────────────────────────────────────────────────────
# No-op for Supabase — tables already exist
db_init <- function() {
  message("[db] Supabase mode — no init needed")
  invisible(TRUE)
}

# ── db_connect ────────────────────────────────────────────────────
# No-op for Supabase — kept for compatibility
db_connect <- function() {
  list(supabase=TRUE, url=.supa_url())
}
dbDisconnect <- function(con, ...) invisible(NULL)

# ── OFFICER MANAGEMENT ─────────────────────────────────────────────
db_seed_admin <- function() {
  u  <- Sys.getenv("ADMIN_USERNAME")
  p  <- Sys.getenv("ADMIN_PASSWORD")
  if (!nchar(u) || !nchar(p)) {
    warning("[auth] ADMIN_USERNAME / ADMIN_PASSWORD not set — no admin seeded")
    return(invisible(NULL))
  }
  existing <- .supa_get("officers", list(username=paste0("eq.", u)))
  if (nrow(existing) > 0) {
    message(sprintf("[auth] Admin '%s' already exists", u))
    return(invisible(NULL))
  }
  hash <- bcrypt::hashpw(p)
  .supa_post("officers", data.frame(
    username      = u,
    password_hash = hash,
    role          = "admin",
    active        = 1L,
    created_at    = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    stringsAsFactors = FALSE
  ))
  message(sprintf("[auth] Admin '%s' seeded", u))
}

db_check_password <- function(username, password) {
  row <- .supa_get("officers",
    list(username=paste0("eq.", username), active="eq.1"))
  if (nrow(row) == 0) return(NULL)
  if (!bcrypt::checkpw(password, row$password_hash[1])) return(NULL)
  list(username=row$username[1], role=row$role[1],
       name=row$username[1])
}

db_add_officer <- function(username, password, role="officer") {
  if (nchar(password) < 10)
    stop("Password must be at least 10 characters")
  hash <- bcrypt::hashpw(password)
  resp <- .supa_post("officers", data.frame(
    username      = username,
    password_hash = hash,
    role          = role,
    active        = 1L,
    created_at    = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    stringsAsFactors = FALSE
  ))
  resp_status(resp) %in% c(200L, 201L, 204L)
}

db_deactivate_officer <- function(username) {
  .supa_patch("officers", "username", username, list(active=0L))
}

db_load_officers <- function() {
  .supa_get("officers")
}

# ── AUDIT LOG ──────────────────────────────────────────────────────
audit <- function(officer, officer_name, action, case_id=NA,
                  detail=NA, session_id=NA) {
  .supa_post("audit_log", data.frame(
    ts           = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    officer      = officer %||% "",
    officer_name = officer_name %||% "",
    action       = action,
    case_id      = case_id %||% NA_character_,
    detail       = detail  %||% NA_character_,
    session_id   = session_id %||% NA_character_,
    stringsAsFactors = FALSE
  ))
  invisible(NULL)
}

db_load_audit <- function(limit=500L) {
  .supa_get("audit_log", list(order="ts.desc"), limit=limit)
}

# ── CASES ──────────────────────────────────────────────────────────
db_load_cases <- function() {
  d <- .supa_get("cases", list(order="timestamp.desc"), limit=50000L)
  if (nrow(d) == 0) return(NULL)
  # Ensure correct types
  int_cols <- c("ncic_level","section_13","conf_num","officer_ncic_override",
                "risk_score","kw_score","network_score")
  for (col in intersect(int_cols, names(d)))
    d[[col]] <- suppressWarnings(as.integer(d[[col]]))
  d
}

db_save_cases <- function(df) {
  if (nrow(df) == 0) return(invisible(NULL))
  batch_size <- 100L
  for (i in seq_len(ceiling(nrow(df)/batch_size))) {
    start <- (i-1)*batch_size + 1L
    end   <- min(i*batch_size, nrow(df))
    batch <- df[start:end, ]
    .supa_post("cases", batch, upsert=TRUE)
  }
  invisible(NULL)
}

db_update_case <- function(case_id, fields) {
  .supa_patch("cases", "case_id", case_id, fields)
  invisible(NULL)
}

# ── API FAILURES ───────────────────────────────────────────────────
db_log_failure <- function(case_id, tweet_text, error_msg, attempt=1L) {
  .supa_post("api_failures", data.frame(
    case_id    = case_id,
    tweet_text = tweet_text,
    error_msg  = error_msg,
    attempt    = as.integer(attempt),
    ts         = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    resolved   = 0L,
    stringsAsFactors = FALSE
  ))
  invisible(NULL)
}

db_resolve_failure <- function(case_id) {
  .supa_patch("api_failures", "case_id", case_id, list(resolved=1L))
  invisible(NULL)
}

db_load_failures <- function(unresolved_only=TRUE) {
  params <- if (unresolved_only) list(resolved="eq.0") else list()
  .supa_get("api_failures", params)
}

db_clear_failures <- function() {
  req <- .supa_req("api_failures", "DELETE") |>
    req_url_query(resolved="eq.1")
  req_perform(req)
  invisible(NULL)
}

# ── KEYWORD WEIGHTS ────────────────────────────────────────────────
db_load_weights <- function() {
  d <- .supa_get("keyword_weights")
  if (nrow(d) == 0) return(list())
  setNames(as.list(d$weight), d$keyword)
}

db_save_weights <- function(w) {
  if (length(w) == 0) return(invisible(NULL))
  df <- data.frame(
    keyword = names(w),
    weight  = unlist(w),
    stringsAsFactors = FALSE
  )
  .supa_post("keyword_weights", df, upsert=TRUE)
  invisible(NULL)
}

# ── S13 QUEUE ──────────────────────────────────────────────────────
db_s13_enqueue <- function(case_row, officer) {
  .supa_post("s13_queue", data.frame(
    case_id      = case_row$case_id,
    county       = case_row$county       %||% "",
    platform     = case_row$platform     %||% "",
    tweet_text   = case_row$tweet_text   %||% "",
    ncic_level   = as.integer(case_row$ncic_level   %||% 0L),
    risk_score   = as.integer(case_row$risk_score   %||% 0L),
    target_group = case_row$target_group %||% "None",
    validated_by = officer,
    validated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    status       = "PENDING",
    dci_ref      = NA_character_,
    notes        = NA_character_,
    created_at   = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    updated_at   = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    stringsAsFactors = FALSE
  ))
  invisible(NULL)
}

db_s13_update_status <- function(id, status, dci_ref=NA, notes=NA) {
  fields <- list(
    status     = status,
    updated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
  )
  if (!is.na(dci_ref)) fields$dci_ref <- dci_ref
  if (!is.na(notes))   fields$notes   <- notes
  .supa_patch("s13_queue", "id", id, fields)
  invisible(NULL)
}

db_s13_load <- function() {
  .supa_get("s13_queue", list(order="created_at.desc"))
}

db_s13_backfill <- function() {
  # No-op for Supabase — backfill handled by ingest pipeline
  invisible(NULL)
}

# ── EXAMPLES (FEW-SHOT TRAINING) ───────────────────────────────────
load_examples <- function(limit=20L) {
  .supa_get("examples", list(order="ts.desc"), limit=limit)
}

save_example <- function(tweet, ncic_level, category, reasoning, outcome) {
  .supa_post("examples", data.frame(
    tweet      = tweet,
    ncic_level = as.character(ncic_level),
    category   = category,
    reasoning  = reasoning,
    outcome    = outcome,
    ts         = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    stringsAsFactors = FALSE
  ))
  invisible(NULL)
}

# ── GOLD STANDARD ──────────────────────────────────────────────────
db_add_gold <- function(tweet, true_level, officer, notes="") {
  .supa_post("gold_standard", data.frame(
    tweet_text   = tweet,
    true_level   = as.integer(true_level),
    labelled_by  = officer,
    labelled_at  = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    agreed       = 0L,
    notes        = notes,
    stringsAsFactors = FALSE
  ))
  invisible(NULL)
}

db_load_gold <- function() {
  .supa_get("gold_standard", list(order="labelled_at.desc"))
}

# ── EVAL RUNS ──────────────────────────────────────────────────────
db_load_eval_runs <- function() {
  .supa_get("eval_runs", list(order="run_at.desc"))
}

db_save_eval_run <- function(n_total, n_correct, precision,
                              recall, f1, threshold, run_by) {
  .supa_post("eval_runs", data.frame(
    run_at          = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    n_total         = as.integer(n_total),
    n_correct       = as.integer(n_correct),
    precision_score = as.numeric(precision),
    recall_score    = as.numeric(recall),
    f1_score        = as.numeric(f1),
    threshold       = as.integer(threshold),
    run_by          = run_by,
    stringsAsFactors = FALSE
  ))
  invisible(NULL)
}

# ── OFFICER AGREEMENT ──────────────────────────────────────────────
load_officer_agreement <- function() {
  .supa_get("officer_agreement", list(order="ts.desc"))
}

save_officer_agreement <- function(case_id, officer_a, level_a,
                                    officer_b, level_b) {
  delta   <- abs(as.integer(level_a) - as.integer(level_b))
  flagged <- if (delta >= 2L) 1L else 0L
  .supa_post("officer_agreement", data.frame(
    case_id    = case_id,
    officer_a  = officer_a,
    level_a    = as.integer(level_a),
    officer_b  = officer_b,
    level_b    = as.integer(level_b),
    level_delta= delta,
    flagged    = flagged,
    ts         = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    stringsAsFactors = FALSE
  ))
  invisible(NULL)
}

# ── CONFIDENCE CALIBRATION ─────────────────────────────────────────
load_calibration <- function() {
  .supa_get("confidence_calibration", list(order="conf_bucket.asc"))
}

update_calibration <- function(conf_bucket, correct) {
  existing <- .supa_get("confidence_calibration",
    list(conf_bucket=paste0("eq.", conf_bucket)))
  if (nrow(existing) == 0) {
    .supa_post("confidence_calibration", data.frame(
      conf_bucket = as.integer(conf_bucket),
      n_total     = 1L,
      n_correct   = as.integer(correct),
      accuracy    = as.numeric(correct),
      updated_at  = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
      stringsAsFactors = FALSE
    ))
  } else {
    n_total   <- existing$n_total[1] + 1L
    n_correct <- existing$n_correct[1] + as.integer(correct)
    .supa_patch("confidence_calibration", "conf_bucket", conf_bucket,
      list(n_total=n_total, n_correct=n_correct,
           accuracy=round(n_correct/n_total, 4),
           updated_at=format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")))
  }
  invisible(NULL)
}

# ── KEYWORD RETIREMENTS ────────────────────────────────────────────
fetch_stale_keywords <- function(days=90L) {
  cutoff <- format(Sys.time() - as.difftime(days, units="days"),
                   "%Y-%m-%dT%H:%M:%SZ")
  .supa_get("keyword_bank",
    list(status="eq.approved",
         last_matched_at=paste0("lt.", cutoff)))
}

retire_keyword <- function(kw_id, officer, reason="") {
  resp <- .supa_patch("keyword_bank", "id", kw_id,
    list(status="retired",
         retired_by=officer,
         retired_at=format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")))
  if (resp_status(resp) %in% c(200L, 204L)) {
    .supa_post("keyword_retirements", data.frame(
      keyword    = kw_id,
      retired_by = officer,
      retired_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
      reason     = reason,
      stringsAsFactors = FALSE
    ))
    return(TRUE)
  }
  FALSE
}

# ── KEYWORD CHANGELOG ──────────────────────────────────────────────
load_keyword_changelog <- function(limit=50L) {
  .supa_get("keyword_changelog", list(order="changed_at.desc"), limit=limit)
}

log_keyword_change <- function(keyword, action, old_tier=NA, new_tier=NA,
                                old_status=NA, new_status=NA,
                                changed_by="", reason="") {
  .supa_post("keyword_changelog", data.frame(
    keyword    = keyword,
    action     = action,
    old_tier   = old_tier   %||% NA_integer_,
    new_tier   = new_tier   %||% NA_integer_,
    old_status = old_status %||% NA_character_,
    new_status = new_status %||% NA_character_,
    changed_by = changed_by,
    changed_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    reason     = reason,
    stringsAsFactors = FALSE
  ))
  invisible(NULL)
}

# ── DISAGREEMENTS ──────────────────────────────────────────────────
log_disagreement <- function(officer, case_id, tweet,
                              gpt_level, officer_level) {
  .supa_post("disagreements", data.frame(
    ts            = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    officer       = officer,
    case_id       = case_id,
    tweet         = tweet,
    gpt_level     = as.integer(gpt_level),
    officer_level = as.integer(officer_level),
    stringsAsFactors = FALSE
  ))
  invisible(NULL)
}

# ── GPT CACHE (Supabase-backed) ────────────────────────────────────
# Optional: use Supabase gpt_cache table instead of local RDS
# classify_cache env stays in memory; this syncs on startup
sync_gpt_cache_from_supabase <- function(env) {
  d <- tryCatch(.supa_get("gpt_cache", limit=50000L), error=function(e) data.frame())
  if (nrow(d) == 0) return(invisible(NULL))
  for (i in seq_len(nrow(d)))
    assign(d$cache_key[i],
           tryCatch(jsonlite::fromJSON(d$result_json[i]), error=function(e) NULL),
           envir=env)
  message(sprintf("[cache] %d GPT results loaded from Supabase", nrow(d)))
}

save_gpt_cache_to_supabase <- function(key, result, env) {
  assign(key, result, envir=env)
  tryCatch(
    .supa_post("gpt_cache", data.frame(
      cache_key   = key,
      result_json = jsonlite::toJSON(result, auto_unbox=TRUE),
      created_at  = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
      stringsAsFactors = FALSE
    ), upsert=TRUE),
    error = function(e) NULL
  )
  invisible(NULL)
}

# ── DB POLL (cross-session sync) ───────────────────────────────────
# Replaces SQLite poll — fetches recently updated cases from Supabase
db_poll_recent <- function(since_ts) {
  .supa_get("cases",
    list(updated_at=paste0("gt.", format(since_ts, "%Y-%m-%dT%H:%M:%SZ")),
         order="updated_at.desc"),
    limit=500L)
}

message("[db] Supabase database layer loaded ✅")
