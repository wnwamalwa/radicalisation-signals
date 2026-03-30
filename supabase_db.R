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
    req_headers("Prefer" = "return=minimal") |>
    req_url_query(!!filter_col := paste0("eq.", filter_val)) |>
    req_body_raw(
      jsonlite::toJSON(data, auto_unbox=TRUE, na="null"),
      type="application/json"
    )
  req_perform(req)
}

# ── OFFICERS ──────────────────────────────────────────────────────
db_seed_admin <- function(u, p) {
  existing <- .supa_get("officers", list(username=paste0("eq.", u)))
  if (nrow(existing) > 0) return(invisible(NULL))
  .supa_post("officers", data.frame(
    username      = u,
    password_hash = bcrypt::hashpw(p),
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
  .supa_get("officers", list(active="eq.1", order="username.asc"))
}

# ── KEYWORD WEIGHTS ───────────────────────────────────────────────
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
    case_id     = case_id,
    officer_a   = officer_a,
    level_a     = as.integer(level_a),
    officer_b   = officer_b,
    level_b     = as.integer(level_b),
    level_delta = delta,
    flagged     = flagged,
    ts          = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
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
      list(n_total   = n_total,
           n_correct = n_correct,
           accuracy  = round(n_correct/n_total, 4),
           updated_at= format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")))
  }
  invisible(NULL)
}

# ── KEYWORD RETIREMENTS ────────────────────────────────────────────
fetch_stale_keywords <- function(days=90L) {
  cutoff <- format(Sys.time() - as.difftime(days, units="days"),
                   "%Y-%m-%dT%H:%M:%SZ")
  .supa_get("keyword_bank",
    list(status         = "eq.approved",
         last_matched_at= paste0("lt.", cutoff)))
}

retire_keyword <- function(kw_id, officer, reason="") {
  resp <- .supa_patch("keyword_bank", "id", kw_id,
    list(status     = "retired",
         retired_by = officer,
         retired_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")))
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

load_disagreements <- function(limit=50L) {
  .supa_get("disagreements", list(order="ts.desc"), limit=limit)
}

# ── GPT CACHE (Supabase-backed) ────────────────────────────────────
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
db_poll_recent <- function(since_ts) {
  .supa_get("cases",
    list(updated_at = paste0("gt.", format(since_ts, "%Y-%m-%dT%H:%M:%SZ")),
         order      = "updated_at.desc"),
    limit=500L)
}

# ================================================================
#  VALIDATION EVIDENCE + CONTEXT-AWARE KEYWORD WEIGHTS
#  Learning loop: officer decisions feed back into scoring model
# ================================================================

# ── Save officer decision with keyword evidence and context ───────
db_save_validation_evidence <- function(case_id, officer, action,
                                         keywords, context_type,
                                         tweet_text="", ncic_level=NA,
                                         county="", platform="") {
  .supa_post("validation_evidence", data.frame(
    case_id      = case_id,
    officer      = officer,
    action       = action,
    keywords     = paste(keywords, collapse="|"),
    context_type = context_type,
    tweet_text   = substr(tweet_text %||% "", 1, 500),
    ncic_level   = as.integer(ncic_level %||% NA_integer_),
    county       = county %||% "",
    platform     = platform %||% "",
    created_at   = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    stringsAsFactors = FALSE
  ))
  invisible(NULL)
}

# ── Load recent validation evidence (for live learning feed) ──────
db_load_validation_evidence <- function(limit=20L) {
  .supa_get("validation_evidence",
            list(order="created_at.desc"),
            limit=limit)
}

# ── Load context weights as nested list: keyword -> context -> weight
db_load_context_weights <- function() {
  d <- .supa_get("keyword_context_weights")
  if (nrow(d) == 0) return(list())
  result <- list()
  for (i in seq_len(nrow(d))) {
    kw  <- d$keyword[i]
    ctx <- d$context_type[i]
    if (is.null(result[[kw]])) result[[kw]] <- list()
    result[[kw]][[ctx]] <- as.numeric(d$weight[i])
  }
  result
}

# ── Boost or suppress a keyword's weight for a specific context ───
# confirm/escalate → boost; downgrade/clear → suppress
db_update_context_weight <- function(keyword, context_type, action, officer) {
  keyword    <- tolower(trimws(keyword))
  is_harmful <- context_type %in% c("ethnic_incitement", "political_violence")
  is_safe    <- context_type %in% c("satire", "neutral")
  is_confirm <- action %in% c("CONFIRMED", "ESCALATED")
  is_suppress<- action %in% c("CLEARED", "DOWNGRADED")

  existing <- tryCatch(
    .supa_get("keyword_context_weights",
      list(keyword      = paste0("eq.", keyword),
           context_type = paste0("eq.", context_type))),
    error = function(e) data.frame()
  )

  if (nrow(existing) == 0) {
    base_weight <- dplyr::case_when(
      context_type == "ethnic_incitement"    ~ 60,
      context_type == "political_violence"   ~ 55,
      context_type == "historical_reference" ~ 35,
      context_type == "satire"               ~ 10,
      context_type == "neutral"              ~  5,
      TRUE                                   ~ 30
    )
    .supa_post("keyword_context_weights", data.frame(
      keyword            = keyword,
      context_type       = context_type,
      weight             = as.numeric(base_weight),
      confirmation_count = as.integer(is_confirm),
      suppression_count  = as.integer(is_suppress),
      last_updated       = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
      stringsAsFactors   = FALSE
    ))
  } else {
    old_weight <- as.numeric(existing$weight[1])
    new_weight <- if (is_confirm)
      min(100, old_weight + if (is_harmful) 7 else 3)
    else if (is_suppress)
      max(1,   old_weight - if (is_safe)   10 else 5)
    else
      old_weight

    .supa_patch("keyword_context_weights", "keyword", keyword,
      list(
        weight             = round(new_weight, 1),
        confirmation_count = as.integer(existing$confirmation_count[1]) + as.integer(is_confirm),
        suppression_count  = as.integer(existing$suppression_count[1])  + as.integer(is_suppress),
        last_updated       = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
      )
    )

    log_keyword_change(
      keyword    = keyword,
      action     = if (is_confirm) "CONTEXT_BOOSTED" else "CONTEXT_SUPPRESSED",
      old_status = as.character(round(old_weight)),
      new_status = as.character(round(new_weight)),
      changed_by = officer,
      reason     = paste0("Officer ", action, " in context: ", context_type)
    )
  }
  invisible(NULL)
}

# ── Master function: call this from validation observer in app.R ──
db_process_validation_evidence <- function(case_id, officer, action,
                                            keywords, context_type,
                                            tweet_text, ncic_level,
                                            county, platform) {
  if (length(keywords) == 0 || nchar(context_type %||% "") == 0)
    return(invisible(NULL))

  # 1. Save evidence record
  db_save_validation_evidence(
    case_id=case_id, officer=officer, action=action,
    keywords=keywords, context_type=context_type,
    tweet_text=tweet_text, ncic_level=ncic_level,
    county=county, platform=platform
  )

  # 2. Update context weights for each keyword
  for (kw in keywords)
    tryCatch(
      db_update_context_weight(kw, context_type, action, officer),
      error = function(e)
        message(sprintf("[evidence] weight update failed for '%s': %s", kw, e$message))
    )

  # 3. Add to keyword_bank if confirming harmful content
  is_confirm <- action %in% c("CONFIRMED", "ESCALATED")
  is_harmful <- context_type %in% c("ethnic_incitement", "political_violence")
  is_safe    <- context_type %in% c("satire", "neutral")
  tier       <- if (!is.na(ncic_level) && ncic_level >= 5) 1L else
                if (!is.na(ncic_level) && ncic_level >= 4) 2L else 3L

  for (kw in keywords) {
    kw_clean <- tolower(trimws(kw))
    existing_kw <- tryCatch(
      .supa_get("keyword_bank", list(keyword=paste0("eq.", kw_clean))),
      error = function(e) data.frame()
    )

    if (is_confirm && is_harmful && nrow(existing_kw) == 0) {
      tryCatch(
        .supa_post("keyword_bank", data.frame(
          keyword    = kw_clean,
          tier       = tier,
          category   = context_type,
          language   = "sw_en",
          context    = paste0("Officer-selected: ", context_type),
          added_by   = officer,
          added_at   = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
          status     = "approved",
          confidence = 0.95,
          stringsAsFactors = FALSE
        )),
        error = function(e)
          message(sprintf("[evidence] keyword_bank insert failed for '%s': %s", kw, e$message))
      )
      log_keyword_change(
        keyword    = kw,
        action     = "ADDED",
        new_tier   = tier,
        new_status = "approved",
        changed_by = officer,
        reason     = paste0("Officer-selected evidence: ", context_type)
      )
    }

    # 4. Suppress in safe context — annotate but keep in bank
    if (is_safe && nrow(existing_kw) > 0) {
      tryCatch(
        .supa_patch("keyword_bank", "keyword", kw_clean,
          list(context = paste0(
            existing_kw$context[1] %||% "",
            " | suppressed in: ", context_type
          ))),
        error = function(e) NULL
      )
      log_keyword_change(
        keyword    = kw,
        action     = "CONTEXT_SUPPRESSED",
        changed_by = officer,
        reason     = paste0("Officer cleared/downgraded in context: ", context_type)
      )
    }
  }

  message(sprintf("[evidence] %s: %d keywords processed · context=%s · action=%s",
                  case_id, length(keywords), context_type, action))
  invisible(NULL)
}

# ── Get context-adjusted score for a keyword in a given context ───
db_get_context_score <- function(keyword, context_type) {
  keyword <- tolower(trimws(keyword))
  row <- tryCatch(
    .supa_get("keyword_context_weights",
      list(keyword      = paste0("eq.", keyword),
           context_type = paste0("eq.", context_type))),
    error = function(e) data.frame()
  )
  if (nrow(row) == 0) return(NULL)
  as.numeric(row$weight[1])
}

message("[db] Supabase database layer loaded ✅")
message("[db] Validation evidence layer loaded ✅")
