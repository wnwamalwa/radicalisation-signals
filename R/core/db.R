# ================================================================
#  R/db.R — SQLite Database Layer + v6 Helper Functions
#  Radicalisation Signals · IEA Kenya NIRU
# ================================================================

# ── SQLite DATABASE LAYER ─────────────────────────────────────────
if (!dir.exists(CACHE_DIR)) dir.create(CACHE_DIR, recursive=TRUE)

db_connect <- function() {
  con <- dbConnect(RSQLite::SQLite(), DB_FILE)
  dbExecute(con, "PRAGMA journal_mode=WAL;")   # concurrent reads without locking
  con
}

db_init <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS cases (",
    "case_id TEXT PRIMARY KEY, county TEXT, sub_location TEXT,",
    "src_lat REAL, src_lng REAL, target_county TEXT, tgt_lat REAL, tgt_lng REAL,",
    "target_group TEXT, platform TEXT, handle TEXT, tweet_text TEXT, language TEXT,",
    "ncic_level INTEGER, ncic_label TEXT, section_13 INTEGER, confidence TEXT,",
    "conf_num INTEGER, conf_band TEXT, category TEXT,",
    "validated_by TEXT, validated_at TEXT, action_taken TEXT,",
    "officer_ncic_override INTEGER, risk_score INTEGER, risk_level TEXT,",
    "risk_formula TEXT, kw_score INTEGER, network_score INTEGER,",
    "signals TEXT, trend_data TEXT, timestamp TEXT, timestamp_chr TEXT,",
    "notes TEXT DEFAULT '')"))
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS keyword_weights (",
    "keyword TEXT PRIMARY KEY, weight REAL)"))
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS examples (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, tweet TEXT, ncic_level TEXT,",
    "category TEXT, reasoning TEXT, outcome TEXT, ts TEXT)"))
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS disagreements (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, officer TEXT,",
    "case_id TEXT, tweet TEXT, gpt_level INTEGER, officer_level INTEGER)"))
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS api_failures (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ",
    "case_id TEXT, tweet_text TEXT, error_msg TEXT, ",
    "attempt INTEGER DEFAULT 1, ts TEXT, resolved INTEGER DEFAULT 0)"))
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS officers (",
    "username TEXT PRIMARY KEY, ",
    "password_hash TEXT NOT NULL, ",
    "role TEXT DEFAULT 'officer', ",
    "active INTEGER DEFAULT 1, ",
    "created_at TEXT)"))
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS s13_queue (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ",
    "case_id TEXT NOT NULL, ",
    "county TEXT, platform TEXT, tweet_text TEXT, ",
    "ncic_level INTEGER, risk_score INTEGER, ",
    "target_group TEXT, validated_by TEXT, validated_at TEXT, ",
    "status TEXT DEFAULT 'PENDING', ",   # PENDING | FILED | DCI_ALERTED | RESOLVED
    "dci_ref TEXT, ",                    # DCI reference number when filed
    "notes TEXT, ",
    "created_at TEXT, updated_at TEXT)"))
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS audit_log (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ",
    "ts TEXT NOT NULL, ",
    "officer TEXT NOT NULL, ",
    "officer_name TEXT, ",
    "action TEXT NOT NULL, ",
    "case_id TEXT, ",
    "detail TEXT, ",
    "session_id TEXT)"))

  # ── v6 tables ────────────────────────────────────────────────
  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS gold_standard (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ",
    "tweet_text TEXT NOT NULL, ",
    "true_level INTEGER NOT NULL, ",
    "labelled_by TEXT NOT NULL, ",
    "labelled_at TEXT, ",
    "second_label INTEGER, ",
    "second_labelled_by TEXT, ",
    "agreed INTEGER DEFAULT 0, ",
    "notes TEXT DEFAULT '')"))

  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS eval_runs (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ",
    "run_at TEXT NOT NULL, ",
    "n_total INTEGER, ",
    "n_correct INTEGER, ",
    "precision_score REAL, ",
    "recall_score REAL, ",
    "f1_score REAL, ",
    "threshold INTEGER, ",
    "run_by TEXT)"))

  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS officer_agreement (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ",
    "case_id TEXT NOT NULL, ",
    "officer_a TEXT, level_a INTEGER, ",
    "officer_b TEXT, level_b INTEGER, ",
    "level_delta INTEGER, ",
    "flagged INTEGER DEFAULT 0, ",
    "adjudicated_by TEXT, ",
    "final_level INTEGER, ",
    "ts TEXT)"))

  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS confidence_calibration (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ",
    "conf_bucket INTEGER, ",
    "n_total INTEGER DEFAULT 0, ",
    "n_correct INTEGER DEFAULT 0, ",
    "accuracy REAL, ",
    "updated_at TEXT)"))

  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS keyword_retirements (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ",
    "keyword TEXT NOT NULL, ",
    "tier INTEGER, category TEXT, language TEXT, ",
    "retired_by TEXT, retired_at TEXT, ",
    "reason TEXT, times_matched INTEGER DEFAULT 0)"))

  dbExecute(con, paste0(
    "CREATE TABLE IF NOT EXISTS keyword_changelog (",
    "id INTEGER PRIMARY KEY AUTOINCREMENT, ",
    "keyword TEXT NOT NULL, ",
    "action TEXT NOT NULL, ",
    "old_tier INTEGER, new_tier INTEGER, ",
    "old_status TEXT, new_status TEXT, ",
    "changed_by TEXT, changed_at TEXT, ",
    "reason TEXT DEFAULT '')"))

  message("[db] SQLite initialised at ", DB_FILE)
}

# ── Officer auth helpers ─────────────────────────────────────────
# Seed a default admin on first run from .Renviron:
#   ADMIN_USERNAME=admin
#   ADMIN_PASSWORD=<your-strong-password>
# Never hardcode credentials in app.R.
db_seed_admin <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  existing <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM officers")$n
  if (existing > 0) return(invisible(NULL))   # already seeded
  
  u <- Sys.getenv("ADMIN_USERNAME")
  p <- Sys.getenv("ADMIN_PASSWORD")
  if (nchar(u) == 0 || nchar(p) == 0) {
    warning("[auth] ADMIN_USERNAME / ADMIN_PASSWORD not set in .Renviron — no officers seeded. ",
            "Run setup_officers.R to add officers before deploying.")
    return(invisible(NULL))
  }
  hashed <- bcrypt::hashpw(p)
  dbExecute(con,
            "INSERT INTO officers (username, password_hash, role, active, created_at) VALUES (?,?,?,1,?)",
            list(u, hashed, "admin", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
  message(sprintf("[auth] Default admin '%s' seeded from .Renviron", u))
}

db_check_password <- function(username, password) {
  # Returns list(ok, role) — timing-safe via bcrypt::checkpw
  con <- db_connect(); on.exit(dbDisconnect(con))
  row <- dbGetQuery(con,
                    "SELECT password_hash, role, active FROM officers WHERE username = ?",
                    list(trimws(tolower(username))))
  if (nrow(row) == 0 || row$active[1] != 1L)
    return(list(ok=FALSE, role=NA_character_))
  ok <- tryCatch(bcrypt::checkpw(password, row$password_hash[1]),
                 error = function(e) FALSE)
  list(ok=ok, role=if(ok) row$role[1] else NA_character_)
}

# ── Password strength checker (mirrors setup_officers.R rules) ───
check_pw_strength <- function(password, username) {
  errs <- c()
  if (nchar(password) < 10)
    errs <- c(errs, "At least 10 characters required")
  if (!grepl("[A-Z]", password))
    errs <- c(errs, "At least one uppercase letter required")
  if (!grepl("[a-z]", password))
    errs <- c(errs, "At least one lowercase letter required")
  if (!grepl("[0-9]", password))
    errs <- c(errs, "At least one digit required")
  if (!grepl("[^A-Za-z0-9]", password))
    errs <- c(errs, "At least one special character required (e.g. ! @ # $)")
  if (tolower(password) == tolower(username))
    errs <- c(errs, "Password must not match the username")
  if (grepl("password|ncic|admin|officer|intel|kenya", tolower(password)))
    errs <- c(errs, "Password contains a common word — choose something unique")
  errs
}

db_add_officer <- function(username, password, role="officer") {
  errs <- check_pw_strength(password, username)
  if (length(errs) > 0)
    stop(paste("Password rejected:", paste(errs, collapse="; ")))
  con <- db_connect(); on.exit(dbDisconnect(con))
  hashed <- bcrypt::hashpw(password)
  dbExecute(con,
            "INSERT OR REPLACE INTO officers (username, password_hash, role, active, created_at) VALUES (?,?,?,1,?)",
            list(trimws(tolower(username)), hashed, role, format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
  message(sprintf("[auth] Officer '%s' (role: %s) added/updated", username, role))
}

db_deactivate_officer <- function(username) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con, "UPDATE officers SET active=0 WHERE username=?",
            list(trimws(tolower(username))))
  message(sprintf("[auth] Officer '%s' deactivated", username))
}

# ── Audit log helpers ────────────────────────────────────────────
audit <- function(officer, officer_name, action, case_id=NA, detail=NA, session_id=NA) {
  tryCatch({
    con <- db_connect(); on.exit(dbDisconnect(con))
    dbExecute(con,
              "INSERT INTO audit_log (ts, officer, officer_name, action, case_id, detail, session_id)
       VALUES (?, ?, ?, ?, ?, ?, ?)",
              list(format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
                   as.character(officer %||% "unknown"),
                   as.character(officer_name %||% ""),
                   as.character(action),
                   if (is.na(case_id))  NA_character_ else as.character(case_id),
                   if (is.na(detail))   NA_character_ else as.character(detail),
                   if (is.na(session_id)) NA_character_ else as.character(session_id)))
  }, error = function(e) message("[audit] log failed: ", e$message))
}

db_load_audit <- function(limit=500) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  tryCatch(
    dbGetQuery(con, sprintf(
      "SELECT id, ts, officer, officer_name, action, case_id, detail
       FROM audit_log ORDER BY id DESC LIMIT %d", as.integer(limit))),
    error = function(e) data.frame()
  )
}

db_load_officers <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  tryCatch(
    dbGetQuery(con,
               "SELECT username, role, active, created_at FROM officers ORDER BY created_at"),
    error = function(e) data.frame()
  )
}

# ── S13 Escalation Queue helpers ─────────────────────────────────
db_s13_enqueue <- function(case_row, officer) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  existing <- dbGetQuery(con,
                         "SELECT id FROM s13_queue WHERE case_id=?", list(case_row$case_id))
  if (nrow(existing) > 0) return(invisible(FALSE))  # already queued
  now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  dbExecute(con,
            "INSERT INTO s13_queue
     (case_id,county,platform,tweet_text,ncic_level,risk_score,
      target_group,validated_by,validated_at,status,created_at,updated_at)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            list(case_row$case_id, case_row$county, case_row$platform,
                 substr(case_row$tweet_text,1,200),
                 case_row$ncic_level, case_row$risk_score,
                 case_row$target_group %||% "", officer,
                 format(Sys.time(),"%Y-%m-%d %H:%M"),
                 "PENDING", now, now))
  invisible(TRUE)
}

db_s13_update_status <- function(id, status, dci_ref=NA, notes=NA) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con,
            "UPDATE s13_queue SET status=?, dci_ref=?, notes=?, updated_at=? WHERE id=?",
            list(status,
                 if (is.na(dci_ref)) NA_character_ else dci_ref,
                 if (is.na(notes))   NA_character_ else notes,
                 format(Sys.time(), "%Y-%m-%d %H:%M:%S"), as.integer(id)))
}

db_s13_load <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  tryCatch(
    dbGetQuery(con,
               "SELECT * FROM s13_queue ORDER BY
       CASE status WHEN 'PENDING' THEN 0 WHEN 'FILED' THEN 1
                   WHEN 'DCI_ALERTED' THEN 2 ELSE 3 END,
       ncic_level DESC, risk_score DESC"),
    error = function(e) data.frame()
  )
}

db_init()
db_seed_admin()

# ── S13 queue backfill ───────────────────────────────────────────
# On startup, any validated L4/L5 case not already in s13_queue
# is automatically enqueued so the queue is never empty after deploy.
db_s13_backfill <- function() {
  tryCatch({
    con <- db_connect(); on.exit(dbDisconnect(con))
    # Get all validated high-severity cases not yet in queue
    existing <- dbGetQuery(con, "SELECT case_id FROM s13_queue")$case_id
    cases    <- dbGetQuery(con,
      "SELECT * FROM cases WHERE ncic_level >= 4 AND validated_by IS NOT NULL")
    if (nrow(cases) == 0) return(invisible(NULL))
    new_cases <- cases[!cases$case_id %in% existing, ]
    if (nrow(new_cases) == 0) return(invisible(NULL))
    for (i in seq_len(nrow(new_cases))) {
      row <- new_cases[i, ]
      dbExecute(con,
        paste0("INSERT OR IGNORE INTO s13_queue ",
               "(case_id,county,platform,tweet_text,ncic_level,risk_score,",
               "target_group,validated_by,validated_at,status,created_at,updated_at) ",
               "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"),
        list(row$case_id,
             row$county    %||% "Unknown",
             row$platform  %||% "Unknown",
             substr(row$tweet_text %||% "", 1, 500),
             as.integer(row$ncic_level),
             as.integer(row$risk_score %||% 0L),
             row$target_group %||% "",
             row$validated_by %||% "system",
             row$validated_at %||% format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
             "PENDING",
             format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
             format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
    }
    n <- nrow(new_cases)
    if (n > 0) message(sprintf("[startup] S13 backfill: %d case(s) added to queue", n))
  }, error = function(e) message("[startup] S13 backfill error: ", e$message))
}
db_s13_backfill()

# ── Cases ────────────────────────────────────────────────────────
db_load_cases <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  d <- dbReadTable(con, "cases")
  if (nrow(d) == 0) return(NULL)
  d$timestamp  <- as.POSIXct(d$timestamp, tz="Africa/Nairobi")
  d$section_13 <- as.logical(d$section_13)
  d
}

db_save_cases <- function(df) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbWithTransaction(con, {
    dbExecute(con, "DELETE FROM cases")
    dw <- df
    dw$timestamp  <- format(df$timestamp, "%Y-%m-%d %H:%M:%S")
    dw$section_13 <- as.integer(df$section_13)
    dbWriteTable(con, "cases", dw, append=TRUE)
  })
  invisible(TRUE)
}

db_update_case <- function(case_id, fields) {
  # Atomic update of specific fields for one case — used during validation
  con <- db_connect(); on.exit(dbDisconnect(con))
  cols <- names(fields)
  set_sql <- paste(sprintf('"%s" = :%s', cols, cols), collapse=", ")
  sql <- sprintf('UPDATE cases SET %s WHERE case_id = :dot_cid', set_sql)
  dbExecute(con, sql, params=c(fields, list(dot_cid=case_id)))
}

# ── API Failure Queue ────────────────────────────────────────────
db_log_failure <- function(case_id, tweet_text, error_msg, attempt=1L) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con,
            "INSERT INTO api_failures (case_id,tweet_text,error_msg,attempt,ts,resolved) VALUES (:case_id,:tweet,:err,:attempt,:ts,0)",
            params=list(case_id=case_id, tweet=substr(tweet_text,1,100),
                        err=substr(error_msg,1,200), attempt=as.integer(attempt),
                        ts=format(Sys.time(),"%Y-%m-%d %H:%M:%S")))
}

db_resolve_failure <- function(case_id) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con, "UPDATE api_failures SET resolved=1 WHERE case_id=:cid AND resolved=0",
            params=list(cid=case_id))
}

db_load_failures <- function(unresolved_only=TRUE) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  sql <- if (unresolved_only)
    "SELECT * FROM api_failures WHERE resolved=0 ORDER BY ts DESC"
  else
    "SELECT * FROM api_failures ORDER BY ts DESC LIMIT 100"
  tryCatch(dbGetQuery(con, sql),
           error=function(e) data.frame())
}

db_clear_failures <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con, "UPDATE api_failures SET resolved=1")
}

# ── Keyword weights ──────────────────────────────────────────────
db_load_weights <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  d <- dbReadTable(con, "keyword_weights")
  if (nrow(d) == 0) return(NULL)
  as.list(setNames(d$weight, d$keyword))
}

db_save_weights <- function(w) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbWithTransaction(con, {
    dbExecute(con, "DELETE FROM keyword_weights")
    dbWriteTable(con, "keyword_weights",
                 data.frame(keyword=names(w), weight=unlist(w),
                            stringsAsFactors=FALSE), append=TRUE)
  })
  invisible(TRUE)
}

# ── Examples (few-shot bank) ─────────────────────────────────────
load_examples <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  d <- dbReadTable(con, "examples")
  if (nrow(d) == 0) return(list())
  lapply(seq_len(nrow(d)), function(i)
    list(tweet=d$tweet[i], ncic_level=d$ncic_level[i],
         category=d$category[i], reasoning=d$reasoning[i],
         outcome=d$outcome[i], ts=as.POSIXct(d$ts[i])))
}

save_examples <- function(ex) invisible(NULL)  # legacy stub

add_example <- function(tweet, ncic_level, category, reasoning, outcome) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con,
            "INSERT INTO examples (tweet,ncic_level,category,reasoning,outcome,ts) VALUES (:tweet,:ncic_level,:category,:reasoning,:outcome,:ts)",
            params=list(tweet=tweet, ncic_level=as.character(ncic_level),
                        category=category %||% "", reasoning=reasoning %||% "",
                        outcome=outcome, ts=format(Sys.time(),"%Y-%m-%d %H:%M:%S")))
}

# ── Disagreements ────────────────────────────────────────────────
load_disagreements <- function() {
  con <- db_connect(); on.exit(dbDisconnect(con))
  d <- dbReadTable(con, "disagreements")
  if (nrow(d) == 0)
    return(data.frame(ts=character(), officer=character(), case_id=character(),
                      tweet=character(), gpt_level=integer(), officer_level=integer(),
                      stringsAsFactors=FALSE))
  d[, c("ts","officer","case_id","tweet","gpt_level","officer_level")]
}

log_disagreement <- function(officer, case_id, tweet, gpt_lvl, off_lvl) {
  con <- db_connect(); on.exit(dbDisconnect(con))
  dbExecute(con,
            "INSERT INTO disagreements (ts,officer,case_id,tweet,gpt_level,officer_level) VALUES (:ts,:officer,:case_id,:tweet,:gpt_level,:officer_level)",
            params=list(ts=format(Sys.time(),"%Y-%m-%d %H:%M"), officer=officer,
                        case_id=case_id, tweet=substr(tweet,1,80),
                        gpt_level=as.integer(gpt_lvl), officer_level=as.integer(off_lvl)))
}

# ── ADAPTIVE KEYWORD WEIGHTS ─────────────────────────────────────
DEFAULT_KEYWORD_WEIGHTS <- list(
  "kabila"=30,"waende"=35,"hawastahili"=30,"migrants"=25,
  "extremists"=28,"tutawaonyesha"=40,"wezi"=32,"nguvu"=28,
  "never be trusted"=35,"go back"=30,"cockroach"=50,
  "vermin"=50,"funga"=28,"waondoke"=38,"sisi na wao"=32,
  "damu"=30,"piga"=35,"chinja"=45,"angamiza"=48
)

kw_weights_global <- db_load_weights() %||% DEFAULT_KEYWORD_WEIGHTS

save_kw_weights <- function(w) {
  db_save_weights(w)
}

