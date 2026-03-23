# ================================================================
#  migrate_to_supabase.R — SQLite → Supabase Migration
#  Radicalisation Signals · IEA Kenya NIRU AI Hackathon
#
#  Migrates:
#    1. cases            (1,347 rows)
#    2. keyword_weights
#    3. examples
#    4. disagreements
#    5. officers
#
#  Safe to run multiple times — upserts on primary key.
#  app.R continues working on SQLite during migration.
#
#  Usage: source("migrate_to_supabase.R")
# ================================================================

readRenviron("secrets/.Renviron")
library(DBI)
library(RSQLite)
library(httr2)
library(jsonlite)
library(digest)

source("supabase_helpers.R")

DB_FILE <- "cache/ews.sqlite"

if (!file.exists(DB_FILE))
  stop("SQLite DB not found at: ", DB_FILE,
       "\nMake sure you are running from the app folder (folder 5)")

message("Opening SQLite connection...")
con <- dbConnect(RSQLite::SQLite(), DB_FILE, flags=RSQLite::SQLITE_RO)
message(sprintf("Connected — tables: %s",
                paste(dbListTables(con), collapse=", ")))

message("\n", strrep("=", 60))
message("SQLITE → SUPABASE MIGRATION")
message(format(Sys.time(), "%d %b %Y %H:%M EAT"))
message(strrep("=", 60))

# ── HELPERS ───────────────────────────────────────────────────────

# Convert data frame to list of rows with explicit NULLs
# Supabase requires all rows to have identical keys — NAs must be NULL not absent
df_to_rows <- function(df) {
  lapply(seq_len(nrow(df)), function(i) {
    row <- as.list(df[i, ])
    lapply(row, function(v) if (length(v) == 1 && is.na(v)) NULL else v)
  })
}

# Insert rows list to Supabase — uses raw JSON to preserve all keys
supa_insert_rows <- function(table, rows_list) {
  request(paste0(SUPA_URL, "/rest/v1/", table)) |>
    req_headers(
      "apikey"        = SUPA_KEY,
      "Authorization" = paste("Bearer", SUPA_KEY),
      "Content-Type"  = "application/json",
      "Prefer"        = "resolution=merge-duplicates,return=minimal"
    ) |>
    req_body_raw(
      jsonlite::toJSON(rows_list, auto_unbox=TRUE, null="null"),
      type = "application/json"
    ) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
}

# Migrate a table in batches — checks HTTP status per batch
migrate_table <- function(label, df, batch_size = 100L) {
  n <- nrow(df)
  if (n == 0) {
    message(sprintf("  [%s] empty — skipping", label))
    return(0L)
  }
  message(sprintf("  [%s] migrating %d rows...", label, n))
  inserted <- 0L
  for (i in seq(1, n, by = batch_size)) {
    batch     <- df[i:min(i + batch_size - 1, n), ]
    rows_list <- df_to_rows(batch)
    resp      <- tryCatch(
      supa_insert_rows(label, rows_list),
      error = function(e) { message("    ❌ error: ", e$message); NULL }
    )
    if (!is.null(resp) && resp_status(resp) %in% c(200L, 201L, 204L)) {
      inserted <- inserted + nrow(batch)
    } else if (!is.null(resp)) {
      body <- tryCatch(resp_body_string(resp), error=function(e) "")
      message(sprintf("    ❌ batch %d status %d: %s",
                      ceiling(i/batch_size), resp_status(resp),
                      substr(body, 1, 120)))
    }
  }
  message(sprintf("  [%s] ✅ %d/%d rows migrated", label, inserted, n))
  inserted
}

# ── 1. CASES ──────────────────────────────────────────────────────
message("\n── 1. Cases")
sqlite_cases <- tryCatch(
  dbReadTable(con, "cases"),
  error = function(e) { message("  ❌ could not read cases: ", e$message); data.frame() }
)

if (nrow(sqlite_cases) > 0) {
  # Rename tweet_text → text for Supabase schema
  names(sqlite_cases)[names(sqlite_cases) == "tweet_text"] <- "text"

  # Add source field
  sqlite_cases$source   <- "synthetic"
  sqlite_cases$platform <- sqlite_cases$platform %||% "Unknown"

  # Format timestamp
  sqlite_cases$timestamp <- tryCatch(
    format(as.POSIXct(sqlite_cases$timestamp, tz="Africa/Nairobi"),
           "%Y-%m-%dT%H:%M:%SZ"),
    error = function(e) format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
  )

  # Coerce types
  sqlite_cases$ncic_level   <- as.integer(sqlite_cases$ncic_level   %||% 0)
  sqlite_cases$risk_score   <- as.integer(sqlite_cases$risk_score   %||% 0)
  sqlite_cases$conf_num     <- as.integer(sqlite_cases$conf_num     %||% 0)
  sqlite_cases$section_13   <- as.integer(sqlite_cases$section_13   %||% 0)
  sqlite_cases$kw_score     <- as.integer(sqlite_cases$kw_score     %||% 0)
  sqlite_cases$network_score <- as.integer(sqlite_cases$network_score %||% 20)

  # Drop SQLite-only columns
  drop_cols <- c("timestamp_chr")
  sqlite_cases <- sqlite_cases[, !names(sqlite_cases) %in% drop_cols, drop=FALSE]

  n_cases <- migrate_table("cases", sqlite_cases, batch_size=100L)
} else {
  message("  [cases] no data found")
  n_cases <- 0L
}

# ── 2. KEYWORD WEIGHTS ────────────────────────────────────────────
message("\n── 2. Keyword weights")
sqlite_weights <- tryCatch(
  dbReadTable(con, "keyword_weights"),
  error = function(e) { message("  ❌ ", e$message); data.frame() }
)

n_weights <- 0L
if (nrow(sqlite_weights) > 0) {
  # SQLite has keyword/weight columns; Supabase uses kw_key/kw_value
  names(sqlite_weights)[names(sqlite_weights) == "keyword"] <- "kw_key"
  names(sqlite_weights)[names(sqlite_weights) == "weight"]  <- "kw_value"
  sqlite_weights$updated_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")
  sqlite_weights$updated_by <- "migration"

  n_weights <- migrate_table("keyword_weights", sqlite_weights, batch_size=50L)
}

# ── 3. EXAMPLES ───────────────────────────────────────────────────
message("\n── 3. Examples (few-shot bank)")
sqlite_examples <- tryCatch(
  dbReadTable(con, "examples"),
  error = function(e) { message("  ❌ ", e$message); data.frame() }
)

n_examples <- 0L
if (nrow(sqlite_examples) > 0) {
  # SQLite has 'tweet' column; Supabase uses 'text'
  names(sqlite_examples)[names(sqlite_examples) == "tweet"] <- "text"
  sqlite_examples$source <- "sqlite_migration"
  sqlite_examples$ts     <- format(
    as.POSIXct(sqlite_examples$ts %||% Sys.time()),
    "%Y-%m-%dT%H:%M:%SZ"
  )
  # Drop id — Supabase auto-generates
  sqlite_examples$id <- NULL

  n_examples <- migrate_table("examples", sqlite_examples, batch_size=50L)
}

# ── 4. DISAGREEMENTS ──────────────────────────────────────────────
message("\n── 4. Disagreements")
sqlite_dis <- tryCatch(
  dbReadTable(con, "disagreements"),
  error = function(e) { message("  ❌ ", e$message); data.frame() }
)

n_dis <- 0L
if (nrow(sqlite_dis) > 0) {
  # SQLite has 'tweet'; Supabase uses 'text'
  names(sqlite_dis)[names(sqlite_dis) == "tweet"] <- "text"
  sqlite_dis$ts <- format(
    as.POSIXct(sqlite_dis$ts %||% Sys.time()),
    "%Y-%m-%dT%H:%M:%SZ"
  )
  sqlite_dis$id <- NULL

  n_dis <- migrate_table("disagreements", sqlite_dis, batch_size=50L)
}

# ── 5. OFFICERS ───────────────────────────────────────────────────
message("\n── 5. Officers")
sqlite_officers <- tryCatch(
  dbGetQuery(con, "SELECT * FROM officers"),
  error = function(e) { message("  ❌ ", e$message); data.frame() }
)

n_officers <- 0L
if (nrow(sqlite_officers) > 0) {
  # Map SQLite columns to Supabase schema
  df_off <- data.frame(
    username      = sqlite_officers$username,
    password_hash = sqlite_officers$password_hash,
    role          = sqlite_officers$role %||% "officer",
    full_name     = sqlite_officers$full_name %||% sqlite_officers$username,
    email         = sqlite_officers$email     %||% NA_character_,
    status        = ifelse(sqlite_officers$active == 1, "ACTIVE", "INACTIVE"),
    created_at    = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    stringsAsFactors = FALSE
  )

  n_officers <- migrate_table("officers", df_off, batch_size=20L)
}

# ── VERIFY ────────────────────────────────────────────────────────
message("\n", strrep("=", 60))
message("MIGRATION COMPLETE — VERIFICATION")
message(strrep("=", 60))

counts <- list(
  cases            = supa_count("cases"),
  keyword_weights  = supa_count("keyword_weights"),
  examples         = supa_count("examples"),
  disagreements    = supa_count("disagreements"),
  officers         = supa_count("officers")
)

sqlite_counts <- list(
  cases            = tryCatch(dbGetQuery(con,"SELECT COUNT(*) n FROM cases")$n,           error=function(e) 0),
  keyword_weights  = tryCatch(dbGetQuery(con,"SELECT COUNT(*) n FROM keyword_weights")$n, error=function(e) 0),
  examples         = tryCatch(dbGetQuery(con,"SELECT COUNT(*) n FROM examples")$n,        error=function(e) 0),
  disagreements    = tryCatch(dbGetQuery(con,"SELECT COUNT(*) n FROM disagreements")$n,   error=function(e) 0),
  officers         = tryCatch(dbGetQuery(con,"SELECT COUNT(*) n FROM officers")$n,        error=function(e) 0)
)

message(sprintf("\n%-20s %10s %10s %8s",
                "Table", "SQLite", "Supabase", "Status"))
message(strrep("-", 52))
for (tbl in names(counts)) {
  sq  <- sqlite_counts[[tbl]]
  sp  <- counts[[tbl]]
  ok  <- sp >= sq
  message(sprintf("%-20s %10d %10d %8s",
                  tbl, sq, sp, if (ok) "✅" else "⚠"))
}

message("\n", strrep("=", 60))
message("Migration done. app.R still runs on SQLite.")
message("Next step: update app.R to use supabase_helpers.R")
message(strrep("=", 60))

dbDisconnect(con)
message("SQLite connection closed.")
