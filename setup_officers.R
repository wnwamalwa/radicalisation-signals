# ================================================================
#  setup_officers.R — Officer credential management
#  Run this script from the app directory, NOT inside the Shiny app.
#
#  Usage:
#    Rscript setup_officers.R add    <username> <password> [role]
#    Rscript setup_officers.R remove <username>
#    Rscript setup_officers.R list
#    Rscript setup_officers.R reset  <username> <new_password>
#
#  Examples:
#    Rscript setup_officers.R add    omwangi  Str0ng!Pass officer
#    Rscript setup_officers.R add    admin    Adm!n2025   admin
#    Rscript setup_officers.R remove omwangi
#    Rscript setup_officers.R list
#    Rscript setup_officers.R reset  omwangi  NewPass!123
#
#  Roles:
#    officer  — can validate cases (default)
#    admin    — can validate + manage officer accounts (future)
#
#  SECURITY NOTES:
#    - Passwords are stored as bcrypt hashes (cost=12) — never in plain text
#    - Never commit this script's output or the SQLite DB to version control
#    - Run password resets immediately if credentials are suspected compromised
# ================================================================

library(DBI)
library(RSQLite)
library(bcrypt)

DB_FILE   <- file.path("cache", "ews.sqlite")
BCRYPT_COST <- 12L   # increase to 13-14 for higher security (slower)

# ── Password strength checker ─────────────────────────────────────
check_password_strength <- function(password, username) {
  errors <- c()
  if (nchar(password) < 10)
    errors <- c(errors, "At least 10 characters required.")
  if (!grepl("[A-Z]", password))
    errors <- c(errors, "At least one uppercase letter required.")
  if (!grepl("[a-z]", password))
    errors <- c(errors, "At least one lowercase letter required.")
  if (!grepl("[0-9]", password))
    errors <- c(errors, "At least one digit required.")
  if (!grepl("[^A-Za-z0-9]", password))
    errors <- c(errors, "At least one special character required (e.g. ! @ # $ %).")
  if (tolower(password) == tolower(username))
    errors <- c(errors, "Password must not match the username.")
  if (grepl("password|ncic|admin|officer|intel|kenya|2024|2025|2026", tolower(password)))
    errors <- c(errors, "Password must not contain common words (password, ncic, admin, etc.).")
  errors
}

if (!file.exists(DB_FILE))
  stop("Database not found at: ", DB_FILE,
       "\nRun the Shiny app at least once first to initialise the DB.")

con <- local({ c <- dbConnect(SQLite(), DB_FILE); invisible(c); c })
on.exit(dbDisconnect(con))

# Ensure officers table exists (in case running before app first launch)
dbExecute(con, paste0(
  "CREATE TABLE IF NOT EXISTS officers (",
  "username TEXT PRIMARY KEY, ",
  "password_hash TEXT NOT NULL, ",
  "role TEXT DEFAULT 'officer', ",
  "active INTEGER DEFAULT 1, ",
  "created_at TEXT)"))

args <- commandArgs(trailingOnly = TRUE)

if (length(args) == 0) {
  cat("Usage:\n")
  cat("  Rscript setup_officers.R add    <username> <password> [role]\n")
  cat("  Rscript setup_officers.R remove <username>\n")
  cat("  Rscript setup_officers.R list\n")
  cat("  Rscript setup_officers.R reset  <username> <new_password>\n")
  quit(status=0)
}

cmd <- tolower(args[1])

# ── LIST ─────────────────────────────────────────────────────────
if (cmd == "list") {
  rows <- dbGetQuery(con,
                     "SELECT username, role, active, created_at FROM officers ORDER BY created_at")
  if (nrow(rows) == 0) {
    cat("No officers registered.\n")
  } else {
    rows$status <- ifelse(rows$active == 1, "ACTIVE", "INACTIVE")
    rows$active <- NULL
    # Print as a clean table
    cat(sprintf("\n%-20s %-10s %-10s %s\n", "USERNAME", "ROLE", "STATUS", "CREATED AT"))
    cat(strrep("-", 60), "\n")
    for (i in seq_len(nrow(rows))) {
      cat(sprintf("%-20s %-10s %-10s %s\n",
                  rows$username[i], rows$role[i],
                  rows$status[i],  rows$created_at[i]))
    }
    cat(sprintf("\n%d officer(s) total.\n", nrow(rows)))
  }
  quit(status=0)
}

# ── ADD ──────────────────────────────────────────────────────────
if (cmd == "add") {
  if (length(args) < 3)
    stop("Usage: Rscript setup_officers.R add <username> <password> [role]")
  username <- trimws(tolower(args[2]))
  password <- args[3]
  role     <- if (length(args) >= 4) tolower(args[4]) else "officer"
  
  if (!role %in% c("officer", "admin"))
    stop("Role must be 'officer' or 'admin'.")
  
  errs <- check_password_strength(password, username)
  if (length(errs) > 0) {
    cat("❌ Password rejected:\n")
    for (e in errs) cat("  •", e, "\n")
    cat("\nPassword requirements:\n")
    cat("  • 10+ characters\n  • Upper + lowercase\n  • At least one digit\n")
    cat("  • At least one special character\n  • Not the same as username\n")
    cat("  • No common words (password, admin, ncic, etc.)\n")
    stop("Password does not meet complexity requirements.")
  }
  
  existing <- dbGetQuery(con, "SELECT username FROM officers WHERE username=?", list(username))
  if (nrow(existing) > 0)
    stop(sprintf("Username '%s' already exists. Use 'reset' to change the password.", username))
  
  hash <- hashpw(password, gensalt(BCRYPT_COST))
  dbExecute(con,
            "INSERT INTO officers (username, password_hash, role, active, created_at) VALUES (?,?,?,1,?)",
            list(username, hash, role, format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
  cat(sprintf("✅ Officer '%s' (role: %s) added successfully.\n", username, role))
  quit(status=0)
}

# ── RESET PASSWORD ────────────────────────────────────────────────
if (cmd == "reset") {
  if (length(args) < 3)
    stop("Usage: Rscript setup_officers.R reset <username> <new_password>")
  username <- trimws(tolower(args[2]))
  password <- args[3]
  
  if (nchar(password) < 1)
    stop("Password cannot be empty.")
  
  errs <- check_password_strength(password, username)
  if (length(errs) > 0) {
    cat("❌ Password rejected:\n")
    for (e in errs) cat("  •", e, "\n")
    stop("Password does not meet complexity requirements.")
  }
  
  existing <- dbGetQuery(con, "SELECT username FROM officers WHERE username=?", list(username))
  if (nrow(existing) == 0)
    stop(sprintf("Username '%s' not found.", username))
  
  hash <- hashpw(password, gensalt(BCRYPT_COST))
  dbExecute(con, "UPDATE officers SET password_hash=? WHERE username=?", list(hash, username))
  cat(sprintf("✅ Password reset for '%s'.\n", username))
  quit(status=0)
}

# ── REMOVE (deactivate) ──────────────────────────────────────────
if (cmd == "remove") {
  if (length(args) < 2)
    stop("Usage: Rscript setup_officers.R remove <username>")
  username <- trimws(tolower(args[2]))
  
  existing <- dbGetQuery(con, "SELECT username FROM officers WHERE username=?", list(username))
  if (nrow(existing) == 0)
    stop(sprintf("Username '%s' not found.", username))
  
  # Deactivate rather than delete — preserves audit trail linkage
  dbExecute(con, "UPDATE officers SET active=0 WHERE username=?", list(username))
  cat(sprintf("✅ Officer '%s' deactivated (records preserved for audit trail).\n", username))
  quit(status=0)
}

stop(sprintf("Unknown command: '%s'. Use add, remove, list, or reset.", cmd))