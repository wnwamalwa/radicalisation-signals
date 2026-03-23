# ================================================================
#  global.R — Shared Globals
#  Radicalisation Signals v6 · IEA Kenya NIRU AI Hackathon
#
#  Runs ONCE before ui.R and server.R — available to both.
#  This is the correct Shiny pattern for shinyapps.io deployment.
#
#  MODULE MAP:
#   R/constants.R         — NCIC taxonomy, colors, labels
#   R/db.R                — SQLite layer + v6 helper functions
#   R/scoring.R           — Risk scoring, keyword weights, clustering
#   R/data.R              — Seed data, counties, shapefile, case builder
#   R/gpt.R               — GPT classify, language detection, email, forecast
#   R/ui.R                — Full Shiny UI definition
#   R/server_init.R       — rv init, session, dashboard, classify, bulk
#   R/server_validation.R — Validation UI + do_validate + observers
#   R/server_learning.R   — Learning centre + v6 outputs + keyword editors
#   R/server_misc.R       — Forecast, reports, S13 queue, audit log
#
#  secrets/.Renviron (never commit):
#   OPENAI_API_KEY, GMAIL_USER, GMAIL_PASS, OFFICER_EMAIL,
#   SUPERVISOR_EMAIL, ADMIN_USERNAME, ADMIN_PASSWORD,
#   SUPABASE_URL, SUPABASE_KEY,
#   TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_BOT_TOKEN
# ================================================================

# ── Auto-install missing packages ────────────────────────────────
.required_pkgs <- c(
  "shiny","bslib","bsicons","leaflet","leaflet.extras2",
  "DT","dplyr","writexl","httr2","jsonlite","shinyjs",
  "digest","future","promises","plotly","later","highcharter",
  "sf","tmap","tools","DBI","RSQLite","bcrypt","prophet"
)
.missing <- .required_pkgs[!sapply(.required_pkgs, requireNamespace, quietly=TRUE)]
if (length(.missing) > 0) {
  message(sprintf("[startup] Installing %d missing package(s): %s",
                  length(.missing), paste(.missing, collapse=", ")))
  install.packages(.missing, repos="https://cloud.r-project.org", quiet=TRUE)
}
rm(.required_pkgs, .missing)

readRenviron("secrets/.Renviron")

library(shiny);    library(bslib);      library(bsicons)
library(leaflet);  library(leaflet.extras2)
library(DT);       library(dplyr);      library(writexl)
library(httr2);    library(jsonlite);   library(shinyjs)
library(digest);   library(future);     library(promises)
library(plotly);   library(later);      library(highcharter)
library(sf);       library(tmap)
library(DBI);      library(RSQLite);    library(bcrypt)
library(prophet)

plan(multisession)
`%||%` <- function(a, b) if (!is.null(a)) a else b

# ── Load modules ──────────────────────────────────────────────────
source("R/constants.R")   # NCIC taxonomy, colors, app constants
source("R/db.R")          # database layer + v6 helpers
source("R/scoring.R")     # risk scoring, keyword weights
source("R/data.R")        # seed data, counties, shapefile
source("R/gpt.R")         # GPT, language detection, email, forecast
source("R/ui.R")          # ui object
