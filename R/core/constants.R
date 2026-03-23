# ================================================================
#  R/constants.R — NCIC Taxonomy, Colors, App Constants
#  Radicalisation Signals · IEA Kenya NIRU
# ================================================================

library(DT);       library(dplyr);      library(writexl)
library(httr2);    library(jsonlite);   library(shinyjs)
library(digest);   library(future);     library(promises)
library(plotly);   library(later);      library(highcharter)
library(sf);       library(tmap)
library(DBI);      library(RSQLite);    library(bcrypt)
library(prophet)

plan(multisession)
`%||%` <- function(a, b) if (!is.null(a)) a else b

# ── CONSTANTS ────────────────────────────────────────────────────
APP_NAME         <- "Radicalisation Signals"
APP_SUBTITLE     <- "AI Early Warning Platform for Kenya"
OPENAI_MODEL     <- "gpt-4o-mini"
CACHE_DIR        <- "cache"
CACHE_FILE       <- file.path(CACHE_DIR, "classify_cache.rds")  # GPT response cache (RDS)
DB_FILE          <- file.path(CACHE_DIR, "ews.sqlite")           # persistent SQLite store
VAL_PAGE_SIZE    <- 6
SESSION_TIMEOUT  <- 20L   # minutes of inactivity before auto-lock

# ── NCIC 6-LEVEL TAXONOMY ───────────────────────────────────────
NCIC_LEVELS <- c(
  "0" = "Neutral Discussion",
  "1" = "Offensive Language",
  "2" = "Prejudice / Stereotyping",
  "3" = "Dehumanization",
  "4" = "Hate Speech",
  "5" = "Toxic"
)

NCIC_COLORS <- c(
  "0" = "#198754",  # green       — Neutral Discussion
  "1" = "#85b800",  # yellow-green — Offensive Language
  "2" = "#ffc107",  # amber        — Prejudice / Stereotyping
  "3" = "#fd7e14",  # orange       — Dehumanization
  "4" = "#dc3545",  # red          — Hate Speech
  "5" = "#7b0000"   # deep red     — Toxic
)

NCIC_BASE_SCORES <- c("0"=2,"1"=15,"2"=30,"3"=55,"4"=72,"5"=92)

# Okabe-Ito color-blind safe palette for platform/category charts
OKABE_ITO <- c("#E69F00","#56B4E9","#009E73","#F0E442","#0072B2","#D55E00","#CC79A7","#000000")

NCIC_ACTIONS <- c(
  "0" = "No action required. Log for trend analysis.",
  "1" = "Monitor. Flag for weekly review.",
  "2" = "Document pattern. Request platform review if repeated.",
  "3" = "Issue monitoring flag. Initiate engagement with poster.",
  "4" = "Section 13 threshold met. Require takedown. Alert DCI.",
  "5" = "IMMEDIATE ESCALATION. Press charges. Contact DCI now."
)

NCIC_SECTION13 <- c("0"=FALSE,"1"=FALSE,"2"=FALSE,"3"=FALSE,"4"=TRUE,"5"=TRUE)

# maps old ml_labels to NCIC levels for seed data
LABEL_TO_NCIC <- c(
  "TOXIC"       = "5",
  "HATE SPEECH" = "4",
  "PENDING"     = "2",
  "SAFE"        = "0"
)

ncic_color   <- function(lvl) NCIC_COLORS[as.character(lvl)]   %||% "#6c757d"
ncic_name    <- function(lvl) NCIC_LEVELS[as.character(lvl)]   %||% "Unknown"
ncic_action  <- function(lvl) NCIC_ACTIONS[as.character(lvl)]  %||% "Monitor."
ncic_s13     <- function(lvl) isTRUE(NCIC_SECTION13[as.character(lvl)])


