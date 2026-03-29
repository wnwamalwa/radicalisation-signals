# ================================================================
#  ingest_pipelines.R
#  Radicalisation Signals — Pipeline Ingestion Bridge
#
#  PURPOSE:
#    Pulls real signals from Supabase (pipeline_telegram +
#    pipeline_youtube), runs GPT classification on each,
#    and writes classified cases into SQLite cases table.
#
#  USAGE:
#    # From R console (run once to ingest):
#    source("ingest_pipelines.R")
#
#    # From app.R (call function on demand / on schedule):
#    .pipeline_sourced_by_app <- TRUE
#    source("ingest_pipelines.R")
#    ingest_pipelines()
#
#  PREREQUISITES:
#    - app.R must be sourced first (provides classify_tweet,
#      score_risk, db_connect, kw_weights_global, counties, etc.)
#    - secrets/.Renviron must contain SUPABASE_URL + SUPABASE_KEY
#      + OPENAI_API_KEY
# ================================================================

# ── COUNTY REVERSE-GEOCODER ───────────────────────────────────────
# Maps (lat, lng) → nearest county name using Euclidean distance
# on the counties data frame (loaded by app.R / data.R)
lat_lng_to_county <- function(lat, lng) {
  if (is.null(lat) || is.null(lng) ||
      is.na(lat)  || is.na(lng)) return("Unknown")
  dists <- sqrt((counties$lat - lat)^2 + (counties$lng - lng)^2)
  counties$name[which.min(dists)]
}

# ── REGION DETECTOR ──────────────────────────────────────────────
# Kenya bounding box: lat -4.7 to 4.6, lng 34.0 to 41.9
KENYA_BBOX <- list(lat_min=-4.7, lat_max=4.6, lng_min=34.0, lng_max=41.9)

REGIONAL_COUNTRIES <- c(
  # East Africa — primary focus
  "somalia","somali","mogadishu","hargeisa","al-shabaab","alshabab","amisom",
  "ethiopia","ethiopian","addis ababa","tigray","oromo","amhara",
  "uganda","ugandan","kampala","museveni","bobi wine",
  "tanzania","tanzanian","dar es salaam","dodoma","magufuli","samia",
  "rwanda","rwandan","kigali","kagame",
  "burundi","burundian","bujumbura","gitega",
  # Wider region
  "south sudan","sudan","drc","congo","eritrea",
  # Regional orgs / threats
  "igad","eac","east africa","horn of africa","great lakes"
)

detect_region <- function(lat, lng, text) {
  # Check lat/lng first
  if (!is.na(lat) && !is.na(lng)) {
    in_kenya <- lat >= KENYA_BBOX$lat_min && lat <= KENYA_BBOX$lat_max &&
                lng >= KENYA_BBOX$lng_min && lng <= KENYA_BBOX$lng_max
    if (!in_kenya) return("Regional")
  }
  # Check text for regional country mentions
  txt <- tolower(text %||% "")
  if (any(sapply(REGIONAL_COUNTRIES, function(c) grepl(c, txt, fixed=TRUE))))
    return("Regional")
  "Kenya"
}

# ── SUPABASE FETCH (paginated) ────────────────────────────────────
supa_fetch_all <- function(table, since_id = 0L) {
  supa_url <- Sys.getenv("SUPABASE_URL")
  supa_key <- Sys.getenv("SUPABASE_KEY")
  all_rows <- list()
  offset   <- 0L
  page     <- 1000L

  repeat {
    resp <- tryCatch(
      request(paste0(supa_url, "/rest/v1/", table)) |>
        req_headers(
          "apikey"        = supa_key,
          "Authorization" = paste("Bearer", supa_key),
          "Range-Unit"    = "items",
          "Range"         = sprintf("%d-%d", offset, offset + page - 1L)
        ) |>
        req_url_query(
          select = "*",
          id     = paste0("gt.", since_id)
        ) |>
        req_error(is_error = \(r) FALSE) |>
        req_perform() |>
        resp_body_json(simplifyVector = TRUE),
      error = function(e) {
        message(sprintf("  [fetch] ERROR on %s: %s", table, e$message))
        NULL
      }
    )

    if (is.null(resp) || length(resp) == 0) break
    if (is.data.frame(resp) && nrow(resp) == 0) break

    all_rows <- c(all_rows, list(as.data.frame(resp)))
    if (nrow(as.data.frame(resp)) < page) break
    offset <- offset + page
  }

  if (length(all_rows) == 0) return(data.frame())
  do.call(rbind, all_rows)
}

# ── ALREADY INGESTED IDS ──────────────────────────────────────────
already_ingested <- function(source_tag) {
  tryCatch({
    rows <- .supa_get("cases",
      list(case_id=paste0("like.", source_tag, "-*")),
      limit=50000L)
    if (nrow(rows) == 0) return(character(0))
    rows$case_id
  }, error = function(e) {
    message(sprintf("[ingest] Warning fetching ingested IDs: %s", e$message))
    character(0)
  })
}

# ── MAP TELEGRAM ROW → cases ROW ─────────────────────────────────
telegram_to_case <- function(row, gpt_result, kw_w) {
  lvl    <- gpt_result$ncic_level  %||% 0L
  conf   <- gpt_result$confidence  %||% 0L
  rs     <- gpt_result$risk_score  %||% 20L
  ts     <- row$date %||% row$created_at %||% format(Sys.time())

  data.frame(
    case_id              = paste0("TG-", row$id),
    county               = lat_lng_to_county(
                             suppressWarnings(as.numeric(row$latitude)),
                             suppressWarnings(as.numeric(row$longitude))),
    sub_location         = as.character(row$chat_title %||% ""),
    src_lat              = suppressWarnings(as.numeric(row$latitude  %||% NA)),
    src_lng              = suppressWarnings(as.numeric(row$longitude %||% NA)),
    target_county        = "",
    tgt_lat              = NA_real_,
    tgt_lng              = NA_real_,
    target_group         = as.character(gpt_result$target_group %||% "None"),
    platform             = "Telegram",
    handle               = as.character(
                             row$sender_username %||% row$sender_name %||% "unknown"),
    text                 = as.character(row$text %||% ""),
    language             = as.character(gpt_result$language %||% "Unknown"),
    ncic_level           = as.integer(lvl),
    ncic_label           = as.character(ncic_name(lvl)),
    section_13           = as.integer(isTRUE(gpt_result$section_13)),
    confidence           = paste0(conf, "%"),
    conf_num             = as.integer(conf),
    conf_band            = as.character(conf_band(conf)),
    category             = as.character(gpt_result$category %||% row$ncic_category %||% "Unknown"),
    validated_by         = NA_character_,
    validated_at         = NA_character_,
    action_taken         = NA_character_,
    officer_ncic_override= NA_integer_,
    risk_score           = as.integer(rs),
    risk_level           = as.character(if (rs >= 65) "HIGH" else if (rs >= 35) "MEDIUM" else "LOW"),
    risk_formula         = as.character(gpt_result$risk_formula %||% ""),
    kw_score             = as.integer(row$signal_score %||% 0L),
    network_score        = 20L,
    signals              = as.character(paste(unlist(gpt_result$signals %||% list()), collapse="; ")),
    trend_data           = "",
    timestamp            = as.character(ts),
    timestamp_chr        = format(as.POSIXct(substr(ts, 1, 19),
                             format="%Y-%m-%dT%H:%M:%S", tz="UTC"),
                             "%d %b %Y %H:%M", tz="Africa/Nairobi"),
    notes                = "",
    region               = detect_region(suppressWarnings(as.numeric(row$latitude)), suppressWarnings(as.numeric(row$longitude)), as.character(row$text %||% "")),
    source               = "telegram",
    created_at           = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    source               = "telegram",
    created_at           = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    stringsAsFactors     = FALSE
  )
}

# ── MAP YOUTUBE ROW → cases ROW ───────────────────────────────────
youtube_to_case <- function(row, gpt_result) {
  lvl  <- gpt_result$ncic_level %||% 0L
  conf <- gpt_result$confidence %||% 0L
  rs   <- gpt_result$risk_score %||% 20L
  ts   <- row$published_at %||% row$ingested_at %||% format(Sys.time())

  data.frame(
    case_id              = paste0("YT-", row$id),
    county               = lat_lng_to_county(
                             suppressWarnings(as.numeric(row$latitude)),
                             suppressWarnings(as.numeric(row$longitude))),
    sub_location         = as.character(row$video_title %||% ""),
    src_lat              = suppressWarnings(as.numeric(row$latitude  %||% NA)),
    src_lng              = suppressWarnings(as.numeric(row$longitude %||% NA)),
    target_county        = "",
    tgt_lat              = NA_real_,
    tgt_lng              = NA_real_,
    target_group         = as.character(gpt_result$target_group %||% "None"),
    platform             = "YouTube",
    handle               = as.character(row$author %||% row$channel %||% "unknown"),
    text                 = as.character(row$text %||% ""),
    language             = as.character(gpt_result$language %||% "Unknown"),
    ncic_level           = as.integer(lvl),
    ncic_label           = as.character(ncic_name(lvl)),
    section_13           = as.integer(isTRUE(gpt_result$section_13)),
    confidence           = paste0(conf, "%"),
    conf_num             = as.integer(conf),
    conf_band            = as.character(conf_band(conf)),
    category             = as.character(gpt_result$category %||% row$ncic_category %||% "Unknown"),
    validated_by         = NA_character_,
    validated_at         = NA_character_,
    action_taken         = NA_character_,
    officer_ncic_override= NA_integer_,
    risk_score           = as.integer(rs),
    risk_level           = as.character(if (rs >= 65) "HIGH" else if (rs >= 35) "MEDIUM" else "LOW"),
    risk_formula         = as.character(gpt_result$risk_formula %||% ""),
    kw_score             = as.integer(row$signal_score %||% 0L),
    network_score        = 20L,
    signals              = as.character(paste(unlist(gpt_result$signals %||% list()), collapse="; ")),
    trend_data           = "",
    timestamp            = as.character(ts),
    timestamp_chr        = tryCatch(
                             format(as.POSIXct(substr(ts, 1, 19),
                               format="%Y-%m-%dT%H:%M:%S", tz="UTC"),
                               "%d %b %Y %H:%M", tz="Africa/Nairobi"),
                             error = function(e) as.character(ts)),
    notes                = "",
    source               = "youtube",
    created_at           = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ"),
    stringsAsFactors     = FALSE
  )
}

# ── MAIN INGEST FUNCTION ──────────────────────────────────────────
ingest_pipelines <- function(
    tg_limit  = 200L,   # max Telegram rows per run
    yt_limit  = 200L,   # max YouTube rows per run
    min_score = 1L,     # skip rows below this signal score
    dry_run   = FALSE   # TRUE = classify but don't write to SQLite
) {

  message("\n", strrep("=", 60))
  message("PIPELINE INGESTION BRIDGE")
  message(format(Sys.time(), "%d %b %Y %H:%M EAT"))
  message(strrep("=", 60))

  api_key  <- Sys.getenv("OPENAI_API_KEY")
  if (!nchar(api_key))
    stop("OPENAI_API_KEY not found in secrets/.Renviron")

  total_inserted <- 0L
  total_skipped  <- 0L
  total_errors   <- 0L

  # ── TELEGRAM ───────────────────────────────────────────────────
  message("\n[telegram] Fetching from Supabase...")
  tg_raw <- supa_fetch_all("pipeline_telegram")

  if (nrow(tg_raw) == 0) {
    message("[telegram] No rows found in Supabase")
  } else {
    # Filter by signal score
    tg_raw <- tg_raw[!is.na(tg_raw$signal_score) &
                      tg_raw$signal_score >= min_score, ]
    # Skip already ingested
    done   <- already_ingested("TG")
    tg_new <- tg_raw[!paste0("TG-", tg_raw$id) %in% done, ]
    # Apply limit
    tg_new <- head(tg_new[order(-tg_new$signal_score), ], tg_limit)

    message(sprintf("[telegram] %d total | %d already ingested | %d to classify",
                    nrow(tg_raw), length(done), nrow(tg_new)))

    if (nrow(tg_new) > 0) {
      for (i in seq_len(nrow(tg_new))) {
        row  <- tg_new[i, ]
        text <- trimws(row$text %||% "")

        if (nchar(text) < 10) { total_skipped <- total_skipped + 1L; next }

        message(sprintf("  [TG %d/%d] Classifying: %s",
                        i, nrow(tg_new), substr(text, 1, 60)))

        gpt <- tryCatch(
          classify_tweet(text, kw_weights_global,
                         handle  = as.character(row$sender_username %||% "unknown"),
                         county  = lat_lng_to_county(
                                     suppressWarnings(as.numeric(row$latitude)),
                                     suppressWarnings(as.numeric(row$longitude))),
                         cases_df = data.frame()),
          error = function(e) {
            message(sprintf("    ⚠ GPT error: %s", e$message))
            NULL
          }
        )

        if (is.null(gpt)) { total_errors <- total_errors + 1L; next }

        # Regional signals: only keep L4 and L5
        is_regional <- detect_region(
          suppressWarnings(as.numeric(row$latitude)),
          suppressWarnings(as.numeric(row$longitude)),
          as.character(row$text %||% "")
        ) == "Regional"
        if (is_regional && (gpt$ncic_level %||% 0) < 4) {
          total_skipped <- total_skipped + 1L
          next
        }

        case_row <- telegram_to_case(row, gpt, kw_weights_global)

        if (!dry_run) {
          tryCatch({
            resp <- .supa_post("cases", case_row, upsert=TRUE)
            if (resp_status(resp) %in% c(200L, 201L, 204L)) {
              total_inserted <- total_inserted + 1L
              message(sprintf("    ✅ L%d | conf:%d%% | risk:%d | %s",
                              case_row$ncic_level, case_row$conf_num,
                              case_row$risk_score, case_row$county))
            } else {
              message(sprintf("    ❌ Supabase write failed: status %d", resp_status(resp)))
              total_errors <- total_errors + 1L
            }
          }, error = function(e) {
            message(sprintf("    ❌ Write error: %s", e$message))
            total_errors <- total_errors + 1L
          })
        } else {
          message(sprintf("    [DRY RUN] Would insert L%d | conf:%d%% | %s",
                          case_row$ncic_level, case_row$conf_num, case_row$county))
          total_inserted <- total_inserted + 1L
        }

        Sys.sleep(0.3)  # rate limit safety
      }
    }
  }

  # ── YOUTUBE ────────────────────────────────────────────────────
  message("\n[youtube] Fetching from Supabase...")
  yt_raw <- supa_fetch_all("pipeline_youtube")

  if (nrow(yt_raw) == 0) {
    message("[youtube] No rows found in Supabase")
  } else {
    # Filter test rows and low signal
    yt_raw <- yt_raw[!is.na(yt_raw$signal_score) &
                      yt_raw$signal_score >= min_score, ]
    yt_raw <- yt_raw[!grepl("test", tolower(yt_raw$text %||% ""), fixed=TRUE) |
                      yt_raw$signal_score >= 2L, ]
    # Skip already ingested
    done   <- already_ingested("YT")
    yt_new <- yt_raw[!paste0("YT-", yt_raw$id) %in% done, ]
    # Apply limit
    yt_new <- head(yt_new[order(-yt_new$signal_score), ], yt_limit)

    message(sprintf("[youtube] %d total | %d already ingested | %d to classify",
                    nrow(yt_raw), length(done), nrow(yt_new)))

    if (nrow(yt_new) > 0) {
      for (i in seq_len(nrow(yt_new))) {
        row  <- yt_new[i, ]
        text <- trimws(row$text %||% "")

        if (nchar(text) < 10) { total_skipped <- total_skipped + 1L; next }

        message(sprintf("  [YT %d/%d] Classifying: %s",
                        i, nrow(yt_new), substr(text, 1, 60)))

        gpt <- tryCatch(
          classify_tweet(text, kw_weights_global,
                         handle  = as.character(row$author %||% "unknown"),
                         county  = lat_lng_to_county(
                                     suppressWarnings(as.numeric(row$latitude)),
                                     suppressWarnings(as.numeric(row$longitude))),
                         cases_df = data.frame()),
          error = function(e) {
            message(sprintf("    ⚠ GPT error: %s", e$message))
            NULL
          }
        )

        if (is.null(gpt)) { total_errors <- total_errors + 1L; next }

        # Regional signals: only keep L4 and L5
        is_regional <- detect_region(
          suppressWarnings(as.numeric(row$latitude)),
          suppressWarnings(as.numeric(row$longitude)),
          as.character(row$text %||% "")
        ) == "Regional"
        if (is_regional && (gpt$ncic_level %||% 0) < 4) {
          total_skipped <- total_skipped + 1L
          next
        }

        case_row <- youtube_to_case(row, gpt)

        if (!dry_run) {
          tryCatch({
            resp <- .supa_post("cases", case_row, upsert=TRUE)
            if (resp_status(resp) %in% c(200L, 201L, 204L)) {
              total_inserted <- total_inserted + 1L
              message(sprintf("    ✅ L%d | conf:%d%% | risk:%d | %s",
                              case_row$ncic_level, case_row$conf_num,
                              case_row$risk_score, case_row$county))
            } else {
              message(sprintf("    ❌ Supabase write failed: status %d", resp_status(resp)))
              total_errors <- total_errors + 1L
            }
          }, error = function(e) {
            message(sprintf("    ❌ Write error: %s", e$message))
            total_errors <- total_errors + 1L
          })
        } else {
          message(sprintf("    [DRY RUN] Would insert L%d | conf:%d%% | %s",
                          case_row$ncic_level, case_row$conf_num, case_row$county))
          total_inserted <- total_inserted + 1L
        }

        Sys.sleep(0.3)
      }
    }
  }

  # ── SUMMARY ────────────────────────────────────────────────────
  message("\n", strrep("=", 60))
  message("INGESTION COMPLETE")
  message(strrep("=", 60))
  message(sprintf("  Inserted : %d", total_inserted))
  message(sprintf("  Skipped  : %d (short text / below threshold)", total_skipped))
  message(sprintf("  Errors   : %d", total_errors))

  if (!dry_run) {
    total <- tryCatch({
    r <- .supa_get("cases", list(), limit=1L)
    # Get count from Supabase header
    nrow(.supa_get("cases", limit=100000L))
  }, error=function(e) NA)
    message(sprintf("  Total in SQLite cases: %d", total))
  }
  message(strrep("=", 60))

  invisible(list(inserted=total_inserted, skipped=total_skipped, errors=total_errors))
}

# ── AUTO-RUN WHEN SOURCED STANDALONE ─────────────────────────────
if (!exists(".pipeline_sourced_by_app")) {
  # Source app.R first to get all helper functions
  if (!exists("classify_tweet")) {
    message("[ingest] Sourcing app.R to load helper functions...")
    source("app.R")
  }
  ingest_pipelines()
}
