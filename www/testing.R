# Cumulative Metrics Tracking in Shiny with Supabase
# This document contains improved R code blocks for robust, cumulative tracking of visitors and clicks in a Shiny dashboard using Supabase. The code ensures that metrics are always cumulative across sessions, with auto-saving and session-end logic.
# 1. Fetch Cumulative Totals from Supabase
# Fetches the last row from the Supabase metrics table, ensuring cumulative totals are retrieved on app launch.

# --- 1. Fetch Cumulative Totals from Supabase ---
fetch_current_metrics <- function() {
  tryCatch({
    cat("Fetching latest metrics row from Supabase...\n")
    response <- GET(
      url = paste0(supabase_url, "/rest/v1/metrics?select=*&order=id.desc&limit=1"),
      add_headers(
        apikey = supabase_api,
        Authorization = paste("Bearer", supabase_api)
      )
    )
    if (status_code(response) == 200) {
      data <- content(response, "parsed")
      if (length(data) > 0) {
        metrics <- data[[length(data)]]
        visitors_val <- as.numeric(metrics$visitors %||% 0)
        clicks_val <- as.numeric(metrics$clicks %||% 0)
        id_val <- metrics$id %||% NULL
        cat("✅ Successfully fetched metrics - Visitors:", visitors_val, "Clicks:", clicks_val, "\n")
        return(list(
          visitors = visitors_val,
          clicks = clicks_val,
          id = id_val
        ))
      } else {
        cat("⚠️ No existing metrics found, starting with defaults\n")
        return(list(visitors = 0, clicks = 0, id = NULL))
      }
    } else {
      cat("❌ Failed to fetch metrics. Status code:", status_code(response), "\n")
      return(list(visitors = 0, clicks = 0, id = NULL))
    }
  }, error = function(e) {
    cat("❌ Error fetching metrics:", e$message, "\n")
    return(list(visitors = 0, clicks = 0, id = NULL))
  })
}

# 2. Initialize Cumulative Counters on App Launch
# Initializes the local counters for visitors and clicks using the fetched cumulative totals.

# --- 2. Initialize Cumulative Counters on App Launch ---
initialize_metrics <- function() {
  cat("Initializing metrics on app launch...\n")
  current_metrics <- fetch_current_metrics()
  new_visitors <- current_metrics$visitors + 1  # Add this session's visitor
  new_clicks <- current_metrics$clicks          # Start from last total
  cat("Initializing with - Visitors:", new_visitors, "Clicks:", new_clicks, "\n")
  return(list(
    visitors = new_visitors,
    clicks = new_clicks,
    previous_visitors = current_metrics$visitors,
    previous_clicks = current_metrics$clicks
  ))
}

# 3. Auto-Save Observer (Cumulative, every 30 seconds, only if changed)
# Automatically saves the current cumulative metrics to Supabase every 30 seconds, but only if the values have changed since the last save.

# --- 3. Auto-Save Observer (Cumulative, every 30 seconds, only if changed) ---
last_saved <- reactiveValues(visitors = NULL, clicks = NULL)

observe({
  invalidateLater(30000, session)  # 30 seconds
  visitors_count <- visitors()
  clicks_count <- total_clicks()
  session_duration <- as.numeric(difftime(Sys.time(), session_start, units = "secs"))

  # Only save if values changed
  if (is.null(last_saved$visitors) || visitors_count != last_saved$visitors ||
      is.null(last_saved$clicks) || clicks_count != last_saved$clicks) {
    result <- save_metrics_to_supabase(visitors_count, clicks_count, session_duration)
    if (result) {
      last_saved$visitors <- visitors_count
      last_saved$clicks <- clicks_count
      cat("✅ Auto-saved metrics: Visitors =", visitors_count,
          "Clicks =", clicks_count,
          "Duration =", round(session_duration, 2), "seconds\n")
    } else {
      cat("❌ Failed to auto-save metrics\n")
    }
  }
})

# 4. On Session End (Cumulative)
# Saves the final cumulative metrics to Supabase when the session ends.
#
# # --- 4. On Session End (Cumulative) ---
session$onSessionEnded(function() {
  duration <- as.numeric(difftime(Sys.time(), session_start, units = "secs"))
  final_visitors <- isolate(visitors())
  final_clicks <- isolate(total_clicks())
  save_result <- save_metrics_to_supabase(final_visitors, final_clicks, duration)
  if (save_result) {
    cat("Session ended successfully. Final metrics saved - Visitors:", final_visitors,
        "Clicks:", final_clicks, "Duration:", round(duration, 2), "seconds\n")
  } else {
    cat("Session ended with error saving metrics\n")
  }
})

