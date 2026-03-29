# ================================================================
#  twitter_test.R — Standalone Twitter/X API test
#  Run this BEFORE integrating into app.R
#  
#  Usage: source("twitter_test.R") OR run line by line in RStudio
# ================================================================

readRenviron("secrets/.Renviron")

library(httr2)
library(jsonlite)

# ── 1. CHECK TOKEN ───────────────────────────────────────────────
bearer <- Sys.getenv("TWITTER_BEARER_TOKEN")
if (nchar(bearer) == 0) {
  stop("TWITTER_BEARER_TOKEN not found in secrets/.Renviron")
}
message("✅ Bearer token found (", nchar(bearer), " chars)")

# ── 2. SEARCH FUNCTION ───────────────────────────────────────────
twitter_search <- function(query, max_results = 10) {
  
  message(sprintf("\n[twitter] Searching: '%s' (max %d)", query, max_results))
  
  resp <- tryCatch(
    request("https://api.twitter.com/2/tweets/search/recent") |>
      req_headers(Authorization = paste("Bearer", bearer)) |>
      req_url_query(
        query       = query,
        max_results = max_results,        # 10–100
        # Fields we want back
        `tweet.fields` = paste(c(
          "created_at",
          "author_id",
          "text",
          "lang",
          "public_metrics",
          "geo"
        ), collapse = ","),
        expansions  = "author_id",
        `user.fields` = "username,location"
      ) |>
      req_error(is_error = \(r) FALSE) |>
      req_perform() |>
      resp_body_json(),
    error = function(e) list(.__error__ = conditionMessage(e))
  )
  
  # ── Handle errors ──────────────────────────────────────────────
  if (!is.null(resp$`.__error__`))
    stop("Network error: ", resp$`.__error__`)
  
  if (!is.null(resp$errors)) {
    msg <- resp$errors[[1]]$message %||% "Unknown API error"
    stop("API error: ", msg)
  }
  
  if (!is.null(resp$status) && resp$status == 401)
    stop("401 Unauthorised — check your Bearer Token")
  
  if (!is.null(resp$status) && resp$status == 429)
    stop("429 Rate limited — wait 15 minutes and try again")
  
  n_results <- resp$meta$result_count %||% 0
  message(sprintf("[twitter] Got %d tweet(s)", n_results))
  
  if (n_results == 0) {
    message("[twitter] No results — try a broader query")
    return(data.frame())
  }
  
  # ── Build user lookup: author_id → username + location ─────────
  users <- resp$includes$users %||% list()
  user_map <- setNames(
    lapply(users, function(u) list(
      username = u$username %||% "unknown",
      location = u$location %||% ""
    )),
    sapply(users, `[[`, "id")
  )
  
  # ── Parse tweets into data frame ────────────────────────────────
  tweets_df <- do.call(rbind, lapply(resp$data, function(t) {
    uid  <- t$author_id %||% ""
    user <- user_map[[uid]] %||% list(username="unknown", location="")
    
    data.frame(
      tweet_id    = t$id,
      text        = t$text,
      author_id   = uid,
      username    = paste0("@", user$username),
      location    = user$location,
      lang        = t$lang %||% "unknown",
      created_at  = t$created_at %||% "",
      retweets    = t$public_metrics$retweet_count %||% 0,
      likes       = t$public_metrics$like_count    %||% 0,
      replies     = t$public_metrics$reply_count   %||% 0,
      stringsAsFactors = FALSE
    )
  }))
  
  tweets_df
}

# ── 3. COUNTY GUESSER ────────────────────────────────────────────
# Tries to guess Kenya county from user's location field
guess_county <- function(location) {
  loc <- tolower(location)
  county_keywords <- list(
    "Nairobi"      = c("nairobi","nbi","cbd","westlands","karen","kibera","kasarani"),
    "Mombasa"      = c("mombasa","msa","coast","pwani","nyali","likoni"),
    "Kisumu"       = c("kisumu","lakeside","kondele","nyalenda"),
    "Nakuru"       = c("nakuru","rift valley","gilgil","naivasha"),
    "Uasin Gishu"  = c("eldoret","uasin gishu","eldoret"),
    "Kisii"        = c("kisii","gusii"),
    "Kilifi"       = c("kilifi","malindi","watamu"),
    "Garissa"      = c("garissa","northeastern"),
    "Meru"         = c("meru"),
    "Nyeri"        = c("nyeri","mt kenya","central"),
    "Kakamega"     = c("kakamega","western kenya"),
    "Kiambu"       = c("kiambu","thika","ruiru","limuru"),
    "Machakos"     = c("machakos","kitengela","athi river"),
    "Turkana"      = c("turkana","lodwar"),
    "Mandera"      = c("mandera"),
    "Wajir"        = c("wajir")
  )
  for (county in names(county_keywords)) {
    if (any(sapply(county_keywords[[county]], function(kw) grepl(kw, loc, fixed=TRUE))))
      return(county)
  }
  # fallback — random county weighted by population
  sample(c("Nairobi","Mombasa","Kisumu","Nakuru","Kiambu","Kakamega"), 1,
         prob = c(0.30, 0.15, 0.12, 0.10, 0.10, 0.10, 0.13)[1:6])
}

# ── 4. RUN TESTS ─────────────────────────────────────────────────
message("\n", strrep("=", 60))
message("TEST 1 — Swahili ethnic content query")
message(strrep("=", 60))

results1 <- tryCatch(
  twitter_search("kabila Kenya -is:retweet lang:sw", max_results = 10),
  error = function(e) { message("❌ ERROR: ", e$message); data.frame() }
)

if (nrow(results1) > 0) {
  message("\n✅ SUCCESS — sample results:")
  for (i in seq_len(min(3, nrow(results1)))) {
    message(sprintf("\n  [%d] @%s | lang:%s | RT:%d",
                    i, results1$username[i], results1$lang[i], results1$retweets[i]))
    message(sprintf("      %s", substr(results1$text[i], 1, 100)))
    message(sprintf("      County guess: %s", guess_county(results1$location[i])))
  }
} else {
  message("⚠ No results for this query")
}

message("\n", strrep("=", 60))
message("TEST 2 — English hate speech query")
message(strrep("=", 60))

results2 <- tryCatch(
  twitter_search("tribe Kenya hate -is:retweet lang:en", max_results = 10),
  error = function(e) { message("❌ ERROR: ", e$message); data.frame() }
)

if (nrow(results2) > 0) {
  message(sprintf("\n✅ SUCCESS — %d tweets fetched", nrow(results2)))
  message("\nColumn names: ", paste(names(results2), collapse=", "))
  message("\nLanguage breakdown:")
  print(table(results2$lang))
} else {
  message("⚠ No results for this query")
}

message("\n", strrep("=", 60))
message("TEST 3 — Rate limit check (meta info)")
message(strrep("=", 60))

resp_meta <- tryCatch(
  request("https://api.twitter.com/2/tweets/search/recent") |>
    req_headers(Authorization = paste("Bearer", bearer)) |>
    req_url_query(query = "Kenya", max_results = 10) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform(),
  error = function(e) NULL
)

if (!is.null(resp_meta)) {
  headers <- resp_headers(resp_meta)
  remaining <- headers[["x-rate-limit-remaining"]] %||% "unknown"
  reset_ts  <- headers[["x-rate-limit-reset"]]     %||% "unknown"
  limit     <- headers[["x-rate-limit-limit"]]     %||% "unknown"
  message(sprintf("Rate limit:    %s requests per window", limit))
  message(sprintf("Remaining:     %s requests", remaining))
  if (reset_ts != "unknown") {
    reset_time <- as.POSIXct(as.integer(reset_ts), origin="1970-01-01", tz="Africa/Nairobi")
    message(sprintf("Resets at:     %s EAT", format(reset_time, "%H:%M:%S")))
  }
}

# ── 5. SUMMARY ────────────────────────────────────────────────────
message("\n", strrep("=", 60))
message("SUMMARY")
message(strrep("=", 60))
t1_ok <- nrow(results1) > 0
t2_ok <- nrow(results2) > 0
message(sprintf("Test 1 (Swahili query):  %s", if(t1_ok) "✅ PASS" else "⚠ No results"))
message(sprintf("Test 2 (English query):  %s", if(t2_ok) "✅ PASS" else "⚠ No results"))

if (t1_ok || t2_ok) {
  all_results <- rbind(
    if(t1_ok) results1 else data.frame(),
    if(t2_ok) results2 else data.frame()
  )
  message(sprintf("\nTotal tweets fetched: %d", nrow(all_results)))
  message("Columns available:    ", paste(names(all_results), collapse=", "))
  message("\n✅ API connection working — ready to integrate into app.R")
} else {
  message("\n⚠ Both queries returned no results.")
  message("This could mean:")
  message("  1. No matching tweets in the last 7 days (try broader terms)")
  message("  2. Bearer token issue — regenerate in developer.twitter.com")
  message("  3. Rate limited — wait 15 minutes")
}

message(strrep("=", 60))

