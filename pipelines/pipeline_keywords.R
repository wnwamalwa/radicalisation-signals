# ================================================================
#  pipeline_keywords.R — Adaptive Keyword Extraction Pipeline
#  Radicalisation Signals · IEA Kenya NIRU AI Hackathon
#
#  WORKFLOW:
#    1. Pull recent messages from Supabase (pipeline_telegram)
#    2. Send batches to GPT for new signal term extraction
#    3. GPT suggests new keywords with tier + category
#    4. New keywords saved to keyword_bank as 'pending'
#    5. Officers review in app.R dashboard
#    6. Approved keywords go live immediately next pipeline run
#
#  Usage: source("pipeline_keywords.R")
#  Recommended: run after pipeline_telegram.R each day
# ================================================================

readRenviron("secrets/.Renviron")
library(httr2)
library(jsonlite)

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0) a else b

# ── CREDENTIALS ───────────────────────────────────────────────────
supa_url    <- Sys.getenv("SUPABASE_URL")
supa_key    <- Sys.getenv("SUPABASE_KEY")
openai_key  <- Sys.getenv("OPENAI_API_KEY")

if (nchar(supa_url)   == 0) stop("SUPABASE_URL not found")
if (nchar(supa_key)   == 0) stop("SUPABASE_KEY not found")
if (nchar(openai_key) == 0) stop("OPENAI_API_KEY not found")
message("✅ Credentials loaded")

# ── CONFIGURATION ─────────────────────────────────────────────────
BATCH_SIZE       <- 20L   # messages per GPT call
MAX_BATCHES      <- 5L    # max GPT calls per run (cost control)
GPT_MODEL        <- "gpt-4o-mini"  # cost-efficient model
MIN_CONFIDENCE   <- 0.7   # minimum GPT confidence to suggest keyword

# ── SUPABASE HELPERS ──────────────────────────────────────────────
supa_get <- function(table, query_params = list()) {
  req <- request(paste0(supa_url, "/rest/v1/", table)) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key)
    )
  if (length(query_params) > 0)
    req <- req |> req_url_query(!!!query_params)
  resp <- req_perform(req)
  fromJSON(resp_body_string(resp), flatten = TRUE)
}

supa_insert <- function(table, rows_df) {
  req <- request(paste0(supa_url, "/rest/v1/", table)) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key),
      "Content-Type"  = "application/json",
      "Prefer"        = "return=minimal"
    ) |>
    req_body_raw(
      toJSON(rows_df, auto_unbox = TRUE, na = "null"),
      type = "application/json"
    ) |>
    req_method("POST")
  req_perform(req)
}

# ── FETCH RECENT MESSAGES FROM SUPABASE ───────────────────────────
fetch_recent_messages <- function(limit = 100L) {
  message("[supabase] Fetching recent messages for keyword analysis...")
  req <- request(paste0(supa_url, "/rest/v1/pipeline_telegram")) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key),
      "Range"         = sprintf("0-%d", limit - 1L)
    ) |>
    req_url_query(
      select  = "text,signal_score,ncic_category,triggers",
      order   = "created_at.desc"
    )
  resp <- req_perform(req)
  fromJSON(resp_body_string(resp), flatten = TRUE)
}

# ── FETCH EXISTING KEYWORDS ───────────────────────────────────────
fetch_existing_keywords <- function() {
  message("[supabase] Loading existing keyword bank...")
  req <- request(paste0(supa_url, "/rest/v1/keyword_bank")) |>
    req_headers(
      "apikey"        = supa_key,
      "Authorization" = paste("Bearer", supa_key)
    ) |>
    req_url_query(select = "keyword")
  resp <- req_perform(req)
  df   <- fromJSON(resp_body_string(resp), flatten = TRUE)
  if (nrow(df) == 0) return(character())
  tolower(trimws(df$keyword))
}

# ── GPT KEYWORD EXTRACTION ────────────────────────────────────────
extract_keywords_gpt <- function(messages_text, existing_keywords) {

  # Build system prompt
  system_prompt <- "You are an expert in hate speech detection and 
radicalisation monitoring for Kenya, working with the National Cohesion 
and Integration Commission (NCIC). Your job is to identify NEW signal 
keywords from Kenyan social media messages that indicate:
- Hate speech (chuki, ubaguzi)
- Incitement to violence (uchochezi, vita)
- Dehumanisation of ethnic/religious groups
- Ethnic contempt and discrimination
- Undermining national unity
- Election-based incitement

You understand English, Swahili, Sheng, and Kenyan political context.
You must return ONLY a JSON array — no other text.
Each item must have exactly these fields:
{
  'keyword': 'the exact phrase',
  'tier': 1, 2, or 3,
  'category': one of INCITEMENT|DEHUMANISATION|ETHNIC_CONTEMPT|HATE_SPEECH|SECESSIONISM|ELECTION_INCITEMENT|RELIGIOUS_HATRED|ETHNIC_TENSION|DIVISIVE_CONTENT,
  'language': 'en' or 'sw' or 'sheng',
  'confidence': 0.0 to 1.0,
  'context': 'brief explanation of why this is a signal'
}

TIER DEFINITIONS:
Tier 3 = direct incitement to violence or dehumanisation (most severe)
Tier 2 = ethnic contempt, discrimination, secessionism
Tier 1 = divisive content, weak signals, undermining unity

RULES:
- Only suggest NEW keywords not already in the existing list
- Only suggest keywords that genuinely signal NCIC violations
- Do NOT suggest generic political terms (e.g. 'opposition', 'government')
- Focus on phrases that are specific to hate/incitement context
- Include Swahili and Sheng terms that are unique to Kenya
- Minimum confidence 0.7 to suggest a keyword"

  user_prompt <- paste0(
    "Existing keywords (do NOT suggest these again):\n",
    paste(existing_keywords, collapse = ", "),
    "\n\n---\n\n",
    "Analyse these Kenyan social media messages and suggest NEW signal keywords:\n\n",
    messages_text,
    "\n\n---\n\n",
    "Return ONLY a JSON array of new keyword suggestions. ",
    "If no new keywords found, return empty array: []"
  )

  # Call GPT
  resp <- request("https://api.openai.com/v1/chat/completions") |>
    req_headers(
      "Authorization" = paste("Bearer", openai_key),
      "Content-Type"  = "application/json"
    ) |>
    req_body_raw(
      toJSON(list(
        model    = GPT_MODEL,
        messages = list(
          list(role = "system", content = system_prompt),
          list(role = "user",   content = user_prompt)
        ),
        temperature      = 0.2,
        max_tokens       = 1000L,
        response_format  = list(type = "json_object")
      ), auto_unbox = TRUE),
      type = "application/json"
    ) |>
    req_method("POST") |>
    req_perform()

  result <- fromJSON(resp_body_string(resp), flatten = TRUE)
  raw    <- result$choices[[1]]$message$content %||% "[]"

  # Parse JSON response
  tryCatch({
    parsed <- fromJSON(raw, flatten = TRUE)
    # Handle both array and object with array inside
    if (is.data.frame(parsed)) return(parsed)
    if (is.list(parsed)) {
      # GPT sometimes wraps in object
      for (val in parsed) {
        if (is.data.frame(val)) return(val)
      }
    }
    return(data.frame())
  }, error = function(e) {
    message(sprintf("  ⚠ GPT parse error: %s", e$message))
    return(data.frame())
  })
}

# ── MAIN PIPELINE ─────────────────────────────────────────────────
message("\n", strrep("=", 60))
message("ADAPTIVE KEYWORD EXTRACTION PIPELINE")
message(format(Sys.time(), "%d %b %Y %H:%M EAT"))
message(strrep("=", 60))

# Step 1 — Fetch recent messages
messages_df <- fetch_recent_messages(limit = BATCH_SIZE * MAX_BATCHES)
message(sprintf("[load] %d messages loaded for analysis", nrow(messages_df)))

if (nrow(messages_df) == 0) {
  message("⚠ No messages found — run pipeline_telegram.R first")
  stop("No messages to analyse")
}

# Step 2 — Fetch existing keywords
existing_kws <- fetch_existing_keywords()
message(sprintf("[keywords] %d existing keywords loaded", length(existing_kws)))

# Step 3 — Process in batches
all_suggestions <- data.frame()
n_batches       <- min(MAX_BATCHES, ceiling(nrow(messages_df) / BATCH_SIZE))

message(sprintf("\n[gpt] Processing %d batches of %d messages each...",
                n_batches, BATCH_SIZE))

for (b in seq_len(n_batches)) {
  start <- (b - 1) * BATCH_SIZE + 1L
  end   <- min(b * BATCH_SIZE, nrow(messages_df))
  batch <- messages_df[start:end, ]

  # Format messages for GPT
  msgs_text <- paste(
    sapply(seq_len(nrow(batch)), function(i) {
      sprintf("[%d] %s", i, substr(batch$text[i], 1, 300))
    }),
    collapse = "\n\n"
  )

  message(sprintf("  Batch %d/%d (%d messages)...", b, n_batches, nrow(batch)))

  suggestions <- tryCatch(
    extract_keywords_gpt(msgs_text, existing_kws),
    error = function(e) {
      message(sprintf("  ❌ Batch %d error: %s", b, e$message))
      data.frame()
    }
  )

  if (nrow(suggestions) > 0) {
    message(sprintf("  ✅ %d new keywords suggested", nrow(suggestions)))
    all_suggestions <- rbind(all_suggestions, suggestions)
  } else {
    message(sprintf("  ℹ No new keywords found in batch %d", b))
  }

  Sys.sleep(1) # rate limit
}

# Step 4 — Filter by confidence and deduplicate
if (nrow(all_suggestions) > 0) {

  # Ensure required columns exist
  required_cols <- c("keyword","tier","category","language","confidence","context")
  missing <- setdiff(required_cols, names(all_suggestions))
  for (col in missing) all_suggestions[[col]] <- NA

  # Filter by confidence
  all_suggestions <- all_suggestions[
    !is.na(all_suggestions$confidence) &
    all_suggestions$confidence >= MIN_CONFIDENCE, ]

  # Remove already existing keywords
  all_suggestions <- all_suggestions[
    !tolower(trimws(all_suggestions$keyword)) %in% existing_kws, ]

  # Deduplicate
  all_suggestions <- all_suggestions[
    !duplicated(tolower(trimws(all_suggestions$keyword))), ]

  message(sprintf("\n[suggestions] %d new keywords after filtering",
                  nrow(all_suggestions)))

  if (nrow(all_suggestions) > 0) {

    # Prepare for Supabase insert
    to_insert <- data.frame(
      keyword      = tolower(trimws(all_suggestions$keyword)),
      tier         = as.integer(all_suggestions$tier),
      category     = all_suggestions$category,
      language     = all_suggestions$language %||% "en",
      status       = "pending",
      source       = "gpt",
      suggested_by = GPT_MODEL,
      context      = all_suggestions$context %||% "",
      times_matched = 0L,
      stringsAsFactors = FALSE
    )

    # Step 5 — Insert to Supabase as pending
    resp <- supa_insert("keyword_bank", to_insert)

    if (resp_status(resp) %in% c(200L, 201L, 204L)) {
      message(sprintf("✅ %d keywords saved to keyword_bank as PENDING",
                      nrow(to_insert)))
    } else {
      message(sprintf("❌ Insert failed: %s", resp_body_string(resp)))
    }

    # Show what was suggested
    message("\n[suggested keywords for officer review]")
    message(sprintf("%-30s %5s %-25s %-6s %s",
                    "Keyword", "Tier", "Category", "Lang", "Confidence"))
    message(strrep("-", 80))
    for (i in seq_len(nrow(all_suggestions))) {
      message(sprintf("%-30s %5d %-25s %-6s %.2f",
                      substr(all_suggestions$keyword[i], 1, 30),
                      all_suggestions$tier[i],
                      all_suggestions$category[i],
                      all_suggestions$language[i] %||% "en",
                      all_suggestions$confidence[i]))
    }
  }

} else {
  message("\n[gpt] No new keywords suggested this run")
}

# ── SUMMARY ───────────────────────────────────────────────────────
message("\n", strrep("=", 60))
message("KEYWORD BANK STATUS")
message(strrep("=", 60))

kb <- supa_get("keyword_bank")
if (nrow(kb) > 0) {
  status_counts <- table(kb$status)
  for (s in names(status_counts))
    message(sprintf("  %-10s : %d keywords", s, status_counts[s]))
  message(sprintf("\n  Total keywords: %d", nrow(kb)))
}

message("\n💡 Review pending keywords in the app.R officer dashboard")
message(strrep("=", 60))
