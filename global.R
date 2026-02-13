# ======================
# GLOBAL.R
# ======================
library(shiny)
library(bslib)
library(leaflet)
library(dplyr)
library(DT)
library(plotly)
library(lubridate)

# SuperZip theme look-alike
super_theme <- bs_theme(
  version = 5,
  bootswatch = "flatly",
  base_font = font_google("Inter"),
  heading_font = font_google("Inter"),
  primary = "#2c3e50"
)

# Filter options
narrative_types <- c("Hate Speech", "Misinformation", "Extremism",
                     "Ethnic Tensions", "Political Narratives")

time_periods <- c("Last 6 hours", "Last 24 hours", "Last 7 days", "Last 30 days")

data_sources <- c("Social Media", "News Comments", "Telegram", "Forums", "Blogs")

sentiment_types <- c("Positive", "Neutral", "Negative", "Toxic")

alert_statuses <- c("All", "Flagged Only", "Validated", "Pending Review")

target_groups <- c("Ethnic", "Religious", "Political", "Gender-based")

trending_keywords <- c("#HateSpeech", "#FakeNews", "#Misinformation", "#Elections2027",
                       "#Propaganda", "#Tribalism", "#Violence", "#Protests",
                       "#Incitement", "#Radicalization", "#Extremism", "#PeaceKenya",
                       "#MediaFreedom", "#HumanRights", "#Corruption")

# Mock data for cases across Kenya
set.seed(123)
n_records <- 100

kenya_counties <- data.frame(
  county = c("Nairobi", "Mombasa", "Kisumu", "Eldoret", "Garissa",
             "Nyeri", "Wajir", "Mandera", "Kitui", "Machakos",
             "Nakuru", "Lamu", "Turkana", "Kilifi", "Kakamega"),
  lat = c(-1.286389, -4.043477, -0.091702, 0.514277, -0.456009,
          -0.419663, 1.7500, 3.9376, -1.3667, -1.5167,
          -0.3031, -2.2686, 3.1167, -3.5107, 0.2827),
  lng = c(36.817223, 39.668206, 34.7680, 35.2698, 39.6583,
          36.9476, 40.0667, 41.8667, 38.0167, 37.2667,
          36.0800, 40.9020, 35.5997, 39.8570, 34.7519)
)

# Generate random records with dates spread over 30 days
kenya_cases <- data.frame(
  id = 1:n_records,
  county = sample(kenya_counties$county, n_records, replace = TRUE),
  cases = sample(20:150, n_records, replace = TRUE),
  narrative_type = sample(narrative_types, n_records, replace = TRUE),
  data_source = sample(data_sources, n_records, replace = TRUE),
  sentiment = sample(sentiment_types, n_records, replace = TRUE),
  toxicity_score = sample(10:100, n_records, replace = TRUE),
  keywords = sapply(1:n_records, function(x) paste(sample(trending_keywords, sample(1:3, 1)), collapse = ", ")),
  alert_status = sample(c("Flagged Only", "Validated", "Pending Review"), n_records, replace = TRUE,
                        prob = c(0.3, 0.4, 0.3)),
  target_group = sample(target_groups, n_records, replace = TRUE),
  timestamp = Sys.time() - sample(1:(30*24*60*60), n_records)
)

# Merge coordinates
kenya_cases <- kenya_cases %>%
  left_join(kenya_counties, by = "county") %>%
  mutate(
    lat = lat + runif(n(), -0.15, 0.15),
    lng = lng + runif(n(), -0.15, 0.15),
    date = as.Date(timestamp),
    risk_level = case_when(
      cases < 50 ~ "Low",
      cases <= 100 ~ "Medium",
      TRUE ~ "High"
    )
  )

# Generate daily aggregated data for trends
daily_trends <- kenya_cases %>%
  group_by(date) %>%
  summarise(
    total_cases = sum(cases),
    avg_toxicity = mean(toxicity_score),
    incident_count = n(),
    .groups = "drop"
  ) %>%
  arrange(date)

# Sentiment daily trends
sentiment_trends <- kenya_cases %>%
  group_by(date, sentiment) %>%
  summarise(count = n(), .groups = "drop")

# Narrative type trends
narrative_trends <- kenya_cases %>%
  group_by(date, narrative_type) %>%
  summarise(count = n(), .groups = "drop")
