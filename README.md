# Radicalisation Signals

AI-assisted early warning platform for harmful narratives and radicalisation in Kenya.
Built for the **IEA Kenya NIRU AI Hackathon**.

## Live Dashboard
https://wnwamalwa.shinyapps.io/Radicalisation-Signals/

## Overview
Monitors social media across all 47 Kenya counties for hate speech, ethnic incitement,
and radicalisation signals — classified under the NCIC Act Cap 170 six-level taxonomy
using GPT-4o-mini with mandatory human-in-the-loop officer validation.

## Stack
R Shiny · bslib · GPT-4o-mini · SQLite · IEBC Shapefiles · testthat

## Setup
1. Clone the repo
2. Create `secrets/.Renviron` containing: `OPENAI_API_KEY=sk-...`
3. Place IEBC shapefile at `data/kenyan-counties/County.shp`
4. Install packages listed at the top of `app.R`
5. `shiny::runApp()`

## Legal Framework
NCIC Act Cap 170 · Section 13 · Kenya Constitution 2010 Article 33

## Author
Noah Wamalwa
