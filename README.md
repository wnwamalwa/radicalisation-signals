# 🇰🇪 Radicalisation Signals — v5

> AI-assisted early warning platform for hate speech, ethnic incitement, and radicalisation in Kenya.
> Built on the **NCIC Act Cap 170** framework with GPT-4o-mini and mandatory Human-in-the-Loop validation.

**Live dashboard:** https://wnwamalwa.shinyapps.io/Radicalisation-Signals/

---

## Overview

Radicalisation Signals monitors online content across Kenya's social media landscape for harmful narratives — classifying each post against the NCIC 6-level taxonomy, scoring risk, and surfacing high-severity signals to NCIC intelligence officers for validation before any legal or operational action is taken.

The platform shifts NCIC from **reactive response** to **proactive, geographically targeted prevention**.

---

## Features

| Feature | Detail |
|---|---|
| **AI Classification** | GPT-4o-mini · NCIC 6-level taxonomy · 3-stage decision chain (Violence Override → Target → Intent) |
| **Language Detection** | GPT-based — Swahili, English, Sheng, Kikuyu, Luo, Kalenjin · per-language hate-speech glossaries injected into prompt |
| **Risk Scoring** | Composite formula: `0.55 × (0.30×conf + 0.22×kw + 0.13×net + 0.08×spike + 0.12×ctx + 0.15×src_hist) + 0.45×NCIC_base` |
| **Human-in-the-Loop** | Officer validation: Confirm / Escalate / Downgrade / Clear · every L2+ case requires review |
| **Forecast** | Prophet time-series models per county · 14-day horizon · 80% CI · heuristic fallback |
| **S13 Queue** | Confirmed L4/L5 cases tracked: Pending → Filed → DCI Alerted → Resolved |
| **Audit Log** | Tamper-evident log of every officer action · admin-only · CSV export |
| **Security** | bcrypt-hashed credentials · role-based access (admin/officer) · 20-min session timeout · supervisor email alert |
| **Anti-hallucination** | 6 layers: temperature=0 · JSON schema · Cap 170 chain · confidence scoring · HITL · disagreement retraining |
| **Dashboard** | Signal Intelligence Dashboard · NCIC distribution · platform breakdown · county choropleth · network analysis |
| **Exports** | Excel: Quick Briefing · Detailed Report · NCIC Legal Report · County Report |
| **Loading indicators** | Global animated progress bar on all renders and GPT calls |
| **Auto-setup** | Missing packages auto-installed · admin seeded · S13 queue backfilled on startup |

---

## Stack

```
R Shiny + bslib       — UI framework
GPT-4o-mini           — AI classification + language detection
Prophet               — Time-series county forecast
SQLite (WAL mode)     — Persistent storage, cross-session sync
bcrypt                — Password hashing
leaflet + tmap        — Maps (IEBC official shapefiles)
highcharter + plotly  — Charts
shinyjs               — Loading indicators, session control
```

---

## Setup

### 1 — Clone the repository

```bash
git clone https://github.com/wnwamalwa/radicalisation-signals.git
cd radicalisation-signals/4
```

### 2 — Create `secrets/.Renviron`

```bash
mkdir -p secrets
cat > secrets/.Renviron << 'EOF'
OPENAI_API_KEY=sk-proj-...
GMAIL_USER=you@gmail.com
GMAIL_PASS=xxxx xxxx xxxx xxxx
OFFICER_EMAIL=officer@ncic.go.ke
SUPERVISOR_EMAIL=supervisor@ncic.go.ke
ADMIN_USERNAME=admin
ADMIN_PASSWORD=YourStr0ng!Pass
EOF
```

> **Password requirements:** 10+ characters, uppercase, lowercase, digit, special character. No common words.

### 3 — Place the IEBC shapefile

```
data/kenyan-counties/County.shp   (and .dbf, .shx, .prj)
```

### 4 — Run the app

```r
shiny::runApp()
```

Missing packages are **auto-installed on first run**. The admin account is **auto-seeded** from `.Renviron`. No manual setup required.

---

## Officer Management

Use `setup_officers.R` to manage officer accounts from the command line:

```bash
# Add an officer
Rscript setup_officers.R add    omwangi  'Mwang!2025#' officer

# Add an admin
Rscript setup_officers.R add    admin    'S3cur!ty#Ke' admin

# Reset a password
Rscript setup_officers.R reset  omwangi  'NewPass!99#'

# List all officers
Rscript setup_officers.R list

# Deactivate an officer
Rscript setup_officers.R remove omwangi
```

> Passwords are stored as **bcrypt hashes** (cost=12). Plain text is never persisted.

---

## NCIC 6-Level Taxonomy

| Level | Label | Section 13 | Action |
|---|---|---|---|
| L0 | Neutral Discussion | — | No action |
| L1 | Offensive Language | — | Monitor |
| L2 | Prejudice / Stereotyping | — | Document pattern |
| L3 | Dehumanization | — | Monitoring flag |
| L4 | Hate Speech | **YES** | Takedown · Alert DCI |
| L5 | Toxic | **YES** | **IMMEDIATE ESCALATION** |

---

## Legal Framework

- NCIC Act **Cap 170**
- Section 13 (hate speech threshold)
- Kenya Constitution 2010, Article 33 (freedom of expression — activism protection)

---

## Security

- All credentials stored as **bcrypt hashes** — never in plain text
- **Role-based access**: `admin` role can view audit log and manage officers; `officer` role validates cases
- **Session timeout**: 20 minutes inactivity → auto-lock → supervisor email notification
- **Audit trail**: every LOGIN, LOGOUT, VALIDATE, ESCALATE, S13 action logged to SQLite with timestamp and session ID
- **Never commit** `secrets/`, `cache/`, `*.sqlite`, or `.Renviron` to version control

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `OPENAI_API_KEY` | ✅ | GPT-4o-mini API key |
| `GMAIL_USER` | For email | Gmail address for outbound alerts |
| `GMAIL_PASS` | For email | Gmail app password (not your login password) |
| `OFFICER_EMAIL` | For email | Destination for case validation alerts |
| `SUPERVISOR_EMAIL` | Recommended | Destination for session timeout security alerts |
| `ADMIN_USERNAME` | ✅ | Username for the auto-seeded admin account |
| `ADMIN_PASSWORD` | ✅ | Password for the admin account (hashed on first run) |

---

## Project Structure

```
4/
├── app.R                  # Main application
├── setup_officers.R       # Officer management CLI
├── concept_note.Rmd       # PDF concept note (render with knitr)
├── secrets/
│   └── .Renviron          # Credentials (gitignored)
├── cache/
│   ├── ews.sqlite         # SQLite database (gitignored)
│   └── classify_cache.rds # GPT response cache (gitignored)
└── data/
    └── kenyan-counties/   # IEBC shapefiles
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Author

**Noah Wamalwa** · Project Lead / Developer
Developed for the **IEA Kenya NIRU AI Hackathon**
