# SEO Prospecting Agent — New Generation Tool

## Overview
The SEO Prospecting Agent is a Python script that finds highly qualified local leads for SEO outreach. It targets businesses ranking beyond Page 1 (typically Google Page 2–5) that have basic on‑page SEO issues and generates an actionable sales pitch for outreach.

## Features
- Targeted SERP extraction using SerpApi (geo‑specific, keyword‑driven)
- Focus on Page 2+ results to find less competitive opportunities
- Deduplication and filtering (keeps best unique URLs, excludes directories like Yelp/Facebook)
- On‑page audits for:
    - Missing or empty H1 tag
    - Missing or unclear NAP (detected mainly via phone number)
- Retry mechanism (three attempts for web requests)
- Actionable reporting: final CSV with a personalized `Sales_Pitch` column

## Prerequisites
- Python 3.12 (preferred)
- SerpApi account and API key

## Required libraries
Install dependencies:
```bash
pip install pandas serpapi requests beautifulsoup4 python-dotenv
```

## Setup

1. Create a `.env` file in the project root:
```env
SERPAPI_API_KEY="YOUR_SERPAPI_KEY_HERE"
```

2. Create `serp_config.py` to define targets:
```python
# serp_config.py
CITIES = [
        "Austin",
        "Dallas",
        "Houston",
]

KEYWORDS = [
        "fleet maintenance service",
        "heavy duty truck repair",
        "diesel alignment shop",
]
```

## How to run
The script accepts command-line arguments for the start and end page numbers.

Syntax:
```bash
python agent.py <START_PAGE_NUMBER> <END_PAGE_NUMBER>
```

Example — scrape results from Page 2 (rank 11) to Page 5 (rank 50):
```bash
python agent.py 2 5
```

## Output
- CSV report containing audits and a `Sales_Pitch` column ready for outreach.

## Refinements and Enhancements (serp_config optimization)
Refining keywords improves lead quality. Examples:

- High intent — targets users seeking specific fixes:
    - Why: captures customers with immediate need
    - Examples: `emergency roadside tire change`, `mobile diesel repair near me`

- Service‑page only — targets businesses that should have dedicated service pages:
    - Why: easier to audit page content and pricing
    - Examples: `[Service] + pricing`, `[Service] + cost`, `[Service] + quote`

- Exclusion thinking — mentally avoid keywords dominated by national brands if you want more local opportunities (no code change required).

Use these refinements in `KEYWORDS` to get cleaner, higher‑intent results.

- On-page audit strategy — maybe fine tuning the audit strategy could generate higher quality leads
