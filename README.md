# Smart SEO Prospecting Agent 🤖

## Overview
The **Smart SEO Prospecting Agent** is an advanced Python tool designed to automate the discovery of high-value local SEO leads. Unlike standard scrapers, this agent performs a **"Gap Analysis"**: it compares a prospect's real-world reputation (Google Business Profile ratings) against their website's technical health to identify the most "winnable" clients.

It targets businesses typically ranking on Google Pages 2–5, audits them for critical SEO failures, and generates **human-like, narrative sales pitches** ready for cold outreach.

## 🚀 Key Features

### 🧠 Intelligent Filtering & Auditing
*   **Smart Directory Exclusion:** Automatically filters out noise like Indeed, Glassdoor, Gov sites, and huge national brands (Penske, Ford) to focus on local SMBs.
*   **Fuzzy Logic Matching:** Uses token-based matching for H1 tags. It won't fail a site just because the H1 is "Best Diesel Repair" instead of the exact keyword "Diesel Repair."
*   **Robust NAP Detection:** Detects phone numbers via regex and modern `href="tel:"` link analysis.
*   **Anti-Bot Handling:** Intelligently detects firewalls (403 Forbidden). It **SKIPS** these sites rather than marking them as "Broken," preventing embarrassing outreach errors.

### 📊 GBP & Gap Analysis
*   **Competitive Context:** The agent fetches **Google Business Profile (GBP)** data (Ratings & Review Counts) for every prospect.
*   **The "Gap" Strategy:**
    *   **Scenario A (Gold Mine):** Strong GBP Reputation + Weak Website = Pitch focuses on "Your site isn't doing your reputation justice."
    *   **Scenario B (Ghost):** No GBP Presence + Weak Website = Pitch focuses on "You are invisible to local customers."

### 📧 Actionable Output
*   **Human-Like Pitches:** No more robotic error lists (`Fail: H1`). The agent generates full sentences like: *"I noticed you have a great reputation on Maps (4.8 stars), but your website is missing the key tags needed to rank organically."*
*   **Dual Export:**
    1.  `Instantly_Import_Actionable_*.csv`: Cleaned, formatted file ready to upload directly to cold email tools (Instantly.ai, Lemlist).
    2.  `SEO_Detailed_Audit_*.xlsx`: A comprehensive Excel report with full audit data for human review.

## Prerequisites
*   Python 3.10+
*   SerpApi Account & API Key

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-folder>
    ```

2.  **Install dependencies:**
    ```bash
    pip install pandas serpapi requests beautifulsoup4 python-dotenv openpyxl
    ```

3.  **Environment Setup:**
    Create a `.env` file in the project root:
    ```env
    SERPAPI_API_KEY="YOUR_SERPAPI_KEY_HERE"
    ```

4.  **Target Configuration:**
    Create or edit `serp_config.py` to define your niche:
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
        "mobile diesel mechanic",
    ]
    ```

## Usage

The script accepts command-line arguments for the Google Search **Page Range**.

**Syntax:**
```bash
python Agent.py <START_PAGE> <END_PAGE>
```

**Example:**
To scrape results from Page 2 (Rank 11) to Page 5 (Rank 50):
```bash
python Agent.py 2 5
```

## How It Works (The Logic Flow)

1.  **Scrape:** Fetches organic results from Google using SerpApi.
2.  **Clean:** Removes directories (Yelp, Indeed), government sites, and PDFs.
3.  **Enrich:** Fetches the Top Competitor's GBP stats for that specific keyword/city.
4.  **Audit:** Visits every unique prospect URL to check:
    *   Server Status (Is it down or just blocked?)
    *   H1 Tags (Contextual relevance)
    *   NAP (Name, Address, Phone) visibility
    *   Meta Tags & Schema
5.  **Analyze:** Compares the Audit vs. GBP Data.
6.  **Pitch:** Generates a unique "Final_Pitch" string.
7.  **Export:** Saves the actionable leads to CSV and the full data to XLSX.

## Output Files

*   **`Instantly_Import_Actionable_YYYYMMDD.csv`**:
    *   Contains only leads marked `Actionable_Target: YES`.
    *   Columns: `Prospect_Name`, `Website_URL`, `Final_Pitch`, `City`, `Keyword`, etc.
    *   *Ready for import into cold email campaigns.*

*   **`SEO_Detailed_Audit_YYYYMMDD.xlsx`**:
    *   Contains ALL audited sites (including passed audits).
    *   Includes technical details: H1 content, Meta descriptions, specific error codes, and GBP stats.
    *   *Used for manual review or data analysis.*

## Optimization Tips
To get the best results, use **high-intent keywords** in your `serp_config.py`:
*   ❌ *Avoid:* "Trucks" (Too broad, national brands dominate).
*   ✅ *Use:* "Mobile semi truck repair near me" (High urgency, local intent).
*   ✅ *Use:* "Fleet maintenance pricing [City]" (Commercial intent).