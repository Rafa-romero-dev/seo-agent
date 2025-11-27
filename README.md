# Smart SEO Prospecting Agent 🤖

## Overview
The **Smart SEO Prospecting Agent** is an advanced Python tool designed to automate the discovery of high-value local SEO leads. It goes beyond simple scraping by acting as a fully autonomous agent: it finds prospects, audits their site, **crawls for contact information**, and uses **Google Gemini AI** to generate a hyper-personalized 3-stage email campaign.

## 🚀 Key Features

### 🧠 Intelligent Filtering & Auditing
*   **Smart Directory Exclusion:** Automatically filters out noise like Indeed, Glassdoor, Gov sites, and huge national brands.
*   **Fuzzy Logic Matching:** Uses token-based matching for H1 tags to avoid false negatives.
*   **Anti-Bot Handling:** Intelligently detects firewalls (403 Forbidden). It **SKIPS** these sites rather than marking them as "Broken."

### 🕷️ Deep Crawling
*   **Mandatory Email Detection:** If an email isn't found on the homepage, the agent intelligently hunts for a "Contact Us" page, navigates there, and extracts the email.
*   **Heuristic Scoring:** Scores links to find the best contact page (e.g., prefers `/contact-us` over `/about`).
*   **Junk Filtering:** Filters out system emails (`sentry@`, `noreply@`) and file extensions that look like emails (`image@2x.png`).

### 🤖 Generative AI Campaigns
*   **Gap Analysis:** Compares the prospect's Google Business Profile (GBP) reputation against their website health.
*   **3-Stage Campaign:** Instead of a single pitch, the agent generates:
    1.  **Email 1 (The Hook):** Personalized observation based on their specific audit gap.
    2.  **Email 2 (Value):** Educational explanation of why the error matters.
    3.  **Email 3 (Breakup):** A professional closing email.

## Prerequisites
*   Python 3.10+
*   SerpApi Account & API Key
*   Google Gemini API Key (AI Studio)

## Installation

1.  **Clone the repository:**
    ```bash
    git clone git@github.com:Rafa-romero-dev/seo-agent.git
    cd seo-agent
    ```

2.  **Install dependencies:**
    ```bash
    pip install pandas serpapi requests beautifulsoup4 python-dotenv google-generativeai
    ```

3.  **Environment Setup:**
    Create a `.env` file in the project root:
    ```env
    SERPAPI_API_KEY="YOUR_SERPAPI_KEY_HERE"
    GEMINI_API_KEY="YOUR_GEMINI_KEY_HERE"
    ```

4.  **Target Configuration:**
    Edit `serp_config.py` to define your niche:
    ```python
    CITIES = ["Midland, Texas, United States"]
    KEYWORDS = ["mobile diesel mechanic"]
    ```

## Usage

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
2.  **Clean:** Removes directories, government sites, and PDFs.
3.  **Enrich:** Fetches the Top Competitor's GBP stats (Reviews/Rating).
4.  **Audit & Crawl:** 
    *   Checks H1, Meta, and Server Status.
    *   **Crawler Logic:** If no email is on the home page, finds the Contact URL, hops to it, and scrapes the email.
5.  **Analyze:** Categorizes the lead (e.g., "Hidden Gem" vs "Healthy Site").
6.  **Generate:** Sends the profile to **Google Gemini (2.5 Flash)** to write the custom email sequence.
7.  **Export:** Saves a CSV ready for import into cold email tools.

## Output Files

*   **`MapWinners_Campaign_YYYYMMDD.csv`**:
    *   Contains only Actionable Leads with Emails.
    *   Columns: `Prospect_Name`, `Email_Address`, `Subject_1`, `Body_1`, `Subject_2`, `Body_2`, `Subject_3`, `Body_3`.
    *   *Ready for import into Instantly.ai / Lemlist.*

*   **`SEO_Detailed_Audit_YYYYMMDD.xlsx`**:
    *   Contains ALL audited sites (including those with missing emails for manual review).
    *   Includes full technical details.
