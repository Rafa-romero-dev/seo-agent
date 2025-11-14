import os
from dotenv import load_dotenv
import pandas as pd
from serpapi import GoogleSearch
import requests
from bs4 import BeautifulSoup
from serp_config import CITIES, KEYWORDS
import time
import random
import re
import argparse

# Load environment variables from .env file
load_dotenv()

# Global data list to store results
results_data = []

def serpapi_extractor(keyword, city, start_page, end_page, api_key):
    """
    Fetches SERP data from Google using the SerpApi.
    ... (docstring for Args and Returns)
    """
    extracted_results = []
    
    # Calculate starting index (st) for the first page
    start_index = (start_page - 1) * 10 
    
    # Iterate through the pages/rank offsets
    for current_rank in range(start_index, (end_page * 10), 10):
        # The SerpApi parameters
        params = {
            "api_key": api_key,
            "engine": "google",
            "q": f"{keyword} {city}",
            "location": city,
            "hl": "en",
            "start": current_rank,
            "num": 10
        }
        
        try:
            search = GoogleSearch(params)
            results = search.get_dict()
            
            if 'error' in results:
                print(f"  -> SERP API Error: {results['error']}")
                break
                
            organic_results = results.get("organic_results", [])
            
            if not organic_results:
                print(f"  -> No more results found after rank {current_rank}. Stopping.")
                break

            for i, result in enumerate(organic_results):
                rank = current_rank + i + 1
                
                if 'link' in result and 'google.com' not in result['link']:
                    extracted_results.append({
                        'Rank': rank,
                        'URL': result['link'],
                        'Title': result['title'],
                        'Snippet': result.get('snippet', 'No Snippet Found'),
                        'Keyword': keyword,
                        'City': city,
                        'Target_Query': f"{keyword} {city}",
                        'On_Page_H1_Status': '',
                        'On_Page_NAP_Issue': ''
                    })
            
            # Note: We can't access credit info easily without another call or parsing,
            # so we'll simplify the printout for the blog
            print(f"  -> Fetched 10 results starting at rank {current_rank}.")
            
            time.sleep(random.uniform(1, 3)) 
            
        except Exception as e:
            print(f"  -> Critical API Request Error: {e}")
            break
            
    return extracted_results

def clean_and_deduplicate(raw_data):
    """
    Converts raw list data to a DataFrame, normalizes URLs, and removes duplicates.
    ... (docstring for Args and Returns)
    """
    
    if not raw_data:
        print("No data to clean.")
        return pd.DataFrame()
    
    df = pd.DataFrame(raw_data)
    
    def normalize_url(url):
        if not isinstance(url, str):
            return url
        url = url.replace("https://", "").replace("http://", "")
        url = url.rstrip('/')
        if url.startswith("www."):
            url = url[4:]
        return url
        
    df['Normalized_URL'] = df['URL'].apply(normalize_url)
    
    # Deduplicate: Keep the row with the BEST (lowest) Rank
    df_unique = df.sort_values(by=['Normalized_URL', 'Rank'], ascending=[True, True])
    df_cleaned = df_unique.drop_duplicates(subset=['Normalized_URL'], keep='first')
    
    df_cleaned = df_cleaned.drop(columns=['Normalized_URL'])
    
    return df_cleaned

def run_on_page_audit(url: str, max_retries: int = 3) -> dict:
    """
    Performs quick, non-intrusive SEO checks (H1, basic NAP) on a given URL.

    Args:
        url (str): The prospect's website URL.
        max_retries (int): Maximum number of attempts for the request.

    Returns:
        dict: Status of key SEO elements (H1, NAP).
    """

    audit_data = {
        'H1_Status': 'Fail: H1 Missing or Empty',
        'NAP_Issue': 'Fail: NAP Info Not Found/Clear'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = None

    for attempt in range(max_retries):
        try:
            # Note: The timeout is part of the request logic, not the retry logic
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            break
        
        except requests.exceptions.RequestException as e:
            # Handle all connection-level issues (Timeout, ConnectionError, HTTPError, etc.)
            print(f"  -> Error on {url} (Attempt {attempt + 1}/{max_retries}): {e.__class__.__name__}. Retrying...")
            time.sleep(1)
            
            # If this was the final attempt, the error will be handled outside the loop.
            if attempt == max_retries - 1:
                # Store the final error for handling outside the loop
                audit_data['H1_Status'] = f"Error: Request Failed ({e.__class__.__name__})"
                return audit_data

    try:
        # Check if response object exists and is successful (200, no raise_for_status() error)
        # Note: raise_for_status() is handled in the loop, but this ensures a response was received.
        if response and response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 1. H1 Check
            h1_tag = soup.find('h1')
            h1_text = h1_tag.get_text(strip=True) if h1_tag else ''
            if h1_text:
                audit_data['H1_Status'] = f"Pass: {h1_text[:50]}..."
            
            # 2. NAP Check
            phone_patterns = r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}" # Common phone number regex
            
            # Search the entire body text
            search_text = soup.body.get_text() if soup.body else soup.get_text()
            if re.search(phone_patterns, search_text):
                audit_data['NAP_Issue'] = 'Pass: Phone Number Found'
            else:
                # Check the footer specifically
                footer = soup.find('footer')
                if footer and re.search(phone_patterns, footer.get_text()):
                    audit_data['NAP_Issue'] = 'Pass: Phone Number Found (in footer)'

        elif response and response.status_code != 200:
            # Handle final non-200 status codes not caught by the loop's raise_for_status 
            audit_data['H1_Status'] = f"Error: HTTP {response.status_code}"
            
    except Exception as e:
        # This catches errors during parsing (e.g., in BeautifulSoup, re.search, etc.)
        audit_data['H1_Status'] = f"Error: Parsing/Internal Failure ({e.__class__.__name__})"
    
    return audit_data

def generate_sales_pitch(row):
    """
    Generates a personalized, concise sales pitch based on audit failures.
    
    Args:
        row (pd.Series): A row from the audited DataFrame.
        
    Returns:
        str: The full, actionable sales pitch text.
    """
    
    pitch_points = []
    
    # Check 1: H1 Failure (High-Impact SEO Issue)
    h1_status = row['H1_Audit_Result']
    if h1_status.startswith("Fail"):
        pitch_points.append(
            "Missing a crucial H1 heading on your homepage! Search engines rely on this to know what you do. This is the **fastest way to improve your visibility**."
        )
    
    # Check 2: NAP Failure (Trust & Local SEO Issue)
    nap_status = row['NAP_Audit_Result']
    if nap_status.startswith("Fail"):
        pitch_points.append(
            "Can't find your phone number or address quickly. This hurts user trust and is a **critical fix for local SEO rankings** (the 'Map Pack')."
        )
        
    # Check 3: General Failure/Error
    if "Error" in h1_status:
        pitch_points.append(
            f"Your website returned an error ({h1_status.split(':')[-1].strip()}) when we tried to audit it. This suggests a potential **server or site health issue** that needs immediate attention."
        )

    # Compile the final pitch
    if pitch_points:
        return " | ".join(pitch_points)
    else:
        return "Audit Passed! Top-tier SEO, requires a more advanced audit."
    
def create_final_report(df):
    """
    Applies the sales pitch logic, cleans up columns, and exports the final report.
    
    Args:
        df (pd.DataFrame): The fully audited DataFrame.
    """
    
    if df.empty:
        print("Report not generated: No unique prospects found.")
        return
        
    # 1. Generate the Sales Pitch
    print("  -> Generating sales pitches...")
    df['Sales_Pitch'] = df.apply(generate_sales_pitch, axis=1)
    
    # 2. Add an 'Actionable' filter column (only target those with failures)
    df['Actionable_Target'] = df['Sales_Pitch'].apply(lambda x: 'YES' if 'Fail' in x or 'Error' in x else 'NO')

    # 3. Reorder and Select Final Columns for the Report
    final_columns = [
        'Actionable_Target', # Sort by this first
        'Rank',
        'Keyword',
        'City',
        'URL',
        'Title',
        'Sales_Pitch',
        'H1_Audit_Result',
        'NAP_Audit_Result',
        'Snippet',
        'Target_Query'
    ]
    
    df_report = df[final_columns]
    
    # 4. Sort and Export
    df_report = df_report.sort_values(by=['Actionable_Target', 'Rank'], ascending=[False, True])
    
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    filename = f"SEO_Prospect_Report_{timestamp}.csv"
    
    df_report.to_csv(filename, index=False)
    
    print(f"\n[SUCCESS] Final report created!")
    print(f"File: {filename}")
    print(f"Total Actionable Leads: {df_report['Actionable_Target'].value_counts().get('YES', 0)}")
    print("The script is complete. Run finished.")

if __name__ == '__main__':
    
    # 1. Load API Key
    SERPAPI_API_KEY = os.environ.get("SERPAPI_API_KEY")
    if not SERPAPI_API_KEY:
        print("\n[ERROR] SERPAPI_API_KEY environment variable not found.")
        print("Please set the variable before running the script (e.g., export SERPAPI_API_KEY='YOUR_KEY').")
        exit()

    # 2. Parse Command-Line Arguments for Page Range
    parser = argparse.ArgumentParser(description="SEO Prospect Agent: Scrapes SERPs and audits prospects.")
    parser.add_argument('start_page', type=int, help='The starting Google SERP page number (e.g., 2).')
    parser.add_argument('end_page', type=int, help='The ending Google SERP page number (e.g., 5).')
    args = parser.parse_args()
    
    start_page = args.start_page
    end_page = args.end_page
    
    if start_page < 1 or end_page < 1 or start_page > end_page:
        print("[ERROR] Invalid page range. Start page must be >= 1 and Start page <= End page.")
        exit()
    
    try:
        print("\n--- STARTING SERPAPI EXTRACTION ---")
        
        # 3. SerpApi Extractor Logic
        for city in CITIES:
            for keyword in KEYWORDS:
                
                print(f"\n[QUERY] Targeting: '{keyword} {city}' (Pages {start_page}-{end_page})")
                
                harvested_data = serpapi_extractor(
                    keyword, 
                    city, 
                    start_page, 
                    end_page, 
                    SERPAPI_API_KEY
                )
                results_data.extend(harvested_data)
                
        
        print("\n--- SERP EXTRACTION COMPLETE ---")
        print(f"Total raw results collected: {len(results_data)}")
        
        # 4. Data Cleaning and Deduplication
        print("\n--- STARTING DATA CLEANUP ---")
        cleaned_df = clean_and_deduplicate(results_data) 
        print(f"Total unique URLs after cleaning: {cleaned_df.shape[0]}")
        
        # 4.5. Filter out Directory/Social Sites
        directory_domains = ['yelp.com', 'google.com/maps', 'yellowpages.com', 'facebook.com', 'linkedin.com']
        print("\n--- Filtering Directory/Social Sites ---")
        initial_count = cleaned_df.shape[0]
        
        def is_directory(url):
            return any(domain in url.lower() for domain in directory_domains)

        cleaned_df['Is_Directory'] = cleaned_df['URL'].apply(is_directory)
        cleaned_df_filtered = cleaned_df[cleaned_df['Is_Directory'] == False].drop(columns=['Is_Directory'])

        print(f"Removed {initial_count - cleaned_df_filtered.shape[0]} directory URLs.")
        print(f"Total URLs to audit: {cleaned_df_filtered.shape[0]}")
        
        # 5. On-Page Auditor
        if not cleaned_df_filtered.empty:
            print("\nReady to begin On-Page Auditing of unique prospects.")
            
            # Prepare lists to hold the audit results
            h1_statuses = []
            nap_issues = []
            
            for index, row in cleaned_df_filtered.iterrows():
                url = row['URL']
                # Correct index+1 for readable output
                print(f"  -> Auditing {index+1}/{cleaned_df_filtered.shape[0]}: {url}") 
                
                audit_results = run_on_page_audit(url)
                
                h1_statuses.append(audit_results['H1_Status'])
                nap_issues.append(audit_results['NAP_Issue'])
                
                # Add a small delay between site requests
                time.sleep(random.uniform(0.5, 1.5))
                
            cleaned_df_filtered['H1_Audit_Result'] = h1_statuses
            cleaned_df_filtered['NAP_Audit_Result'] = nap_issues
                
            print("\n--- ON-PAGE AUDIT COMPLETE ---")
                
        # 6. Data Consolidation & Reporting
        create_final_report(cleaned_df_filtered)
            
    except Exception as e:
        print(f"\n[FATAL ERROR] An error occurred: {e}")
        
    finally:
        pass