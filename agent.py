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
from requests.exceptions import RequestException

# Load environment variables from .env file
load_dotenv()

# List of common User-Agents to rotate (Prevents 403 Forbidden errors)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/605.1.15",
]

# Domains to filter out
DIRECTORY_DOMAINS = [
    'yelp.com', 'google.com', 'facebook.com', 'linkedin.com', 'yellowpages.com', 
    'mapquest.com', 'angieslist.com', 'thumbtack.com', 'bbb.org', 'nextdoor.com',
    'porch.com', 'cargurus.com', 'mechanicadvisor.com', 'repairpal.com'
]

# Global data list to store results
results_data = []

def serpapi_extractor(keyword, city, start_page, end_page, api_key, max_api_retries=3):
    """
    Fetches SERP data from Google using the SerpApi with retry logic.
    """
    extracted_results = []
    start_index = (start_page - 1) * 10 
    
    for current_rank in range(start_index, (end_page * 10), 10):
        
        for attempt in range(max_api_retries):
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
                    error_msg = results['error']
                    # Check for rate limit or transient errors
                    if any(err in error_msg.lower() for err in ["rate limit", "internal server error"]):
                        print(f"-> SERP API Transient Error: {error_msg}. Retrying in {2**attempt}s...")
                        time.sleep(2 ** attempt + random.uniform(0, 1))
                        continue # Go to next attempt
                    else:
                        print(f"-> SERP API Fatal Error: {error_msg}")
                        return extracted_results # Stop extraction
                
                organic_results = results.get("organic_results", [])
                
                if not organic_results and current_rank == start_index:
                    print(f"-> No results found for this query.")
                    break # Break retry loop, then break rank loop
                elif not organic_results:
                    print(f"-> No more results found after rank {current_rank}. Stopping.")
                    break # Break retry loop, then break rank loop

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
                            'H1_Audit_Result': '',
                            'NAP_Audit_Result': '',
                            'Schema_Issue': '',
                            'Error_Status': '',
                            'Company_Name': ''
                        })
                
                print(f"-> Fetched 10 results starting at rank {current_rank}.")
                time.sleep(random.uniform(1, 3)) 
                break # Success, break retry loop

            except Exception as e:
                print(f"-> Critical API Request Error: {e.__class__.__name__}. Retrying in {2**attempt}s...")
                time.sleep(2 ** attempt + random.uniform(0, 1))
        
        else: # Runs if the inner loop completes without break (all retries failed)
            print(f"-> Max API retries exceeded for rank {current_rank}. Moving to next query.")
            break # Break rank loop
            
    return extracted_results

def clean_and_deduplicate(raw_data):
    """
    Converts raw list data to a DataFrame, filters directories, normalizes URLs, and removes duplicates.
    """
    if not raw_data:
        print("No data to clean.")
        return pd.DataFrame()
    
    df = pd.DataFrame(raw_data)
    
    def normalize_url(url):
        if not isinstance(url, str):
            return url
        # Enhanced normalization
        url = url.replace("https://", "").replace("http://", "")
        url = url.split('#')[0].split('?')[0] # Remove fragments and query strings
        url = url.rstrip('/')
        if url.startswith("www."):
            url = url[4:]
        return url
        
    df['Normalized_URL'] = df['URL'].apply(normalize_url)
    
    def is_directory(url):
        return any(domain in url.lower() for domain in DIRECTORY_DOMAINS)

    df['Is_Directory'] = df['Normalized_URL'].apply(is_directory)
    # Keep only non-directory sites
    df = df[df['Is_Directory'] == False].drop(columns=['Is_Directory'])
    
    # Deduplicate: Keep the row with the BEST (lowest) Rank
    df_unique = df.sort_values(by=['Normalized_URL', 'Rank'], ascending=[True, True])
    df_cleaned = df_unique.drop_duplicates(subset=['Normalized_URL'], keep='first')
    
    df_cleaned = df_cleaned.drop(columns=['Normalized_URL'])
    
    return df_cleaned

def run_on_page_audit(url: str, keyword: str, city: str, max_retries: int = 3) -> dict: # Increased retries
    """
    Performs quick, non-intrusive SEO checks on a given URL.
    Uses User-Agent rotation and retries.
    """
    # --- AUDIT DATA INITIALIZATION ---
    audit_data = {
        'H1_Audit_Result': 'Fail: H1 Missing or Empty',
        'Title_Status': 'Fail: Title Not Found',
        'Meta_Desc_Status': 'Fail: Missing Meta Description',
        'NAP_Audit_Result': 'Fail: NAP Info Not Found/Clear',
        'Schema_Issue': 'Fail: No LocalBusiness Schema Found',
        'Robots_Status': 'Pass: Index/Follow',
        'Phone_Number': 'N/A',
        'Company_Name': 'N/A' ,
        'Error_Status': 'Success'
    }
    
    response = None

    for attempt in range(max_retries):
        try:
            # --- ROTATE USER AGENT & SLOWER RETRY SLEEP ---
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            # Use longer, randomized sleep before retries
            if attempt > 0:
                # Slower, randomized sleep for HTTP retries
                sleep_time = random.uniform(3, 5) 
                print(f"-> Retrying in {sleep_time:.1f}s...")
                time.sleep(sleep_time)

            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status() 
            
            # If we get here, the request was successful (200 OK)
            break
        
        except RequestException as e:
            error_class_name = e.__class__.__name__
            print(f"-> Error on {url} (Attempt {attempt + 1}/{max_retries}): {error_class_name}. Retrying...")
            
            if attempt == max_retries - 1:
                audit_data['H1_Audit_Result'] = f"Error: Request Failed ({error_class_name})"
                audit_data['Error_Status'] = f"Error: {error_class_name}"
                return audit_data
            
    # --- PARSING LOGIC (Only runs if request succeeded) ---
    try:
        if response and response.content:
            # Explicitly check for content to avoid errors on empty responses
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # --- Company Name Extraction ---
            if soup.title and soup.title.string:
                title_text = soup.title.string.strip()
                # Use a robust split based on common separators
                company_name_part = re.split(r'[-|:-]', title_text)[0].strip()
                audit_data['Company_Name'] = company_name_part or 'N/A'
                
                # --- Title Check ---
                if len(title_text) < 10 or "Home" in title_text or "Default" in title_text:
                    audit_data['Title_Status'] = "Fail: Weak/Default Title Tag"
                else:
                    audit_data['Title_Status'] = f"Pass: {title_text[:30]}..."
            else:
                audit_data['Title_Status'] = "Fail: Missing Title Tag"

            # --- Meta Description Check ---
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                desc_content = meta_desc['content'].strip()
                if len(desc_content) > 20:
                    audit_data['Meta_Desc_Status'] = f"Pass: {desc_content[:30]}..."
                else:
                    audit_data['Meta_Desc_Status'] = "Fail: Empty or Very Short Meta Description"
            else:
                audit_data['Meta_Desc_Status'] = "Fail: Missing Meta Description"
            
            # --- H1 Check (Keyword Relevance) ---
            h1_tag = soup.find('h1')
            if h1_tag:
                h1_text = h1_tag.get_text(strip=True)
                if h1_text:
                    # Check if the keyword or city is mentioned (Case insensitive)
                    if keyword.lower() in h1_text.lower() or city.lower() in h1_text.lower():
                        audit_data['H1_Audit_Result'] = f"Pass: {h1_text[:50]}..."
                    else:
                        audit_data['H1_Audit_Result'] = f"Fail: Irrelevant H1 ({h1_text[:30]}...)"
                # If H1 tag exists but is empty, it remains 'Fail: H1 Missing or Empty' from initialization
            
            # --- NAP Check (Extraction + Status) ---
            # Looks for phone number OR common address markers (St, Rd, Ave, Zip Code)
            phone_regex = r"(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})"
            nap_regex = phone_regex + r"|(\b\d{5}(?:-\d{4})?\b)|(\b(Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Suite|Unit)\b)"
            
            search_text = soup.body.get_text() if soup.body else soup.get_text()
            
            # 1. NAP Status Check
            if re.search(nap_regex, search_text, re.IGNORECASE):
                audit_data['NAP_Audit_Result'] = 'Pass: Address/Phone Found'
                # 2. NAP Data Extraction
                phone_match = re.search(phone_regex, search_text)
                if phone_match:
                    audit_data['Phone_Number'] = phone_match.group(0).strip()
            else:
                footer = soup.find('footer')
                if footer and re.search(nap_regex, footer.get_text(), re.IGNORECASE):
                    audit_data['NAP_Audit_Result'] = 'Pass: Address/Phone Found (in footer)'

            # --- Schema Markup Check ---
            schema_scripts = soup.find_all('script', type='application/ld+json')
            for script in schema_scripts:
                # Use script.string to safely access content
                if script.string and ('LocalBusiness' in script.string or 'Organization' in script.string):
                    audit_data['Schema_Issue'] = 'Pass: LocalBusiness/Organization Schema Found'
                    break
            
            # --- Meta Robots Tag Check ---
            meta_robots = soup.find('meta', attrs={'name': 'robots'})
            if meta_robots and meta_robots.get('content'):
                content = meta_robots['content'].lower()
                if 'noindex' in content:
                    audit_data['Robots_Status'] = 'Fail: NOINDEX tag found'
                elif 'nofollow' in content:
                    audit_data['Robots_Status'] = 'Fail: NOFOLLOW tag found'
            
            
    except Exception as e:
        # Handle parsing failures gracefully
        error_class_name = e.__class__.__name__
        audit_data['H1_Audit_Result'] = f"Error: Parsing Failure ({error_class_name})"
        audit_data['Error_Status'] = f"Error: Parsing Failure ({error_class_name})"
    
    return audit_data

def serpapi_gbp_extractor(keyword, city, api_key, max_api_retries=3) -> dict:
    """
    Fetches GBP data (Rating, Reviews) from the Local Pack using SerpApi.
    Returns a dictionary of extracted GBP data or default values.
    """
    
    # Use only 1 page (start=0) for Local Pack, as results are contained on page 1.
    params = {
        "api_key": api_key,
        "engine": "google",
        "q": f"{keyword} {city}",
        "location": city,
        "hl": "en",
        "start": 0,
        "num": 10  # num is irrelevant for local pack, but required
    }
    
    gbp_data = {
        'GBP_Place_ID': '',
        'GBP_Rating': 0.0,
        'GBP_Review_Count': 0
    }
    
    for attempt in range(max_api_retries):
        try:
            search = GoogleSearch(params)
            results = search.get_dict()
            
            if 'error' in results:
                print(f"-> GBP API Error (Attempt {attempt + 1}): {results['error']}")
                if attempt < max_api_retries - 1:
                    time.sleep(2 ** attempt + random.uniform(0, 1))
                    continue
                return gbp_data

            local_results = results.get("local_results", [])
            
            if local_results:
                # Iterate through local results to find the most relevant one, 
                # or just use the first few as proxies for the local competitive landscape
                for result in local_results:
                    # We will only process the first result as a baseline for the target query
                    gbp_data['GBP_Place_ID'] = result.get('place_id', '')
                    gbp_data['GBP_Rating'] = result.get('rating', 0.0)
                    # Convert review count string " (100)" to integer
                    reviews_str = result.get('reviews', ' (0)').replace('(', '').replace(')', '').strip()
                    try:
                        gbp_data['GBP_Review_Count'] = int(reviews_str)
                    except ValueError:
                        gbp_data['GBP_Review_Count'] = 0
                    
                    # For this step, we just grab the first one found to indicate the query's GBP environment
                    break 

            time.sleep(random.uniform(1, 2))
            return gbp_data
            
        except Exception as e:
            print(f"-> Critical GBP Request Error: {e.__class__.__name__}. Retrying...")
            if attempt < max_api_retries - 1:
                time.sleep(2 ** attempt + random.uniform(0, 1))
                continue
            return gbp_data
    
    return gbp_data

def _get_rating_score(rating, reviews) -> str:
    """Classifies the GBP status for pitch generation."""
    if rating >= 4.5 and reviews >= 50:
        return "Strong" # Winning on reputation
    elif rating >= 4.0 and reviews >= 10:
        return "Decent" # Room for improvement
    elif rating > 0 and reviews > 0:
        return "Weak" # Low volume or poor score
    else:
        return "Missing" # No rating/reviews found
    
def generate_sales_pitch(row):
    """
    Generates a personalized, concise sales pitch based on audit failures AND GBP performance.
    """
    pitch_points = []
    intro_statement = (
        "Based on an **AI-powered analysis** of your site and local market, we found the following actionable gap(s): "
    )
    
    # --- AUDIT ANALYSIS ---
    h1_status = row['H1_Audit_Result']
    nap_status = row['NAP_Audit_Result']
    robots_status = row.get('Robots_Status', 'Pass: Index/Follow')

    # --- GBP ANALYSIS ---
    gbp_rating = row.get('GBP_Rating', 0.0)
    gbp_reviews = row.get('GBP_Review_Count', 0)
    gbp_score = _get_rating_score(gbp_rating, gbp_reviews)
    
    # --- 1. CRITICAL FAILURES (Non-Negotiable Fixes) ---
    if row.get('Error_Status') != 'Success':
          pitch_points.append(
            f"Your website returned an error ({row.get('Error_Status').split('(')[0].strip()}) when we tried to audit it. This suggests a potential **server or site health issue** that needs immediate attention."
        )
    if "Request Failed" in row.get('Error_Status') or "Parsing Failure" in row.get('Error_Status'):
        return intro_statement + " | ".join(pitch_points)

    if robots_status.startswith("Fail: NOINDEX"):
        pitch_points.append(
            "Critical Issue: Your homepage has a **'NOINDEX' tag**, meaning Google is blocked from showing you in search results! This is the most urgent fix."
        )
    
    # --- 2. GBP-ENHANCED PITCHES (Leveraging Social Proof) ---
    seo_failure_count = sum([
        1 if h1_status.startswith("Fail") else 0,
        1 if nap_status.startswith("Fail") else 0
    ])

    if seo_failure_count >= 1:
        if gbp_score == "Strong":
            # Target 1: Winning on Reputation, Losing on Website (Huge Upside)
            pitch_points.append(
                f"You have a **strong GBP reputation ({gbp_rating} stars from {gbp_reviews} reviews)**, which is fantastic. However, your website's fundamentals (H1/NAP) are holding back those customers from converting once they click through. We can double your lead volume."
            )
        elif gbp_score in ["Weak", "Missing"]:
            # Target 2: Losing on Both Fronts (Major Opportunity)
            pitch_points.append(
                f"Your website has fundamental SEO gaps (H1/NAP), and your **GBP profile is weak** ({gbp_rating} stars from {gbp_reviews} reviews). By fixing your website AND implementing a review generation system, you could dominate the Map Pack."
            )

    # If no major SEO pitch was made (i.e., SEO is good but GBP is weak)
    if not pitch_points and gbp_score in ["Weak", "Missing"]:
        pitch_points.append(
            f"Your website is well-optimized, but your **GBP profile is weak** ({gbp_rating} stars from {gbp_reviews} reviews). We can focus on generating reviews to secure the Map Pack and maximize the power of your current rankings."
        )

    # --- 3. STANDARD SEO GAPS (Only if not already covered by GBP pitch) ---
    if not any("H1" in p for p in pitch_points):
        if "Irrelevant" in h1_status:
             pitch_points.append(
                "Your main headline (H1) doesn't mention your core service or city. Google relies on this to rank you. Fixing this is the **fastest way to improve visibility**."
            )
        elif "Missing" in h1_status:
            pitch_points.append(
                "Missing a crucial H1 heading on your homepage! Search engines rely on this to know what you do. This is the **fastest way to improve your visibility**."
            )
    
    if not any("NAP" in p for p in pitch_points) and nap_status.startswith("Fail"):
        pitch_points.append(
            "Can't find your phone number or address quickly. This hurts user trust and is a **critical fix for local SEO rankings** (the 'Map Pack')."
        )
        
    # --- 4. SECONDARY GAPS ---
    if row['Title_Status'].startswith("Fail"):
        pitch_points.append(f"Your title tag is {row['Title_Status'].split(':')[1].strip().lower()}. This is your first impression to Google and potential customers.")
    
    if row['Meta_Desc_Status'].startswith("Fail: Missing"):
        pitch_points.append("You are missing a crucial Meta Description, leaving Google to write its own (often poor) one for your search results.")

    schema_status = row['Schema_Issue']
    if schema_status.startswith("Fail"):
        pitch_points.append(
            "You are missing Structured Data (Schema Markup). Adding 'LocalBusiness' schema is essential for Google to understand your services and is a huge factor for Map visibility."
        )

    if pitch_points:
        return intro_statement + " | ".join(pitch_points)
    else:
        return intro_statement + "None! Your site is strong. We specialize in advanced strategies (authority building, conversion rate optimization) to take you from #10 to #1."
    
def create_final_report(df):
    """
    Applies the sales pitch logic, cleans up columns, and exports the final report
    into two formats:
    1. A simplified CSV for cold outreach tools (Instantly.ai).
    2. A detailed XLSX for human review and internal data storage.
    """
    
    if df.empty:
        print("Report not generated: No unique prospects found.")
        return
        
    # 1. Generate the Sales Pitch (if not already done)
    if 'Sales_Pitch' not in df.columns:
        print("-> Generating sales pitches...")
        df['Sales_Pitch'] = df.apply(generate_sales_pitch, axis=1)
    
    # 2. Add an 'Actionable' filter column
    df['Actionable_Target'] = df['Sales_Pitch'].apply(lambda x: 'YES' if 'Fail' in x or 'Error' in x or 'Critical' in x else 'NO')

    # --- CSV EXPORT FOR INSTANTLY.AI (Simplified Structure) ---
    print("\n--- EXPORTING INSTANTLY.AI CSV ---")
    
    # Create clean columns for seamless import (no spaces/special chars in names)
    df['Prospect_Name'] = df['Company_Name'].apply(lambda x: x if x and x != 'N/A' else 'Client')
    df['Final_Pitch'] = df['Sales_Pitch']
    df['Website_URL'] = df['URL']
    df['Rank_Found'] = df['Rank']
    
    instantly_columns = [
        'Actionable_Target',
        'Prospect_Name', 
        'Website_URL',
        'Final_Pitch',
        'Keyword',
        'City',
        'Rank_Found',
        'Phone_Number'
    ]
    
    # Filter only actionable targets for the cold email list
    df_instantly = df[df['Actionable_Target'] == 'YES'][instantly_columns]
    
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    csv_filename = f"Instantly_Import_Actionable_{timestamp}.csv"
    
    df_instantly.to_csv(csv_filename, index=False)
    print(f"File: {csv_filename} (Ready for Instantly.ai/Outreach Import)")

    # --- EXCEL EXPORT FOR HUMAN REVIEW (Detailed Audit Report) ---
    print("\n--- EXPORTING DETAILED AUDIT XLSX ---")

    # Full set of detailed columns for the human report
    detailed_columns = [
        'Actionable_Target',
        'Rank',
        'Company_Name',
        'Keyword',
        'City',
        'URL',
        'Title',
        'Final_Pitch',
        'H1_Audit_Result',
        'NAP_Audit_Result',
        'Phone_Number',
        'Robots_Status',
        'Title_Status',
        'Meta_Desc_Status',
        'Schema_Issue',
        'GBP_Rating',
        'GBP_Review_Count',
        'Error_Status',
        'Snippet',
        'Target_Query'
    ]
    
    # Ensure all columns exist before selecting
    for col in detailed_columns:
        if col not in df.columns:
            df[col] = ''
    
    df_report_full = df[detailed_columns]
    
    xlsx_filename = f"SEO_Detailed_Audit_{timestamp}.xlsx"
    df_report_full.to_excel(xlsx_filename, index=False)

    # --- FINAL SUMMARY ---
    actionable_count = len(df_instantly)

    print(f"\n[SUCCESS] Final report created!")
    print(f"Human Readable File: {xlsx_filename}")
    print(f"Total Unique Prospects Audited: {len(df_report_full)}")
    print(f"Total Actionable Leads (Ready for Outreach): {actionable_count}")
    print("The script is complete. Run finished.")

if __name__ == '__main__':
    
    # 1. Load API Key
    SERPAPI_API_KEY = os.environ.get("SERPAPI_API_KEY")
    if not SERPAPI_API_KEY:
        print("\n[ERROR] SERPAPI_API_KEY environment variable not found.")
        print("Please set the variable before running the script (e.g., export SERPAPI_API_KEY='YOUR_KEY').")
        exit()

    # 2. Parse Command-Line Arguments
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
        
        # 4.5 GBP Data Collection
        print("\n--- STARTING GBP DATA COLLECTION ---")
        gbp_place_ids = []
        gbp_ratings = []
        gbp_reviews = []
        
        # Run GBP audit once per Keyword/City combination, then apply to all rows for that query
        for city in CITIES:
            for keyword in KEYWORDS:
                print(f"-> Fetching GBP data for: '{keyword} {city}'")
                
                # Fetch GBP data
                gbp_results = serpapi_gbp_extractor(keyword, city, SERPAPI_API_KEY)
                
                # Identify all rows belonging to the current keyword/city pair
                mask = (cleaned_df['Keyword'] == keyword) & (cleaned_df['City'] == city)
                
                # Apply the fetched GBP data to all matching rows
                cleaned_df.loc[mask, 'GBP_Place_ID'] = gbp_results['GBP_Place_ID']
                cleaned_df.loc[mask, 'GBP_Rating'] = gbp_results['GBP_Rating']
                cleaned_df.loc[mask, 'GBP_Review_Count'] = gbp_results['GBP_Review_Count']
                
        print("--- GBP DATA COLLECTION COMPLETE ---")
        
        # 5. On-Page Auditor
        if not cleaned_df.empty:
            print("\nReady to begin On-Page Auditing of unique prospects.")
            
            # Prepare lists for audit results (must match keys in run_on_page_audit)
            audit_results_list = []
            
            # Loop through rows
            for index, row in cleaned_df.iterrows():
                url = row['URL']
                print(f"  -> Auditing {index+1}/{cleaned_df.shape[0]}: {url}") 
                
                audit_results = run_on_page_audit(url, row['Keyword'], row['City'])
                
                # Append the results dictionary along with the original row data
                # Merge the audit results back into the DataFrame row
                for key, value in audit_results.items():
                    # Use a conditional assignment to set the column data
                    # This is slightly more verbose but avoids pre-creating 
                    # dozens of empty lists and appending to them one by one.
                    cleaned_df.loc[index, key] = value
                
                time.sleep(random.uniform(0.5, 1.5))
                
            print("\n--- ON-PAGE AUDIT COMPLETE ---")
                
        # 6. Data Consolidation & Reporting
        create_final_report(cleaned_df)
            
    except Exception as e:
        print(f"\n[FATAL ERROR] An error occurred: {e}")
        
    finally:
        pass