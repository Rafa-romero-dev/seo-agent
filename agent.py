import os
from dotenv import load_dotenv
import argparse
import time
import random
import google.generativeai as genai

# Import from new modules
from serp_config import CITIES, KEYWORDS
from modules.serp_client import serpapi_extractor, serpapi_gbp_extractor
from modules.utils import clean_and_deduplicate
from modules.crawler import run_on_page_audit
from modules.reporting import create_final_report

# Load environment variables from .env file
load_dotenv()

# Initialize Gemini Client (Global configuration for modules that might need it implicitly, 
# though ai_engine handles its own model instantiation, configuring it globally is good practice)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    print("[WARNING] GEMINI_API_KEY not found in .env")

# Global data list to store results
results_data = []

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
            
            cleaned_df['Email_Address'] = 'N/A'
            cleaned_df['H1_Audit_Result'] = 'Fail: Not Audited'
            cleaned_df['NAP_Audit_Result'] = 'Fail: Not Audited'
            
            # Loop through rows
            for index, row in cleaned_df.iterrows():
                url = row['URL']
                print(f"-> Auditing {index+1}/{cleaned_df.shape[0]}: {url}") 
                
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