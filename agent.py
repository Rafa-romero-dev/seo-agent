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
import json
from urllib.parse import urljoin, urlparse
import google.generativeai as genai
from requests.exceptions import RequestException

# Load environment variables from .env file
load_dotenv()

# Initialize Gemini Client
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    print("[WARNING] GEMINI_API_KEY not found in .env")

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
    # --- Job Boards ---
    'indeed', 'glassdoor', 'ziprecruiter', 'simplyhired', 'linkedin', 
    'snagajob', 'monster.com', 'careerbuilder', 'jooble', 'talent.com', 
    'upwork', 'fiverr', 'salary.com', 'lensa', 'postjobfree',

    # --- Government & Official ---
    '.gov', 'texas.gov', 'dot.state', 'fmcsa', 'osha.gov', 'usps.com', 
    'police', 'sheriff', 'cityof', 'countyof',

    # --- Directories & Aggregators ---
    'yelp', 'yellowpages', 'bbb.org', 'mapquest', 'angieslist', 
    'thumbtack', 'nextdoor', 'porch', 'cargurus', 'mechanicadvisor', 
    'repairpal', '4roadservice', 'findtruckservice', 'truckdown', 
    'nttrdirectory', 'truckerguideapp', 'chamberofcommerce', 
    'carfax', 'kbb.com', 'edmunds', 'autotrader', 'superpages', 
    'dexknows', 'whitepages', 'trustpilot', 'groupon', 'local.yahoo',

    # --- Social Media & Big Tech ---
    'facebook', 'instagram', 'tiktok', 'youtube', 'twitter', 'pinterest', 
    'wikipedia', 'reddit', 'medium', 'google', 'apple', 'amazon',

    # --- National Fleet / Rental / Corporate ---
    'uhaul', 'penske', 'budgettruck', 'ryder', 'enterprise', 
    'loves.com', 'pilotflyingj', 'ta-petro', 'flyj', 'hertz', 
    'goodyear', 'firestone', 'pepboys', 'autozone', 'oreillyauto', 
    'advanceautoparts', 'napaonline', 'discounttire', 'michelinman', 
    'safelite', 'aamco', 'meineke', 'jiffylube', 'valvoline', 
    'ford.com', 'chevrolet.com', 'toyota.com', 'honda.com', 'dodge.com'
]

# Filter out "emails" that are actually file names or system addresses
JUNK_EMAIL_EXTENSIONS = ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.js', '.css', '.woff', '.ttf']
JUNK_EMAIL_PREFIXES = ['sentry', 'noreply', 'no-reply', 'hostmaster', 'postmaster', 'webmaster', 'example']
JUNK_EMAIL_DOMAINS = ['wix.com', 'godaddy.com', 'squarespace.com', 'sentry.io', 'wordpress.com', 'google.com', 'yandex.ru', 'example.com']

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
        url_lower = url.lower()
        if any(domain in url_lower for domain in DIRECTORY_DOMAINS):
            return True
        if url_lower.endswith('.pdf'):
            return True
        return False

    df['Is_Directory'] = df['Normalized_URL'].apply(is_directory)
    df = df[df['Is_Directory'] == False].drop(columns=['Is_Directory'])
    
    # Deduplicate: Keep the row with the BEST (lowest) Rank
    df_unique = df.sort_values(by=['Normalized_URL', 'Rank'], ascending=[True, True])
    df_cleaned = df_unique.drop_duplicates(subset=['Normalized_URL'], keep='first')
    
    df_cleaned = df_cleaned.drop(columns=['Normalized_URL'])
    
    return df_cleaned

def extract_emails_from_html(soup):
    """
    Robust extraction: Checks mailto, visible text, and raw HTML.
    Prioritizes business emails over generic ones.
    """
    emails = set()
    
    # 1. Check 'mailto:' links (High Confidence)
    for link in soup.select('a[href^="mailto:"]'):
        email = link.get('href').replace('mailto:', '').split('?')[0].strip()
        if email and '@' in email:
            emails.add(email.lower())

    # 2. Regex Search in Text AND Raw HTML (Catch hidden emails)
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    
    # A. Visible Text
    text_content = soup.get_text(" ", strip=True) 
    found_text = re.findall(email_pattern, text_content)
    emails.update([e.lower() for e in found_text])
    
    # B. Raw HTML (catches values inside input fields or scripts)
    raw_html = str(soup)
    found_raw = re.findall(email_pattern, raw_html)
    emails.update([e.lower() for e in found_raw])

    # 3. Filtering & Prioritization
    valid_emails = []
    
    for email in emails:
        # Filter Junk Extensions
        if any(email.endswith(ext) for ext in JUNK_EMAIL_EXTENSIONS):
            continue
            
        local_part, domain_part = email.split('@')
        
        # Filter Junk Prefixes
        if local_part in JUNK_EMAIL_PREFIXES:
            continue
            
        # Filter Junk Domains
        if domain_part in JUNK_EMAIL_DOMAINS:
            continue
            
        valid_emails.append(email)

    if not valid_emails:
        return []

    # 4. Sorting: Prioritize 'info', 'contact', 'sales', 'office'
    priority_prefixes = ['info', 'contact', 'sales', 'office', 'admin', 'hello', 'service']
    
    def sort_score(e):
        prefix = e.split('@')[0]
        if prefix in priority_prefixes:
            return 0 # High priority
        return 1 # Low priority
        
    valid_emails.sort(key=sort_score)
    
    # Debug Print (See what is happening!)
    print(f"   [DEBUG] Emails Found: {valid_emails}")
    
    return valid_emails

def find_best_contact_url(soup, base_url):
    """
    Scans all links, scores them based on heuristics.
    """
    candidates = []
    high_priority_keywords = ['contact', 'contact-us', 'contact_us', 'contactus', 'reach-us', 'get-in-touch']
    fallback_keywords = ['about', 'about-us', 'about_us']
    
    base_netloc = urlparse(base_url).netloc.replace('www.', '')
    
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        text = a.get_text(" ", strip=True).lower()
        
        if href.startswith(('#', 'javascript:', 'mailto:', 'tel:')) or not href:
            continue
            
        full_url = urljoin(base_url, href)
        parsed_url = urlparse(full_url)
        link_netloc = parsed_url.netloc.replace('www.', '')

        if parsed_url.netloc != "" and base_netloc not in link_netloc:
             continue

        path = parsed_url.path.lower()
        score = 0
        
        # A. URL Path Matches
        if any(kw in path for kw in high_priority_keywords):
            score += 100
            
        # B. Link Text Matches
        if 'contact' in text:
            score += 50
            if len(text) < 20: score += 20
                
        # C. Navigation/Footer Context
        parent_tags = [parent.name for parent in a.parents]
        if 'nav' in parent_tags or 'footer' in parent_tags or 'header' in parent_tags:
            score += 10
            
        # D. Fallback: About Pages
        if any(kw in path for kw in fallback_keywords):
            score += 30
        elif 'about' in text and len(text) < 15:
            score += 20

        if score > 0:
            candidates.append((score, full_url))

    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]
    
    return None

def generate_ai_campaign(row):
    """
    Uses Google Gemini (2.5 Flash) to generate a 3-email sequence.
    Returns a dictionary with Email_1, Email_2, and Email_3.
    """
    # 1. Prepare Data
    company = row.get('Company_Name', 'Business Owner')
    city = row.get('City', 'your city')
    gbp_rating = row.get('GBP_Rating', 0)
    h1_status = row.get('H1_Audit_Result', 'Unknown')
    nap_status = row.get('NAP_Audit_Result', 'Unknown')
    
    # 2. Define the Prompt
    prompt = f"""
    You are a top-tier SEO Sales Copywriter for an agency called "MapWinners". 
    Your goal is to write a 3-email cold outreach sequence for a local business.
    
    PROSPECT DETAILS:
    - Business: {company} in {city}
    - Google Maps Rating: {gbp_rating} stars.
    - Audit Issues: Main Heading (H1): "{h1_status}", Contact Info (NAP): "{nap_status}".
    
    STRATEGY:
    1. Email 1 (The Hook): 
        - If Audit Issues are "Fail": Warn that technical errors are hurting their rankings.
        - If Audit Issues are "Pass": Praise their foundation but warn that they need "Authority/Backlinks" to hit #1.
    2. Email 2 (Value - 3 Days Later): Explain WHY the specific error found (H1 or NAP) kills rankings.
    3. Email 3 (Breakup - 7 Days Later): Gentle reminder.
    
    TONE: Professional, concise, high-value. No fluff.
    
    OUTPUT FORMAT:
    You must output a JSON object with these exact keys:
    {{
        "subject_1": "...", "body_1": "...",
        "subject_2": "...", "body_2": "...",
        "subject_3": "...", "body_3": "..."
    }}
    """

    try:
        # 3. Call Gemini API
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        response = model.generate_content(
            prompt,
            generation_config={"response_mime_type": "application/json"}
        )
        
        return json.loads(response.text)

    except Exception as e:
        print(f"-> Gemini Error: {e}")
        return {
            "subject_1": "Error generating", "body_1": "Error",
            "subject_2": "Error generating", "body_2": "Error",
            "subject_3": "Error generating", "body_3": "Error"
        }

def run_on_page_audit(url: str, keyword: str, city: str, max_retries: int = 3) -> dict: 
    """
    Performs SEO checks AND extracts Email/NAP.
    Includes logic to hop to the Contact page if email is missing.
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
        'Email_Address': 'N/A',
        'Company_Name': 'N/A' ,
        'Error_Status': 'Success'
    }
    
    response = None
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.9'
    }

    # --- 1. REQUEST LANDING PAGE ---
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                sleep_time = random.uniform(3, 5) 
                print(f"   -> Retrying in {sleep_time:.1f}s...")
                time.sleep(sleep_time)

            response = requests.get(url, headers=headers, timeout=15)
            
            # Check for blocking status codes
            if response.status_code in [403, 406, 429, 503]:
                print(f"   -> Blocked by firewall ({response.status_code}) on {url}")
                audit_data['H1_Audit_Result'] = "Error: Bot Blocked" 
                audit_data['Error_Status'] = "Blocked"
                return audit_data
            
            response.raise_for_status() 
            break
        
        except RequestException as e:
            if attempt == max_retries - 1:
                audit_data['Error_Status'] = f"Error: {e.__class__.__name__}"
                audit_data['H1_Audit_Result'] = f"Error: Request Failed ({e.__class__.__name__})"
                return audit_data

    # --- 2. PARSE LANDING PAGE ---
    try:
        if response and response.content:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # --- Company Name Extraction ---
            if soup.title and soup.title.string:
                title_text = soup.title.string.strip()
                company_name_part = re.split(r'[-|:-]', title_text)[0].strip()
                audit_data['Company_Name'] = company_name_part or 'N/A'
                
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
            
            # --- H1 Check (Fuzzy/Token-based Matching) ---
            h1_tag = soup.find('h1')
            if h1_tag:
                h1_text = h1_tag.get_text(strip=True)
                if h1_text:
                    required_words = set(keyword.lower().split() + city.lower().split())
                    stop_words = {'in', 'near', 'the', 'and', 'me', 'us', 'tx', 'texas'}
                    required_words = required_words - stop_words
                    found_words = set(h1_text.lower().split())
                    matches = required_words.intersection(found_words)
                    match_percentage = len(matches) / len(required_words) if required_words else 0
                    
                    if match_percentage >= 0.5:
                        audit_data['H1_Audit_Result'] = f"Pass: {h1_text[:50]}..."
                    else:
                        audit_data['H1_Audit_Result'] = f"Fail: Irrelevant H1 ({h1_text[:30]}...)"
            
            # --- Schema Markup Check ---
            schema_scripts = soup.find_all('script', type='application/ld+json')
            for script in schema_scripts:
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

            # --- NAP & EMAIL EXTRACTION ---
            
            # 1. Phone Number Logic
            phone_regex = r"(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})"
            tel_link = soup.select_one('a[href^="tel:"]')
            
            if tel_link:
                audit_data['NAP_Audit_Result'] = 'Pass: Address/Phone Found'
                audit_data['Phone_Number'] = tel_link.get('href').replace('tel:', '').strip()
            elif re.search(phone_regex, soup.get_text(), re.IGNORECASE):
                audit_data['NAP_Audit_Result'] = 'Pass: Address/Phone Found'
                phone_match = re.search(phone_regex, soup.get_text())
                if phone_match:
                    audit_data['Phone_Number'] = phone_match.group(0).strip()
            else:
                 footer = soup.find('footer')
                 if footer and re.search(phone_regex, footer.get_text(), re.IGNORECASE):
                    audit_data['NAP_Audit_Result'] = 'Pass: Address/Phone Found (in footer)'

            # 2. Email Extraction (Landing Page)
            emails = extract_emails_from_html(soup)
            
            # 3. Contact Page Crawl (If Email Missing)
            if not emails:
                contact_url = find_best_contact_url(soup, url)
                
                if contact_url:
                    if contact_url.rstrip('/') != url.rstrip('/'):
                        print(f"-> Crawling Contact Page: {contact_url}")
                        try:
                            resp_contact = requests.get(contact_url, headers=headers, timeout=10)
                            if resp_contact.status_code == 200:
                                soup_contact = BeautifulSoup(resp_contact.content, 'html.parser')
                                new_emails = extract_emails_from_html(soup_contact)
                                if new_emails:
                                    print(f"-> Found {len(new_emails)} email(s) on Contact page!")
                                    emails = new_emails
                        except Exception:
                            pass

            if emails:
                audit_data['Email_Address'] = emails[0]
                print(f"   + Email Secured: {emails[0]}")
            else:
                print(f"   - No Email Found")
            
    except Exception as e:
        audit_data['Error_Status'] = f"Error: Parsing Failure ({e.__class__.__name__})"
    
    return audit_data

def serpapi_gbp_extractor(keyword, city, api_key, max_api_retries=3) -> dict:
    """
    Fetches the top *competitive* GBP data for a keyword/city query 
    using the dedicated google_local engine.
    """
    gbp_data = {
        'GBP_Place_ID': '',  
        'GBP_Rating': 0.0,
        'GBP_Review_Count': 0
    }
    
    params = {
        "api_key": api_key,
        "engine": "google_local", 
        "q": f"{keyword} {city}",
        "location": city,
        "hl": "en",
        "start": 0,
    }
    
    for attempt in range(max_api_retries):
        try:
            search = GoogleSearch(params)
            results = search.get_dict()
            
            if 'error' in results:
                error_msg = results['error']
                if any(err in error_msg.lower() for err in ["rate limit", "internal server error"]):
                    print(f"-> SERP API Transient Error: {error_msg}. Retrying in {2**attempt}s...")
                    time.sleep(2 ** attempt + random.uniform(0.5, 1.5))
                    continue
                else:
                    print(f"-> SERP API Fatal Error: {error_msg}. Giving up on this query.")
                    return gbp_data

            local_results = results.get("local_results")
            
            if local_results and len(local_results) > 0:
                top_result = local_results[0]
                gbp_data['GBP_Place_ID'] = top_result.get('place_id', '')
                gbp_data['GBP_Rating'] = top_result.get('rating', 0.0)
                
                reviews_raw = top_result.get('reviews')
                
                if reviews_raw:
                    reviews_str = str(reviews_raw).replace('(', '').replace(')', '').strip()
                else:
                    reviews_str = "0"
                    
                try:
                    gbp_data['GBP_Review_Count'] = int(reviews_str)
                except ValueError:
                    gbp_data['GBP_Review_Count'] = 0 
                
                print(f"-> GBP data retrieved (Attempt {attempt+1}): Rating {gbp_data['GBP_Rating']}, Reviews {gbp_data['GBP_Review_Count']}")
                return gbp_data 

            else:
                print(f"-> Local API query successful, but no businesses found for: '{keyword} {city}'.")
                return gbp_data 
            
        except Exception as e:
            error_name = e.__class__.__name__
            print(f"-> General SerpApi Error: {error_name}. Retrying in {2**attempt}s...")
            time.sleep(2 ** attempt + random.uniform(0.5, 1.5))
            continue
            
    print(f"-> GBP data collection failed after {max_api_retries} retries.")
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
    Generates a personalized, narrative-style sales pitch based on:
    1. Critical Errors (NOINDEX, Server Down)
    2. The 'Gap' between their GBP Reputation and Website Technicals.
    3. Specific missing SEO elements (H1, NAP).
    """
    
    # --- 1. Extraction & Setup ---
    h1_status = row.get('H1_Audit_Result', '')
    nap_status = row.get('NAP_Audit_Result', '')
    robots_status = row.get('Robots_Status', 'Pass')
    error_status = row.get('Error_Status', 'Success')
    company_name = row.get('Company_Name', 'your business')
    city = row.get('City', 'your area')
    
    # Clean up company name for the email (remove Inc, LLC, etc for casual tone)
    if company_name and company_name != 'N/A':
        company_name = re.sub(r',?\s?(LLC|Inc|Corp|Ltd)\.?$', '', company_name, flags=re.IGNORECASE).strip()
    else:
        company_name = "your business"

    # GBP Data
    gbp_rating = row.get('GBP_Rating', 0.0)
    gbp_reviews = row.get('GBP_Review_Count', 0)
    
    # GBP Strength
    is_gbp_strong = gbp_rating >= 4.0 and gbp_reviews >= 10
    is_gbp_missing = gbp_rating == 0 or gbp_reviews == 0

    # --- 2. Filter Blocked Sites ---
    if error_status == 'Blocked': 
        return "SKIP" 

    # --- 3. Critical Technical Issues (The "Emergency" Pitch) ---
    
    # A. NOINDEX Tag (Site is telling Google to go away)
    if "NOINDEX" in robots_status:
        return (
            "CRITICAL ISSUE: Your website has a 'NOINDEX' tag hidden in the code. "
            "This explicitly tells Google NOT to show your site in search results. "
            "We can fix this immediately to restore your visibility."
        )
    
    # B. Genuine Server Errors (Not blocks)
    if error_status and "Error" in error_status:
         return (
            f"When we tried to audit your site, it returned a server error ({error_status}). "
            "This likely means potential customers can't access your site either. "
            "We can help diagnose and stabilize your site health so you stop losing traffic."
        )

    # --- 4. Scenario-Based Narrative Pitches ---
    
    # Scenario A: The "Hidden Gem" (Strong Reputation, Weak Website)
    # *Best Lead*: They have a real business (reviews) but a bad site.
    if is_gbp_strong and ("Fail" in h1_status or "Fail" in nap_status):
        missing_item = "main headline" if "Fail" in h1_status else "contact info"
        return (
            f"I noticed {company_name} has a great reputation on Google Maps ({gbp_rating} stars), "
            f"but your website isn't doing you justice. Our audit found it's missing a clear {missing_item} "
            "that Google uses to rank you. We can fix these technical gaps to align your website with your real-world reputation."
        )

    # Scenario B: The "Ghost" (No Map Presence + Weak Site)
    # *Opportunity*: They are invisible. Pitch visibility.
    if is_gbp_missing and ("Fail" in h1_status):
        return (
            f"I couldn't find {company_name} in the local Map Pack for '{city}', "
            "and your website is missing the key H1 tags that help you rank there. "
            "Currently, you are invisible to customers searching online. We can help you build your local presence from scratch."
        )

    # Scenario C: Specific H1 Relevance Failure
    # They have an H1, but it says "Home" or "Welcome" instead of "Diesel Mechanic".
    if "Irrelevant" in h1_status:
        return (
            "Your website's main headline doesn't tell Google exactly what you do (it misses your core keywords). "
            "This is the #1 reason local businesses fail to rank organically. "
            "We can tweak your content to target high-intent customers immediately."
        )
    
    # Scenario D: H1 Completely Missing
    if "Missing" in h1_status:
        return (
            "Your homepage is missing a main 'H1' heading. "
            "Search engines rely on this specific tag to understand your services. "
            "Adding this is the fastest, lowest-cost way to improve your visibility."
        )

    # Scenario E: NAP Failure (Trust & Local SEO)
    if "Fail" in nap_status:
        return (
            "We couldn't easily find your phone number or address on your homepage. "
            "If we can't find it, neither can Google's local bot, which hurts your Map rankings significantly. "
            "This is a quick fix that boosts trust and calls."
        )

    # --- 5. The "Growth" Pitch (No Errors Found) ---
    # If the site passes all technical checks, pitch Authority/Backlinks instead.
    return (
        "Your website's technical foundation looks solid. "
        "However, to move from page 2 to page 1, you likely need an Authority strategy (Backlinks & Content) rather than technical fixes. "
        "We specialize in taking established sites like yours to the top spot."
    )

def is_actionable(row):
    """
    Determines if a prospect is actionable based on Data Columns.
    """
    # 1. MANDATORY: Check for Email
    email = row.get('Email_Address')
    if not email or email == 'N/A' or pd.isna(email):
        return 'NO'
    
    # 2. SKIP BLOCKED SITES
    if row.get('Final_Pitch') == "SKIP":
        return 'NO'
        
    # 3. YES: Critical Technical Errors
    if "Fail" in str(row.get('Robots_Status', '')):
        return 'YES'

    # 4. YES: Genuine Server Errors
    error_status = str(row.get('Error_Status', ''))
    if "Error" in error_status and error_status != "Success":
        return 'YES'

    # 5. YES: SEO Gaps
    h1_fail = "Fail" in str(row.get('H1_Audit_Result', ''))
    nap_fail = "Fail" in str(row.get('NAP_Audit_Result', ''))
    
    if h1_fail or nap_fail:
        return 'YES'

    # 6. YES: Healthy Site (Authority Pitch)
    return 'YES'

def create_final_report(df):
    if df.empty:
        print("Report not generated: No unique prospects found.")
        return

    # --- 1. PRE-CALCULATE ACTIONABILITY (Rule-Based) ---
    print("-> Calculating initial actionability (Rule-based)...")
    
    df['Final_Pitch'] = '' 
    df['Actionable_Target'] = df.apply(is_actionable, axis=1)

    # --- 2. FILTER FOR AI GENERATION ---
    ai_candidates = df[
        (df['Actionable_Target'] == 'YES') & 
        (df['Email_Address'] != 'N/A') & 
        (df['Email_Address'].notna())
    ].copy()
    
    print(f"-> Identified {len(ai_candidates)} leads for Campaign generation...")

    # --- 3. RUN GEMINI AI LOOP ---
    email_data = []

    for index, row in ai_candidates.iterrows():
        print(f"-> Generating Campaign for: {row['Company_Name']}...")
        
        campaign = generate_ai_campaign(row)
        
        email_data.append({
            'URL': row['URL'], # Key to merge back
            'Subject_1': campaign.get('subject_1', ''),
            'Body_1': campaign.get('body_1', ''),
            'Subject_2': campaign.get('subject_2', ''),
            'Body_2': campaign.get('body_2', ''),
            'Subject_3': campaign.get('subject_3', ''),
            'Body_3': campaign.get('body_3', '')
        })
        
        time.sleep(7) 

    # --- 4. MERGE AI DATA BACK ---
    if email_data:
        ai_df = pd.DataFrame(email_data)
        df = df.merge(ai_df, on='URL', how='left')
    else:
        for col in ['Subject_1', 'Body_1', 'Subject_2', 'Body_2', 'Subject_3', 'Body_3']:
            df[col] = ''

    # --- 5. EXPORT FOR INSTANTLY ---
    print("\n--- EXPORTING CSV ---")
    
    # Clean Prospect Name
    df['Prospect_Name'] = df['Company_Name'].apply(lambda x: x if x and x != 'N/A' else 'Client')
    df['Website_URL'] = df['URL']
    df['Rank_Found'] = df['Rank']

    # Define Export Columns
    instantly_columns = [
        'Prospect_Name', 
        'Email_Address', 
        'Website_URL',
        'Subject_1', 'Body_1',
        'Subject_2', 'Body_2',
        'Subject_3', 'Body_3',
        'City',
        'Keyword'
    ]
    
    # Filter only actionable targets that actually got emails
    df_instantly = df[(df['Actionable_Target'] == 'YES') & (df['Subject_1'].notna()) & (df['Subject_1'] != '')][instantly_columns].copy()
    
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    csv_filename = f"MapWinners_Campaign_{timestamp}.csv"
    
    df_instantly.to_csv(csv_filename, index=False)
    print(f"File: {csv_filename} (Ready for Instantly.ai)")

    # --- 6. EXPORT DETAILED AUDIT XLSX ---
    print("\n--- EXPORTING XLSX ---")

    detailed_columns = [
        'Actionable_Target',
        'Rank',
        'Company_Name',
        'Email_Address',
        'Keyword',
        'City',
        'URL',
        'Subject_1',
        'Body_1',
        'Subject_2',
        'Body_2',
        'Subject_3',
        'Body_3',
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

    print(f"\n[SUCCESS] Final report created!")
    print(f"Human Readable File: {xlsx_filename}")
    print(f"Total Actionable Leads (Ready for Outreach): {len(df_instantly)}")
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
            
            cleaned_df['Email_Address'] = 'N/A'
            cleaned_df['H1_Audit_Result'] = 'Fail: Not Audited'
            cleaned_df['NAP_Audit_Result'] = 'Fail: Not Audited'
            
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