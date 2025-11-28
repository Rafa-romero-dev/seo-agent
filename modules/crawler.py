import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urlparse, urljoin
import random
import time
from requests.exceptions import RequestException
from .constants import USER_AGENTS, JUNK_EMAIL_EXTENSIONS, JUNK_EMAIL_PREFIXES, JUNK_EMAIL_DOMAINS

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

    priority_prefixes = ['info', 'contact', 'sales', 'office', 'admin', 'hello', 'service']
    
    def sort_score(e):
        prefix = e.split('@')[0]
        if prefix in priority_prefixes:
            return 0
        return 1
        
    valid_emails.sort(key=sort_score)
    
    # Debug Print (If needed)
    #print(f"   [DEBUG] Emails Found: {valid_emails}")
    
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
