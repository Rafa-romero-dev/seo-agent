import pandas as pd
from .constants import DIRECTORY_DOMAINS

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
