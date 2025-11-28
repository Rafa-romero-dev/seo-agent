import google.generativeai as genai
import json
import re

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
    You are a top-tier SEO Sales Copywriter for a top-tier SEO agency. 
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
