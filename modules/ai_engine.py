import google.generativeai as genai
import json


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


