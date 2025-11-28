import pandas as pd
import time
from .ai_engine import generate_ai_campaign
from .utils import is_actionable

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
    csv_filename = f"Leads_Campaign_{timestamp}.csv"
    
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
