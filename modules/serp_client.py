from serpapi import GoogleSearch
import time
import random

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
