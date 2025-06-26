import asyncio
import json
import os
import re
from playwright.async_api import async_playwright
from datetime import datetime
from decouple import config
import google.generativeai as genai
from typing import cast
import pandas as pd
from selectolax.parser import HTMLParser

# --- Configuration ---
JOBS_DB_FILE = "emprego_mz_jobs.json"
CATEGORIES_FILE = "categories.json"
LOCATIONS_FILE = "locations.json"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# Configure the Gemini API
try:
    GEMINI_API_KEY = cast(str, config('GEMINI_API_KEY'))
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
    print("Gemini API key loaded successfully.")
except Exception as e:
    print(f"Error loading Gemini API key: {e}. AI features will be disabled.")
    GEMINI_API_KEY = None

# --- Helper Functions ---
def load_json_file(filename):
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        return []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return []

def save_json_file(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def build_gemini_prompt(html_content, known_categories, known_locations):
    return f"""
    You are an expert data extraction bot. Your task is to analyze the raw HTML content of a job posting page from Mozambique and extract the specified information into a clean, valid JSON object.
    **Instructions:**
    1. Analyze the entire HTML content provided below.
    2. Extract the following fields and format them into a JSON object: "job_title", "company_name", "location", "category", "publication_date", "expiring_date", "job_description", "tasks_of_the_role", "requirements".
    3. For 'tasks_of_the_role' and 'requirements', extract list items and combine them into a single string, with each item separated by a newline.
    4. For 'category' and 'location', if they are mentioned, use the exact values. If not, assign the most appropriate values from the provided 'Known Categories' and 'Known Locations' lists based on the job context.
    5. If a field cannot be found, use an empty string "" as the value.
    6. Ensure the final output is ONLY a valid JSON object.
    **Known Categories:** {json.dumps(known_categories)}
    **Known Locations:** {json.dumps(known_locations)}
    **HTML Content:**
    ```html
    {html_content}
    ```
    """

async def check_if_expired_before_ai(page, job_url):
    print(f"  -> Pre-checking for expiry: {job_url}")
    try:
        await page.goto(job_url, wait_until="domcontentloaded", timeout=60000)
        html_content = await page.content()
    except Exception as e:
        print(f"    -! ERROR loading page {job_url} for pre-check. Skipping. Reason: {e}")
        return True, ""

    parser = HTMLParser(html_content)
    expiry_node = parser.css_first('span.column-2-3:-soup-contains("Expirado")')
    if expiry_node:
        print(f"  -! SKIPPING expired job (found 'Expirado'): {job_url}")
        return True, html_content

    exp_date_node = parser.css_first('span.column-1-3:contains("Expira") + span.column-2-3')
    if exp_date_node:
        expiry_date_str = exp_date_node.text(strip=True)
        try:
            expiry_date = datetime.strptime(expiry_date_str, "%d.%m.%Y").date()
            if expiry_date < datetime.now().date():
                print(f"  -! SKIPPING expired job (date {expiry_date_str} is in the past): {job_url}")
                return True, html_content
        except ValueError:
            pass
    
    return False, html_content

async def extract_details_with_gemini(html_content, job_url, known_categories, known_locations):
    if not GEMINI_API_KEY:
        print("    -! Cannot extract details, Gemini API key not available.")
        return None

    print(f"  -> Processing with AI: {job_url}")
    prompt = build_gemini_prompt(html_content, known_categories, known_locations)
    
    try:
        response = await model.generate_content_async(prompt, request_options={"timeout": 120})
        json_string = response.text.strip().replace("```json", "").replace("```", "").strip()
        job_details = json.loads(json_string)
        job_details['source_url'] = job_url
        return job_details
    except Exception as e:
        print(f"    -! ERROR calling Gemini API for {job_url}. Reason: {e}")
        return None

async def main():
    existing_jobs = load_json_file(JOBS_DB_FILE)
    known_categories = sorted(list(set(job.get("category", "") for job in existing_jobs if job.get("category"))))
    known_locations = sorted(list(set(job.get("location", "") for job in existing_jobs if job.get("location"))))
    
    print(f"Loaded {len(existing_jobs)} existing jobs, {len(known_categories)} unique categories, and {len(known_locations)} unique locations.")
    
    csv_file = "emprego_mz_jobs.csv"
    try:
        df = pd.read_csv(csv_file)
        job_links_to_scrape = df["source_url"].dropna().unique().tolist()
        print(f"Found {len(job_links_to_scrape)} unique job URLs to process from {csv_file}.")
    except (FileNotFoundError, KeyError) as e:
        print(f"Error reading CSV: {e}. Make sure '{csv_file}' exists and has a 'source_url' column.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(user_agent=USER_AGENT)

        updated_jobs = {job["source_url"]: job for job in existing_jobs}

        for i, job_url in enumerate(job_links_to_scrape):
            print(f"\nProcessing job {i+1}/{len(job_links_to_scrape)}...")
            
            is_expired, html_content = await check_if_expired_before_ai(page, job_url)
            if is_expired:
                if job_url in updated_jobs:
                    updated_jobs[job_url]['expiring_date'] = 'Expirado'
                continue
            
            job_details = await extract_details_with_gemini(html_content, job_url, known_categories, known_locations)
            
            if job_details:
                updated_jobs[job_url] = job_details
        
        await browser.close()

    if updated_jobs:
        final_job_list = list(updated_jobs.values())
        new_categories = sorted(list(set(job.get("category", "") for job in final_job_list if job.get("category"))))
        new_locations = sorted(list(set(job.get("location", "") for job in final_job_list if job.get("location"))))
        
        save_json_file(final_job_list, JOBS_DB_FILE)
        save_json_file(new_categories, CATEGORIES_FILE)
        save_json_file(new_locations, LOCATIONS_FILE)
        
        print(f"\n--- Processing Complete ---")
        print(f"Saved {len(final_job_list)} total jobs to {JOBS_DB_FILE}.")
        print(f"Updated master lists for categories and locations.")
    else:
        print("\nNo jobs were processed or updated.")

if __name__ == '__main__':
    asyncio.run(main())