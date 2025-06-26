import asyncio
import json
import os
import re
from playwright.async_api import async_playwright
from datetime import datetime
from decouple import config
import google.generativeai as genai
from typing import cast

# --- Configuration ---
BASE_URL = "https://www.emprego.co.mz"
JOBS_DB_FILE = "emprego_mz_jobs.json"
CATEGORIES_FILE = "categories.json"
LOCATIONS_FILE = "locations.json"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# Configure the Gemini API
GEMINI_API_KEY = cast(str, config('GEMINI_API_KEY'))
genai.configure(api_key=GEMINI_API_KEY)
generation_config = genai.GenerationConfig(
  temperature=0.2,
  top_p=1,
  top_k=1,
  max_output_tokens=8192,
)
safety_settings = [
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
]
model = genai.GenerativeModel(
    model_name="gemini-2.0-flash",
    generation_config=generation_config,
    safety_settings=safety_settings
)

# --- Helper Functions ---
def load_json_file(filename):
    """Safely loads a JSON file."""
    if not os.path.exists(filename):
        return []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return []

def save_json_file(data, filename):
    """Saves data to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_existing_job_urls(filename):
    """Gets a set of already scraped job URLs from the jobs database."""
    jobs_data = load_json_file(filename)
    return set(job.get("source_url") for job in jobs_data if job.get("source_url"))

async def get_all_job_links(page, existing_urls):
    """Gets all unique job links from all categories and pages, skipping known URLs."""
    # This function remains the same as the previous version, its job is to collect links efficiently.
    # ... (You can copy the full get_all_job_links function from the previous script here) ...
    new_job_urls = set()
    print("--- Phase 1: Discovering Categories ---")
    await page.goto(BASE_URL, wait_until="domcontentloaded")
    category_links = await page.locator('div.content-container-1-4 h2:has-text("Categoria") + ul a').all()
    category_urls = [await link.get_attribute('href') for link in category_links]
    print(f"Found {len(category_urls)} job categories.")

    print("\n--- Phase 2: Collecting Job Links ---")
    for category_url in category_urls:
        current_url = category_url
        print(f"  Scraping category: {current_url}")
        page_num = 1
        while True:
            await page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
            job_link_elements = await page.locator('li.clearfix h3.normal-text a').all()
            if not job_link_elements:
                break
            
            for link in job_link_elements:
                job_href = await link.get_attribute('href')
                if job_href and job_href not in existing_urls:
                    new_job_urls.add(job_href)
            
            print(f"    - Page {page_num}: Total unique new links so far: {len(new_job_urls)}")
            
            next_button = page.locator('div.pagination a.nextpostslink')
            if await next_button.count() > 0:
                current_url = await next_button.get_attribute('href')
                page_num += 1
                await asyncio.sleep(1) # Be respectful
            else:
                break
    return list(new_job_urls)

def build_gemini_prompt(html_content, known_categories, known_locations):
    """Builds the detailed prompt for the Gemini API."""
    return f"""
    You are an expert data extraction bot. Your task is to analyze the raw HTML content of a job posting page from Mozambique and extract the specified information into a clean, valid JSON object.

    **Instructions:**
    1.  Analyze the entire HTML content provided below.
    2.  Extract the following fields and format them into a JSON object.
    3.  For 'tasks_of_the_role' and 'requirements', extract the list items and combine them into a single string with newline characters.
    4.  For 'category', if it's not explicitly mentioned, analyze the job title and description and assign the most appropriate category from the provided 'Known Categories' list.
    5.  Do not invent information. If a field cannot be found, use null as the value.
    6.  The 'expiring_date' might say "Expirado". If so, use that value.
    7.  Ensure the final output is ONLY a valid JSON object and nothing else.

    **Known Categories:** {json.dumps(known_categories)}
    **Known Locations:** {json.dumps(known_locations)}

    **HTML Content:**
    ```html
    {html_content}
    ```

    **Required JSON Output Format:**
    {{
      "job_title": "string",
      "company_name": "string",
      "location": "string",
      "category": "string",
      "publication_date": "string (DD.MM.YYYY or YYYY-MM-DD)",
      "expiring_date": "string (DD.MM.YYYY or YYYY-MM-DD or 'Expirado')",
      "job_description": "string",
      "tasks_of_the_role": "string (with newlines for each item)",
      "requirements": "string (with newlines for each item)"
    }}
    """

def clean_html_text(html_text):
    """Remove HTML tags and clean up text."""
    # Remove HTML tags
    clean = re.sub('<[^<]+?>', '', html_text)
    # Clean up whitespace
    clean = re.sub(r'\s+', ' ', clean)
    return clean.strip()

async def check_if_expired_before_ai(page, job_url):
    """
    Visits a job page, quickly extracts the expiry date from HTML,
    and checks if the job is expired before sending it to the AI.
    Returns tuple: (is_expired: bool, html_content: str).
    The html_content is returned to avoid fetching it again.
    """
    print(f"  -> Pre-checking for expiry: {job_url}")
    try:
        await page.goto(job_url, wait_until="domcontentloaded", timeout=60000)
        html_content = await page.content()
    except Exception as e:
        print(f"    -! ERROR loading page {job_url} for pre-check. Skipping job. Reason: {e}")
        return True, "" # Treat as expired if page fails to load

    # Quick and dirty check for expiry date from HTML
    exp_match = re.search(r'<span[^>]*class="[^"]*column-1-3[^"]*"[^>]*>Expira</span>\s*<span[^>]*class="[^"]*column-2-3[^"]*"[^>]*>(.*?)</span>', html_content, re.DOTALL | re.IGNORECASE)

    if not exp_match:
        print(f"    - Could not pre-determine expiry date for {job_url}. Sending to AI for full check.")
        return False, html_content

    expiry_date_str = clean_html_text(exp_match.group(1))

    if "expirado" in expiry_date_str.lower():
        print(f"  -! SKIPPING expired job (found 'Expirado'): {job_url}")
        return True, html_content

    try:
        # Handle format "DD.MM.YYYY"
        expiry_date = datetime.strptime(expiry_date_str, "%d.%m.%Y").date()
        if expiry_date < datetime.now().date():
            print(f"  -! SKIPPING expired job (date {expiry_date_str} is in the past): {job_url}")
            return True, html_content
    except ValueError:
        print(f"    - Could not parse expiry date '{expiry_date_str}'. Sending to AI for full check.")
        pass

    return False, html_content

async def extract_details_with_gemini(html_content, job_url, known_categories, known_locations):
    """Uses Gemini API to extract job details from provided HTML."""
    print(f"  -> Processing with AI: {job_url}")
    prompt = build_gemini_prompt(html_content, known_categories, known_locations)
    
    try:
        response = await model.generate_content_async(prompt)
        # Clean up the response from markdown code block
        json_string = response.text.strip().replace("```json", "").replace("```", "").strip()
        job_details = json.loads(json_string)
        
        # Add the source URL, which the AI doesn't know
        job_details['source_url'] = job_url
        return job_details
    except Exception as e:
        print(f"    -! ERROR calling Gemini API for {job_url}. Reason: {e}")
        return None

async def main():
    # Load existing data to avoid re-scraping and to build master lists
    existing_jobs = load_json_file(JOBS_DB_FILE)
    existing_urls = set(job.get("source_url") for job in existing_jobs)
    
    # Safely extract categories and locations, handling None/NaN values
    known_categories = []
    known_locations = []
    
    for job in existing_jobs:
        # Handle category safely
        category = job.get("category")
        if category and isinstance(category, str) and category.strip():
            # Take the first category if multiple are comma-separated
            category_clean = category.split(',')[0].strip()
            if category_clean and category_clean not in known_categories:
                known_categories.append(category_clean)
        
        # Handle location safely  
        location = job.get("location")
        if location and isinstance(location, str) and location.strip():
            location_clean = location.strip()
            if location_clean and location_clean not in known_locations:
                known_locations.append(location_clean)
    
    # Sort the lists
    known_categories = sorted(known_categories)
    known_locations = sorted(known_locations)
    
    print(f"Found {len(existing_urls)} existing jobs. These will be skipped.")
    print(f"Loaded {len(known_categories)} unique categories and {len(known_locations)} unique locations.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        job_links_to_scrape = await get_all_job_links(page, existing_urls)
        total_links = len(job_links_to_scrape)
        
        if not total_links:
            print("\nNo new job postings found. Exiting.")
            await browser.close()
            return

        print(f"\n--- Phase 3: Using AI to Extract Details for {total_links} New Jobs ---")
        all_new_jobs_data = []
        for i, job_url in enumerate(job_links_to_scrape):
            print(f"Processing job {i+1}/{total_links}...")
            
            # Pre-check for expiry to save API calls
            is_expired, html_content = await check_if_expired_before_ai(page, job_url)
            if is_expired:
                continue
            
            # If not expired, proceed with AI extraction
            job_details = await extract_details_with_gemini(html_content, job_url, known_categories, known_locations)
            
            if job_details:
                # The AI might still find it's expired, which is a good fallback.
                expiry_date_str = job_details.get("expiring_date", "")
                if "expirado" in expiry_date_str.lower():
                    print(f"  -! Skipping expired job (confirmed by AI): {job_url}")
                    continue
                
                # Add to our list and update master lists
                all_new_jobs_data.append(job_details)
                if job_details.get("location") and job_details["location"] not in known_locations:
                    known_locations.append(job_details["location"])
                if job_details.get("category") and job_details["category"] not in known_categories:
                    known_categories.append(job_details["category"])
        
        await browser.close()

    if all_new_jobs_data:
        print(f"\n--- Scraping Complete: Extracted {len(all_new_jobs_data)} new active jobs. ---")
        
        # Combine old and new jobs, then save
        final_job_list = existing_jobs + all_new_jobs_data
        save_json_file(final_job_list, JOBS_DB_FILE)
        print(f"Updated {JOBS_DB_FILE} with {len(all_new_jobs_data)} new jobs.")
        
        # Save updated master lists
        save_json_file(sorted(known_categories), CATEGORIES_FILE)
        save_json_file(sorted(known_locations), LOCATIONS_FILE)
        print(f"Updated master lists for categories and locations.")
    else:
        print("\nNo new active jobs were scraped in this run.")

if __name__ == '__main__':
    asyncio.run(main())