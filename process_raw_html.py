import asyncio
import json
import os
from decouple import config
import google.generativeai as genai

# --- Configuration ---
RAW_INPUT_FILE = "emprego_mz_raw.json"
FINAL_OUTPUT_FILE = "emprego_mz_jobs.json"
LOCATIONS_FILE = "locations.json"
CATEGORIES_FILE = "categories.json"

# --- Gemini Configuration ---
try:
    api_key_value = config("GEMINI_API_KEY", default=None)
    if isinstance(api_key_value, str):
        genai.configure(api_key=api_key_value)
        GEMINI_API_KEY = api_key_value
        print("Gemini API key loaded successfully.")
    else:
        print("Warning: GEMINI_API_KEY not found. AI processing will fail.")
        GEMINI_API_KEY = None
except Exception as e:
    print(f"Error loading Gemini API key: {e}")
    GEMINI_API_KEY = None

# --- AI and Helper Functions ---

async def extract_job_details_with_gemini(html_body: str, source_url: str) -> dict | None:
    """
    Uses the Gemini API to extract structured job data from a raw HTML body.
    """
    if not GEMINI_API_KEY:
        print("  -! ERROR: Gemini API key not configured.")
        return None

    model = genai.GenerativeModel('gemini-1.5-flash')
    prompt = """
    Based on the following HTML body, extract the job information into a clean JSON object.
    Keys: "job_title", "company_name", "location", "category", "publication_date", "expiring_date", "job_description", "tasks_of_the_role", "requirements".
    If a value isn't found, use an empty string "". Output only the JSON object.
    HTML Body:
    ---
    {html_body}
    ---
    """
    try:
        response = await model.generate_content_async(
            prompt.format(html_body=html_body),
            request_options={"timeout": 60}
        )
        job_data = json.loads(response.text.strip().replace("```json", "").replace("```", ""))
        job_data["source_url"] = source_url
        return job_data
    except Exception as e:
        print(f"  -! AI extraction failed for {source_url}. Reason: {e}")
        return None

def save_jobs_to_json(jobs_data, filename):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(jobs_data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"Error saving to JSON: {e}")
        return False

def update_json_list(filename, new_items):
    try:
        existing_items = set()
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            with open(filename, 'r', encoding='utf-8') as f:
                existing_items = set(json.load(f))
        
        new_unique_items = {item for item in new_items if item and item not in existing_items}
        if new_unique_items:
            updated_items = sorted(list(existing_items.union(new_unique_items)))
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(updated_items, f, ensure_ascii=False, indent=2)
            print(f" -> Updated {filename} with {len(new_unique_items)} new items.")
    except Exception as e:
        print(f"Error updating {filename}: {e}")

async def main():
    try:
        with open(RAW_INPUT_FILE, 'r', encoding='utf-8') as f:
            raw_jobs = json.load(f)
    except FileNotFoundError:
        print(f"Error: Raw input file not found at {RAW_INPUT_FILE}. Please run the scraper first.")
        return

    print(f"--- Processing {len(raw_jobs)} jobs from {RAW_INPUT_FILE} ---")
    processed_jobs = []
    
    for i, raw_job in enumerate(raw_jobs):
        print(f"Processing job {i+1}/{len(raw_jobs)}: {raw_job['source_url']}")
        job_details = await extract_job_details_with_gemini(raw_job['raw_html'], raw_job['source_url'])
        if job_details:
            processed_jobs.append(job_details)

    if processed_jobs:
        print(f"\n--- Processing Complete: Successfully processed {len(processed_jobs)} jobs. ---")
        save_jobs_to_json(processed_jobs, FINAL_OUTPUT_FILE)
        print(f"Final data saved to {FINAL_OUTPUT_FILE}")

        # Update locations and categories
        locations = {job.get('location') for job in processed_jobs}
        categories = {job.get('category') for job in processed_jobs}
        update_json_list(LOCATIONS_FILE, locations)
        update_json_list(CATEGORIES_FILE, categories)
    else:
        print("\nNo jobs were successfully processed.")

if __name__ == '__main__':
    asyncio.run(main()) 