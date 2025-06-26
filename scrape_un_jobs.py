import asyncio
import json
import os
import re
from playwright.async_api import async_playwright
from decouple import config
import google.generativeai as genai
from selectolax.parser import HTMLParser

# --- Configuration ---
BASE_URL = "https://unjobs.org"
START_URL = f"{BASE_URL}/duty_stations/mozambique"
OUTPUT_JSON_FILE = "un_jobs_mz.json"
LOCATIONS_FILE = "locations.json"
CATEGORIES_FILE = "categories.json"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
BATCH_SIZE = 10 # Keep batch size small to be respectful to the server

# --- Gemini Configuration ---
# Note: For this to work, you must have a .env file in the same directory
# with the line: GEMINI_API_KEY="your_actual_key_here"
# Or, you can replace config("GEMINI_API_KEY") directly with your key string.
try:
    api_key_value = config("GEMINI_API_KEY", default=None)
    if isinstance(api_key_value, str):
        genai.configure(api_key=api_key_value)
        GEMINI_API_KEY = api_key_value
        print("Gemini API key loaded successfully.")
    else:
        print("Warning: GEMINI_API_KEY not found. AI categorization will be skipped.")
        GEMINI_API_KEY = None
except Exception as e:
    print(f"Error loading Gemini API key: {e}")
    GEMINI_API_KEY = None

# --- Main Functions ---

async def get_all_job_links_un(page, existing_urls):
    """
    Gets all unique job links from the UN Jobs site for Mozambique.
    This function navigates through all pages and collects the job URLs.
    """
    print("--- Phase 1: Collecting Job Links from UN Jobs ---")
    
    new_job_urls = set()
    current_url = START_URL
    page_num = 1
    
    while current_url:
        print(f"  Scraping list page {page_num}: {current_url}")
        try:
            await page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
        except Exception as e:
            print(f"    -! Could not load page {current_url}. Error: {e}. Stopping link collection.")
            break

        # This selector is for the job links on the LISTING pages
        job_links = await page.locator('div.job a.jtitle').all()

        if not job_links:
            print("    -> No job links found on this page. Ending collection.")
            break

        for link in job_links:
            href = await link.get_attribute('href')
            if href:
                full_url = f"{BASE_URL}{href}" if href.startswith('/') else href
                if full_url not in existing_urls and full_url not in new_job_urls:
                    new_job_urls.add(full_url)

        print(f"    -> Found {len(job_links)} links on page. Collected {len(new_job_urls)} new unique links so far.")
        
        # Find the "Next" button to go to the next page
        next_button = page.locator('a.ts:has-text("Next >")').first
        if await next_button.count() > 0:
            next_page_href = await next_button.get_attribute('href')
            current_url = f"{BASE_URL}{next_page_href}" if next_page_href.startswith('/') else next_page_href
            page_num += 1
            await asyncio.sleep(2)  # Small delay to be respectful
        else:
            print("  -> No more pages found. Concluding link collection.")
            break
            
    return list(new_job_urls)

async def scrape_job_details_un(page, job_url, categories):
    """
    Scrapes details for a single UN job page using robust selectors
    and intelligent parsing for the description block.
    """
    print(f"  -> Scraping detail page: {job_url}")
    try:
        # Wait for network to be idle to ensure all JS has loaded content
        await page.goto(job_url, wait_until="networkidle", timeout=60000)
        
        # Add a specific wait for a key content element to appear
        await page.wait_for_selector('div.fp-snippet', timeout=30000)
        
        html = await page.content()
        parser = HTMLParser(html)
    except Exception as e:
        print(f"     -! Could not load or find content on page {job_url}. Error: {e}.")
        return None

    # --- Step 1: Extract details using corrected and more specific selectors ---
    job_title_node = parser.css_first('h2')
    job_title = job_title_node.text(strip=True) if job_title_node else "Not Found"

    company_node = parser.css_first('li.list-group-item:has-text("Organization:") a')
    company_name = company_node.text(strip=True) if company_node else "Not Found"

    # Location is composed of City and Country for better filtering
    city_node = parser.css_first('li.list-group-item:has-text("City:") a')
    country_node = parser.css_first('li.list-group-item:has-text("Country:") a')
    city = city_node.text(strip=True) if city_node else ""
    country = country_node.text(strip=True) if country_node else ""
    location = ", ".join(filter(None, [city, country])) or "Not Found"
    
    # --- Corrected Date Extraction ---
    expiring_date = ""
    deadline_node = parser.css_first('p > b:contains("DEADLINE FOR APPLICATIONS")')
    if deadline_node and deadline_node.parent:
        full_text = deadline_node.parent.text(strip=True)
        # Regex to find a date like "4 July 2025" or "4th JULY 2025"
        match = re.search(r'(\d{1,2}(?:st|nd|rd|th)?\s+\w+\s+\d{4})', full_text, re.IGNORECASE)
        if match:
            expiring_date = match.group(1)

    # --- Step 2: Intelligent Parsing of the Main Content Block ---
    job_description = ""
    tasks_of_the_role = []
    requirements_text = []
    
    content_node = parser.css_first('div.fp-snippet')
    if content_node:
        nodes = content_node.css('p, ul') # Target only paragraphs and lists
        current_section = "description" # Start by collecting the description

        for node in nodes:
            # Check for bolded headers to switch sections
            header_node = node.css_first('b')
            if header_node:
                header_text = header_node.text(strip=True).upper()
                if "BACKGROUND AND PURPOSE" in header_text:
                    current_section = "description"
                elif "ACCOUNTABILITIES/RESPONSIBILITIES" in header_text:
                    current_section = "tasks"
                    continue # Skip the header itself from being added
                elif "QUALIFICATIONS" in header_text or "EXPERIENCE REQUIRED" in header_text:
                    current_section = "requirements"
                    continue
                elif "TERMS AND CONDITIONS" in header_text:
                    current_section = "stop" # Stop collecting at terms
                
            # Append content to the appropriate section
            if current_section == "description":
                job_description += node.text(strip=True) + "\n"
            elif current_section == "tasks" and node.tag == 'ul':
                tasks_of_the_role.extend([li.text(strip=True) for li in node.css('li') if li.text(strip=True)])
            elif current_section == "requirements":
                # Can be a mix of <p> and <ul>
                if node.tag == 'ul':
                    requirements_text.extend([li.text(strip=True) for li in node.css('li') if li.text(strip=True)])
                else:
                    requirements_text.append(node.text(strip=True))
            elif current_section == "stop":
                break

    # Clean up the collected text
    job_description = job_description.strip()
    requirements = "\n".join(filter(None, requirements_text))

    # --- Step 3: Assemble the final dictionary ---
    job_details = {
        "job_title": job_title,
        "company_name": company_name,
        "location": location,
        "publication_date": "", # This field is not reliably available on the site
        "expiring_date": expiring_date,
        "job_description": job_description,
        "tasks_of_the_role": tasks_of_the_role,
        "requirements": requirements,
        "source_url": job_url
    }
    
    # --- Step 4: Use AI for categorization ---
    if GEMINI_API_KEY and job_title and job_description:
        print("     -> Determining category with AI...")
        category = await get_ai_category(job_title, job_description, categories)
        job_details["category"] = category
        print(f"     -> Assigned category: {category}")
    else:
        job_details["category"] = "Uncategorized"

    return job_details

async def get_ai_category(title, description, existing_categories):
    """
    Second AI call: Determines the best category for the job.
    """
    if not GEMINI_API_KEY:
        return "Uncategorized"

    model = genai.GenerativeModel('gemini-1.5-flash')
    prompt = f"""
    Based on the job title and description below, what is the best category for this job from the following list?

    Existing Categories: {', '.join(existing_categories)}

    Job Title: {title}
    Job Description: {description[:1000]}

    If one of the existing categories is a good fit, return that exact category name.
    If none are a good fit, create a new, concise, and relevant category (1-3 words).
    Return only the single category name and nothing else.
    """
    try:
        response = await model.generate_content_async(prompt)
        category = response.text.strip().replace("*", "")
        return category
    except Exception as e:
        print(f"     -! AI categorization failed: {e}")
        return "Uncategorized"

async def main():
    """
    Main function to run the UN Jobs scraper.
    """
    print("--- Starting UN Jobs Scraper ---")
    
    # Load existing data to avoid duplicates
    existing_urls = get_existing_job_urls(OUTPUT_JSON_FILE)
    print(f"Found {len(existing_urls)} existing UN jobs. These will be skipped.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context(user_agent=USER_AGENT)
        page = await context.new_page()

        # Phase 1: Get all job links
        job_links_to_scrape = await get_all_job_links_un(page, existing_urls)
        total_links = len(job_links_to_scrape)
        
        if not total_links:
            print("\nNo new UN job postings found. Exiting.")
            await browser.close()
            return
            
        print(f"\n--- Phase 2: Scraping Details for {total_links} New UN Jobs ---")
        
        # Load categories for the AI to use
        current_categories = []
        if os.path.exists(CATEGORIES_FILE) and os.path.getsize(CATEGORIES_FILE) > 0:
            with open(CATEGORIES_FILE, 'r', encoding='utf-8') as f:
                current_categories = json.load(f)

        all_new_jobs_data = []
        for i in range(0, total_links, BATCH_SIZE):
            batch_links = job_links_to_scrape[i:i + BATCH_SIZE]
            print(f"\n--- Processing Batch {i//BATCH_SIZE + 1}/{ -(-total_links // BATCH_SIZE)} ---")
            
            for j, job_url in enumerate(batch_links):
                current_job_number = i + j + 1
                print(f"Processing job {current_job_number}/{total_links}...")
                try:
                    job_details = await scrape_job_details_un(page, job_url, current_categories)
                    if job_details and job_details.get("job_title") != "Not Found":
                        all_new_jobs_data.append(job_details)
                except Exception as e:
                    print(f"   -! ERROR during detail scraping for {job_url}. Reason: {e}")
            
            # Save after each batch
            if all_new_jobs_data:
                print(f"\n--- Batch Complete: Saving {len(all_new_jobs_data)} new jobs so far ---")
                save_jobs_to_json(all_new_jobs_data, OUTPUT_JSON_FILE)

                # Update locations and categories files
                new_locations = {job.get('location') for job in all_new_jobs_data if job.get('location')}
                new_categories = {job.get('category') for job in all_new_jobs_data if job.get('category') and job.get('category') != 'Uncategorized'}
                
                # Update the live list of categories for subsequent batches
                current_categories = sorted(list(set(current_categories).union(new_categories)))
                update_json_list(LOCATIONS_FILE, new_locations)
                update_json_list(CATEGORIES_FILE, new_categories)
        
        await browser.close()

    if all_new_jobs_data:
        print(f"\n--- Scraping Complete: Extracted a total of {len(all_new_jobs_data)} new active UN jobs. ---")
    else:
        print("\nNo new active UN jobs were successfully scraped in this run.")

# --- Helper Functions (reused from your original scraper) ---
def get_existing_job_urls(filename):
    if not os.path.exists(filename):
        return set()
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            jobs_data = json.load(f)
            return set(job.get("source_url", "") for job in jobs_data if job.get("source_url"))
    except (json.JSONDecodeError, FileNotFoundError):
        return set()

def save_jobs_to_json(new_jobs_data, filename):
    try:
        existing_jobs = []
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            with open(filename, 'r', encoding='utf-8') as f:
                try:
                    existing_jobs = json.load(f)
                except json.JSONDecodeError:
                    print(f"Warning: Could not decode existing JSON from {filename}. Starting fresh.")
                    existing_jobs = []
        
        # Create a dictionary of existing jobs by URL for quick lookups
        existing_jobs_dict = {job.get('source_url'): job for job in existing_jobs}
        
        # Add or update jobs
        for job in new_jobs_data:
            existing_jobs_dict[job['source_url']] = job
        
        # Convert back to a list for saving
        all_jobs = list(existing_jobs_dict.values())

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(all_jobs, f, ensure_ascii=False, indent=2)
        
        return True
    except Exception as e:
        print(f"Error saving to JSON: {e}")
        return False

def update_json_list(filename, new_items):
    try:
        existing_items = set()
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            with open(filename, 'r', encoding='utf-8') as f:
                try:
                    existing_items = set(json.load(f))
                except json.JSONDecodeError:
                    existing_items = set()

        new_unique_items = set(item for item in new_items if item and item not in existing_items)
        
        if not new_unique_items:
            return

        updated_items = sorted(list(existing_items.union(new_unique_items)))

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(updated_items, f, ensure_ascii=False, indent=2)
        
        print(f" -> Updated {filename} with {len(new_unique_items)} new items.")
    except Exception as e:
        print(f"Error updating {filename}: {e}")

if __name__ == '__main__':
    asyncio.run(main())