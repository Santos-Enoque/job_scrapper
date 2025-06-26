import asyncio
import json
import os
import re
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
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
        
        next_button = page.locator('a.ts:has-text("Next >")').first
        if await next_button.count() > 0:
            next_page_href = await next_button.get_attribute('href')
            current_url = f"{BASE_URL}{next_page_href}" if next_page_href.startswith('/') else next_page_href
            page_num += 1
            await asyncio.sleep(2)
        else:
            print("  -> No more pages found. Concluding link collection.")
            break
            
    return list(new_job_urls)

async def scrape_job_details_un(page, job_url, categories):
    """
    Scrapes details for a single UN job page using robust selectors,
    intelligent parsing, and by blocking non-essential requests to prevent timeouts.
    """
    print(f"  -> Scraping detail page: {job_url}")

    # Block non-essential requests to speed up loading and prevent timeouts.
    blocked_resource_types = ["image", "font", "media", "stylesheet"]
    blocked_domains = [
        "googlesyndication.com", "googleadservices.com", "googletagmanager.com",
        "google-analytics.com", "doubleclick.net", "cloudflareinsights.com", "iubenda.com"
    ]
    await page.route(
        "**/*",
        lambda route: route.abort()
        if route.request.resource_type in blocked_resource_types
        or any(domain in route.request.url for domain in blocked_domains)
        else route.continue_()
    )

    try:
        await page.goto(job_url, wait_until="domcontentloaded", timeout=60000)
        
        # Check for Cloudflare challenge
        title = await page.title()
        if "Just a moment" in title or "Verifying you are human" in title:
            print(f"     -> Detected Cloudflare challenge, waiting...")
            # Wait longer for the challenge to resolve
            await asyncio.sleep(10)
            
            # Check if we're past the challenge
            await page.wait_for_load_state("networkidle", timeout=30000)
            title = await page.title()
            if "Just a moment" in title:
                print(f"     -! Cloudflare challenge not resolved, skipping page.")
                return None
        
        # Try multiple selectors since the page structure might vary
        content_found = False
        selectors_to_try = ['div.fp-snippet', 'div.content', 'main', 'article', 'div.job-detail']
        
        for selector in selectors_to_try:
            try:
                await page.wait_for_selector(selector, timeout=10000)
                content_found = True
                break
            except:
                continue
        
        if not content_found:
            print(f"     -! Could not find job content with any known selector.")
            return None
            
        html = await page.content()
        parser = HTMLParser(html)
    except Exception as e:
        print(f"     -! Could not load or find content on page {job_url}. Error: {e}.")
        return None
    finally:
        # Important to unroute to not affect the next page navigation in the loop
        await page.unroute("**/*")

    # --- Data Extraction Logic ---
    job_title = (parser.css_first('h2') or {}).text(strip=True) or "Not Found"
    company_name = (parser.css_first('li.list-group-item:has-text("Organization:") a') or {}).text(strip=True) or "Not Found"
    
    city = (parser.css_first('li.list-group-item:has-text("City:") a') or {}).text(strip=True)
    country = (parser.css_first('li.list-group-item:has-text("Country:") a') or {}).text(strip=True)
    location = ", ".join(filter(None, [city, country])) or "Not Found"
    
    expiring_date = ""
    deadline_node = parser.css_first('p > b:contains("DEADLINE FOR APPLICATIONS")')
    if deadline_node and deadline_node.parent:
        match = re.search(r'(\d{1,2}(?:st|nd|rd|th)?\s+\w+\s+\d{4})', deadline_node.parent.text(), re.IGNORECASE)
        if match:
            expiring_date = match.group(1)

    job_description, tasks_of_the_role, requirements = "", [], ""
    content_node = parser.css_first('div.fp-snippet')
    if content_node:
        current_section = "description"
        req_text_list = []
        for node in content_node.css('p, ul'):
            header_text = (node.css_first('b') or {}).text(strip=True).upper()
            if "BACKGROUND AND PURPOSE" in header_text: current_section = "description"
            elif "ACCOUNTABILITIES/RESPONSIBILITIES" in header_text: current_section = "tasks"; continue
            elif "QUALIFICATIONS" in header_text or "EXPERIENCE REQUIRED" in header_text: current_section = "requirements"; continue
            elif "TERMS AND CONDITIONS" in header_text: break

            if current_section == "description": job_description += node.text(strip=True) + "\n"
            elif current_section == "tasks" and node.tag == 'ul': tasks_of_the_role.extend([li.text(strip=True) for li in node.css('li') if li.text(strip=True)])
            elif current_section == "requirements":
                if node.tag == 'ul': req_text_list.extend([li.text(strip=True) for li in node.css('li') if li.text(strip=True)])
                else: req_text_list.append(node.text(strip=True))
        requirements = "\n".join(filter(None, req_text_list))

    job_details = {
        "job_title": job_title.strip(), "company_name": company_name.strip(), "location": location,
        "publication_date": "", "expiring_date": expiring_date,
        "job_description": job_description.strip(), "tasks_of_the_role": tasks_of_the_role,
        "requirements": requirements, "source_url": job_url
    }
    
    if GEMINI_API_KEY and job_details["job_description"]:
        print("     -> Determining category with AI...")
        job_details["category"] = await get_ai_category(job_details["job_title"], job_details["job_description"], categories)
        print(f"     -> Assigned category: {job_details['category']}")
    else:
        job_details["category"] = "Uncategorized"

    return job_details

async def get_ai_category(title, description, existing_categories):
    """Uses Gemini to determine the best category for the job."""
    if not GEMINI_API_KEY: return "Uncategorized"
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        prompt = f"From the list: [{', '.join(existing_categories)}], what is the best single category for this job?\n\nTitle: {title}\nDescription: {description[:1000]}\n\nIf none fit, create a new, concise category (1-3 words). Return only the category name."
        response = await model.generate_content_async(prompt)
        return response.text.strip().replace("*", "")
    except Exception as e:
        print(f"     -! AI categorization failed: {e}")
        return "Uncategorized"

async def main():
    """Main function to run the UN Jobs scraper, now with stealth evasion."""
    print("--- Starting UN Jobs Scraper ---")
    
    existing_urls = get_existing_job_urls(OUTPUT_JSON_FILE)
    print(f"Found {len(existing_urls)} existing UN jobs. These will be skipped.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, 
            args=[
                "--no-sandbox", 
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor"
            ]
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York"
        )
        
        # Apply stealth patches to make the browser look more human
        stealth = Stealth()
        await stealth.apply_stealth_async(context)
        
        page = await context.new_page()

        job_links_to_scrape = await get_all_job_links_un(page, existing_urls)
        if not job_links_to_scrape:
            print("\nNo new UN job postings found. Exiting.")
            await browser.close()
            return
            
        print(f"\n--- Phase 2: Scraping Details for {len(job_links_to_scrape)} New UN Jobs ---")
        
        current_categories = []
        if os.path.exists(CATEGORIES_FILE) and os.path.getsize(CATEGORIES_FILE) > 0:
            with open(CATEGORIES_FILE, 'r', encoding='utf-8') as f:
                current_categories = json.load(f)

        all_new_jobs_data = []
        total_processed = 0
        for i, job_url in enumerate(job_links_to_scrape):
            print(f"Processing job {i+1}/{len(job_links_to_scrape)}...")
            try:
                job_details = await scrape_job_details_un(page, job_url, current_categories)
                if job_details and job_details.get("job_title") != "Not Found":
                    all_new_jobs_data.append(job_details)
                    total_processed += 1
            except Exception as e:
                print(f"   -! UNHANDLED ERROR for {job_url}. Reason: {e}")
            
            # Add delay between requests to avoid triggering rate limits
            await asyncio.sleep(3)
            
            # Save in batches
            if (i + 1) % BATCH_SIZE == 0 or (i + 1) == len(job_links_to_scrape):
                if all_new_jobs_data:
                    print(f"\n--- Batch Complete: Saving {len(all_new_jobs_data)} jobs ---")
                    save_jobs_to_json(all_new_jobs_data, OUTPUT_JSON_FILE)
                    update_json_list_files(all_new_jobs_data, current_categories)
                    all_new_jobs_data = []  # Clear the batch after saving
        
        await browser.close()

    print(f"\n--- Scraping Complete: Processed {total_processed} new jobs. ---")

# --- Helper Functions ---
def get_existing_job_urls(filename):
    if not os.path.exists(filename): return set()
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return set(job.get("source_url") for job in json.load(f) if job.get("source_url"))
    except (json.JSONDecodeError, FileNotFoundError): return set()

def save_jobs_to_json(new_jobs_data, filename):
    try:
        existing_jobs = []
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            with open(filename, 'r', encoding='utf-8') as f:
                existing_jobs = json.load(f)
        
        existing_jobs_dict = {job.get('source_url'): job for job in existing_jobs}
        for job in new_jobs_data: existing_jobs_dict[job['source_url']] = job
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(list(existing_jobs_dict.values()), f, ensure_ascii=False, indent=2)
    except Exception as e: print(f"Error saving to JSON: {e}")

def update_json_list_files(jobs_data, current_categories):
    new_locations = {job.get('location') for job in jobs_data if job.get('location') != 'Not Found'}
    new_categories = {job.get('category') for job in jobs_data if job.get('category') != 'Uncategorized'}
    
    update_json_list(LOCATIONS_FILE, new_locations)
    current_categories.extend(list(new_categories))
    update_json_list(CATEGORIES_FILE, set(current_categories))

def update_json_list(filename, new_items):
    try:
        existing_items = set()
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            with open(filename, 'r', encoding='utf-8') as f:
                existing_items = set(json.load(f))
        
        new_unique_items = new_items - existing_items
        if not new_unique_items: return

        updated_items = sorted(list(existing_items.union(new_unique_items)))
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(updated_items, f, ensure_ascii=False, indent=2)
        print(f" -> Updated {filename} with {len(new_unique_items)} new items.")
    except Exception as e: print(f"Error updating {filename}: {e}")

if __name__ == '__main__':
    asyncio.run(main())