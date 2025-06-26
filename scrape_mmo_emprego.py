import asyncio
import json
import os
import re
from playwright.async_api import async_playwright
from datetime import datetime
from decouple import config
import google.generativeai as genai
from typing import cast
# from lxml import html as lxml_html  # Not needed for this implementation

# --- Configuration ---
BASE_URL = "https://emprego.mmo.co.mz"
START_URL = f"{BASE_URL}/vagas-em-mocambique/"
JOBS_DB_FILE = "mmo_emprego_jobs.json"
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

def get_existing_job_urls(filename):
    """Gets a set of already scraped job URLs from the jobs database."""
    jobs_data = load_json_file(filename)
    return set(job.get("source_url") for job in jobs_data if job.get("source_url"))

def build_gemini_prompt(html_content, known_categories, known_locations):
    return f"""
    You are an expert data extraction bot. Your task is to analyze the raw HTML content of a job posting page from MMO Emprego Mozambique and extract the specified information into a clean, valid JSON object.
    
    **Instructions:**
    1. Analyze the entire HTML content provided below.
    2. Extract the following fields and format them into a JSON object: "job_title", "company_name", "location", "category", "publication_date", "expiring_date", "job_description", "tasks_of_the_role", "requirements".
    3. For 'tasks_of_the_role' and 'requirements', extract list items and combine them into a single string, with each item separated by a newline.
    4. For 'category' and 'location', if they are mentioned, use the exact values. If not, assign the most appropriate values from the provided 'Known Categories' and 'Known Locations' lists based on the job context.
    5. For dates, look for patterns like "Publicado X dias atrás", "Expira: mês dia, ano" or similar Portuguese date formats.
    6. If a field cannot be found, use an empty string "" as the value.
    7. Ensure the final output is ONLY a valid JSON object.
    
    **Known Categories:** {json.dumps(known_categories)}
    **Known Locations:** {json.dumps(known_locations)}
    
    **HTML Content:**
    ```html
    {html_content}
    ```
    """

async def get_job_links_from_page(page, page_url, existing_urls):
    """Extract job links from a single listing page."""
    # Only navigate if it's a different URL (for pagination with URL changes)
    current_url = page.url
    if current_url != page_url:
        try:
            await page.goto(page_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2000)  # Wait for dynamic content to load
        except Exception as e:
            print(f"    -! ERROR loading page {page_url}. Reason: {e}")
            return []

    # Extract job links - based on the structure seen in the web search results
    job_links = []
    try:
        # Look for job links in the job listing
        links = await page.locator('a[href*="/vaga/"]').all()
        
        for link in links:
            href = await link.get_attribute('href')
            if href:
                if href.startswith('/'):
                    full_url = f"{BASE_URL}{href}"
                else:
                    full_url = href
                
                # Only add if not already in our collection (the calling function will handle deduplication)
                if full_url not in job_links:
                    job_links.append(full_url)
                    
    except Exception as e:
        print(f"    -! ERROR extracting job links. Reason: {e}")
    
    return job_links

async def get_all_job_links(page, existing_urls):
    """Get all job links from all pages with pagination support."""
    print("--- Phase 1: Collecting Job Links from MMO Emprego ---")
    
    all_job_links = []
    all_job_links_set = set()  # Track unique links
    current_page = 1
    current_url = START_URL
    max_pages = 20  # Safety limit to prevent infinite loops
    no_new_content_count = 0  # Track consecutive attempts with no new content
    
    while current_page <= max_pages:
        print(f"  -> Page {current_page}: Scraping job links from: {current_url}")
        page_links = await get_job_links_from_page(page, current_url, existing_urls)
        
        # Check if we got any new links
        new_links = []
        for link in page_links:
            if link not in all_job_links_set and link not in existing_urls:
                new_links.append(link)
                all_job_links_set.add(link)
        
        if new_links:
            all_job_links.extend(new_links)
            no_new_content_count = 0  # Reset counter since we found new content
            print(f"    -> Found {len(new_links)} new job links (Total: {len(all_job_links)})")
        else:
            no_new_content_count += 1
            print(f"    -> No new job links found ({no_new_content_count}/3)")
            
            # If we haven't found new content for 3 consecutive attempts, stop
            if no_new_content_count >= 3:
                print("  -> No new content found for 3 consecutive attempts. Ending link collection.")
                break
        
        # Check for pagination - look for "Carregar Mais Vagas" button or pagination links
        try:
            # Look for load more button first
            load_more_button = page.locator('text="Carregar Mais Vagas"').first
            if await load_more_button.count() > 0:
                print(f"  -> Found 'Carregar Mais Vagas' button, clicking...")
                
                # Get current number of job elements before clicking
                current_job_count = await page.locator('a[href*="/vaga/"]').count()
                
                await load_more_button.click()
                
                # Wait a bit for potential ads to load and be blocked
                await page.wait_for_timeout(2000)
                
                # Try to close any potential ad popups or overlays
                await close_potential_ads(page)
                
                # Wait for new content to load
                await page.wait_for_timeout(3000)
                
                # Check if new content was actually loaded
                new_job_count = await page.locator('a[href*="/vaga/"]').count()
                
                if new_job_count <= current_job_count:
                    print(f"  -> No new content loaded after clicking button. Job count: {current_job_count} -> {new_job_count}")
                    no_new_content_count += 1
                else:
                    print(f"  -> New content loaded. Job count: {current_job_count} -> {new_job_count}")
                    no_new_content_count = 0
                
                current_page += 1
                continue
            else:
                # Check for numbered pagination
                next_page_link = page.locator(f'a:has-text("{current_page + 1}")').first
                if await next_page_link.count() > 0:
                    current_page += 1
                    current_url = f"{START_URL}?page={current_page}"
                    print(f"  -> Going to page {current_page}")
                    await asyncio.sleep(2)  # Be respectful to the server
                    continue
                else:
                    print("  -> No more pagination options found. Ending link collection.")
                    break
        except Exception as e:
            print(f"  -> Error checking pagination: {e}")
            break
    
    print(f"  -> Total unique job links collected: {len(all_job_links)}")
    return all_job_links

async def check_if_expired_before_ai(page, job_url):
    print(f"  -> Pre-checking for expiry: {job_url}")
    try:
        await page.goto(job_url, wait_until="domcontentloaded", timeout=60000)
        html_content = await page.content()
    except Exception as e:
        print(f"    -! ERROR loading page {job_url} for pre-check. Skipping. Reason: {e}")
        return True, ""

    # Check for expired job indicators in Portuguese
    if any(keyword in html_content.lower() for keyword in ["expirado", "expirou", "vaga encerrada"]):
        print(f"  -! SKIPPING expired job: {job_url}")
        return True, html_content

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

async def setup_ad_blocker(page):
    """Set up ad blocking and resource filtering to improve scraping performance."""
    
    # Block resource types that are typically ads or unnecessary for scraping
    blocked_resource_types = [
        "font",         # Block fonts
        "media"         # Block videos/audio
        # Note: Not blocking images or stylesheets as they may be needed for proper page functionality
    ]
    
    # Block common ad domains and tracking scripts
    blocked_domains = [
        "googleadservices.com",
        "googlesyndication.com", 
        "googletagmanager.com",
        "google-analytics.com",
        "doubleclick.net",
        "googletagservices.com",
        "facebook.com/tr",
        "outbrain.com",
        "taboola.com",
        "adsystem.com",
        "amazon-adsystem.com",
        "adsafeprotected.com",
        "scorecardresearch.com",
        "quantserve.com",
        "moatads.com",
        "adsystem.com",
        "pubmatic.com",
        "rubiconproject.com",
        "openx.net",
        "adsystem.com",
        "ads.yahoo.com",
        "advertising.com",
        "adsystem.com",
        "cloudflareinsights.com",
        "hotjar.com",
        "clarity.ms",
        "iubenda.com"
    ]
    
    async def route_handler(route):
        request_url = route.request.url
        resource_type = route.request.resource_type
        
        # Block unwanted resource types
        if resource_type in blocked_resource_types:
            await route.abort()
            return
            
        # Block known ad domains
        if any(domain in request_url for domain in blocked_domains):
            await route.abort()
            return
            
        # Block requests with ad-related keywords in URL
        ad_keywords = [
            "/ads/", "/ad/", "advertisement", "adsystem", "adservice",
            "doubleclick", "googlesyndication", "googleadservices",
            "/banner", "/popup", "/overlay", "adnxs.com"
        ]
        
        if any(keyword in request_url.lower() for keyword in ad_keywords):
            await route.abort()
            return
            
        # Allow all other requests
        await route.continue_()
    
    # Set up the route handler
    await page.route("**/*", route_handler)
    print("  -> Ad blocker activated")

async def close_potential_ads(page):
    """Attempt to close any ad popups, overlays, or modals that might appear."""
    
    # Common selectors for closing ads/popups
    close_selectors = [
        "button[class*='close']",
        "button[class*='dismiss']", 
        "div[class*='close']",
        "span[class*='close']",
        ".close",
        ".dismiss",
        ".modal-close",
        ".popup-close",
        ".overlay-close",
        "[data-dismiss='modal']",
        "[data-close]",
        "button:has-text('×')",
        "button:has-text('Close')",
        "button:has-text('Fechar')",
        "div:has-text('×')",
        # Common ad network close buttons
        ".adsbygoogle + button",
        ".ad-container button",
        "[id*='close']",
        "[class*='ad-close']"
    ]
    
    for selector in close_selectors:
        try:
            close_button = page.locator(selector).first
            if await close_button.count() > 0:
                await close_button.click()
                print(f"    -> Closed potential ad using selector: {selector}")
                await page.wait_for_timeout(500)  # Brief wait after closing
                break
        except:
            continue  # If clicking fails, try next selector
    
    # Also try to press Escape key to close modals
    try:
        await page.keyboard.press("Escape")
    except:
        pass

async def main():
    print("--- Starting MMO Emprego Scraper ---")
    
    existing_jobs = load_json_file(JOBS_DB_FILE)
    existing_urls = get_existing_job_urls(JOBS_DB_FILE)
    known_categories = sorted(list(set(job.get("category", "") for job in existing_jobs if job.get("category"))))
    known_locations = sorted(list(set(job.get("location", "") for job in existing_jobs if job.get("location"))))
    
    print(f"Loaded {len(existing_jobs)} existing jobs, {len(known_categories)} unique categories, and {len(known_locations)} unique locations.")
    print(f"Found {len(existing_urls)} existing URLs to skip.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()
        
        # Block ads and unnecessary resources
        await setup_ad_blocker(page)

        # Phase 1: Get all job links
        job_links_to_scrape = await get_all_job_links(page, existing_urls)
        
        if not job_links_to_scrape:
            print("\nNo new job postings found. Exiting.")
            await browser.close()
            return
            
        print(f"\n--- Phase 2: Scraping Details for {len(job_links_to_scrape)} New Jobs ---")

        updated_jobs = {job["source_url"]: job for job in existing_jobs}

        for i, job_url in enumerate(job_links_to_scrape):
            print(f"\nProcessing job {i+1}/{len(job_links_to_scrape)}...")
            
            is_expired, html_content = await check_if_expired_before_ai(page, job_url)
            if is_expired:
                continue
            
            job_details = await extract_details_with_gemini(html_content, job_url, known_categories, known_locations)
            
            if job_details:
                updated_jobs[job_url] = job_details
                print(f"    -> Successfully processed: {job_details.get('job_title', 'Unknown Title')}")
            
            # Be respectful to the server
            await asyncio.sleep(2)
        
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
        print(f"New jobs processed: {len(job_links_to_scrape)}")
    else:
        print("\nNo jobs were processed or updated.")

if __name__ == '__main__':
    asyncio.run(main()) 