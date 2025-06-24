import asyncio
import json
import pandas as pd
from playwright.async_api import async_playwright
from selectolax.parser import HTMLParser
from datetime import datetime
import os
import re

# --- Configuration ---
BASE_URL = "https://www.emprego.co.mz"
OUTPUT_JSON_FILE = "emprego_mz_jobs.json"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# --- Helper Functions ---
def get_existing_job_urls(filename):
    """Reads a JSON file to get a set of already scraped job URLs."""
    if not os.path.exists(filename):
        return set()
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            jobs_data = json.load(f)
            return set(job.get("source_url", "") for job in jobs_data if job.get("source_url"))
    except (json.JSONDecodeError, FileNotFoundError):
        return set()

def extract_structured_data(page_content):
    """Extracts the structured JSON-LD data from the job page HTML."""
    parser = HTMLParser(page_content)
    node = parser.css_first('script[type="application/ld+json"]')
    if not node:
        return None
    try:
        # The script content might be malformed, so we clean it before parsing
        clean_json_string = node.text(strip=True)
        return json.loads(clean_json_string)
    except (json.JSONDecodeError, AttributeError):
        return None

def extract_text_between_tags(html, start_tag, end_tag):
    """Extract text content between two HTML tags."""
    pattern = f'{re.escape(start_tag)}(.*?){re.escape(end_tag)}'
    match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""

def clean_html_text(html_text):
    """Remove HTML tags and clean up text."""
    # Remove HTML tags
    clean = re.sub('<[^<]+?>', '', html_text)
    # Clean up whitespace
    clean = re.sub(r'\s+', ' ', clean)
    return clean.strip()

def extract_list_items(html, heading_text):
    """Extract list items that come after a specific heading."""
    # Find the heading and the ul that follows it
    pattern = f'<h6[^>]*>{re.escape(heading_text)}</h6>\\s*<ul[^>]*>(.*?)</ul>'
    match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
    
    if match:
        ul_content = match.group(1)
        # Extract all li items
        li_pattern = r'<li[^>]*>(.*?)</li>'
        li_matches = re.findall(li_pattern, ul_content, re.DOTALL | re.IGNORECASE)
        
        items = []
        for li_content in li_matches:
            clean_text = clean_html_text(li_content)
            if clean_text:
                items.append(clean_text)
        
        return "\n".join(items) if items else "N/A"
    
    return "N/A"

def extract_details_from_html(html_content):
    """Extracts all job details from the page's HTML content using string operations."""
    
    details = {
        "job_title": "N/A",
        "company_name": "N/A",
        "location": "N/A",
        "category": "N/A",
        "publication_date": "N/A",
        "expiring_date": "N/A",
        "job_description": "N/A",
        "tasks_of_the_role": "N/A",
        "requirements": "N/A",
        "benefits": "N/A"
    }

    # Extract job title
    title_match = re.search(r'<h1[^>]*class="[^"]*h3[^"]*"[^>]*>(.*?)</h1>', html_content, re.DOTALL | re.IGNORECASE)
    if title_match:
        details["job_title"] = clean_html_text(title_match.group(1))

    # Extract company name
    company_match = re.search(r'<h3[^>]*class="[^"]*h4[^"]*"[^>]*>(.*?)</h3>', html_content, re.DOTALL | re.IGNORECASE)
    if company_match:
        details["company_name"] = clean_html_text(company_match.group(1))

    # Extract job description (text before first h6 in medium-large-text div)
    desc_match = re.search(r'<div[^>]*class="[^"]*medium-large-text[^"]*"[^>]*>(.*?)<h6', html_content, re.DOTALL | re.IGNORECASE)
    if desc_match:
        details["job_description"] = clean_html_text(desc_match.group(1))

    # Extract tasks, requirements, and benefits
    details["tasks_of_the_role"] = extract_list_items(html_content, "Funções")
    details["requirements"] = extract_list_items(html_content, "Requisitos")
    details["benefits"] = extract_list_items(html_content, "Benefícios")

    # Extract metadata from the sidebar
    # Location
    location_match = re.search(r'<span[^>]*class="[^"]*column-1-3[^"]*"[^>]*>Local</span>\s*<span[^>]*class="[^"]*column-2-3[^"]*"[^>]*>(.*?)</span>', html_content, re.DOTALL | re.IGNORECASE)
    if location_match:
        details["location"] = clean_html_text(location_match.group(1))

    # Category
    category_match = re.search(r'<span[^>]*class="[^"]*column-1-3[^"]*"[^>]*>Categoria</span>\s*<span[^>]*class="[^"]*column-2-3[^"]*"[^>]*>(.*?)</span>', html_content, re.DOTALL | re.IGNORECASE)
    if category_match:
        details["category"] = clean_html_text(category_match.group(1))

    # Publication date
    pub_match = re.search(r'<span[^>]*class="[^"]*column-1-3[^"]*"[^>]*>Publicado</span>\s*<span[^>]*class="[^"]*column-2-3[^"]*"[^>]*>(.*?)</span>', html_content, re.DOTALL | re.IGNORECASE)
    if pub_match:
        details["publication_date"] = clean_html_text(pub_match.group(1))

    # Expiry date
    exp_match = re.search(r'<span[^>]*class="[^"]*column-1-3[^"]*"[^>]*>Expira</span>\s*<span[^>]*class="[^"]*column-2-3[^"]*"[^>]*>(.*?)</span>', html_content, re.DOTALL | re.IGNORECASE)
    if exp_match:
        details["expiring_date"] = clean_html_text(exp_match.group(1))

    return details

# --- Main Scraping Logic ---
async def scrape_job_details(page, job_url):
    """Visits a single job page and extracts all required details."""
    print(f"  -> Scraping detail page: {job_url}")
    await page.goto(job_url, wait_until="domcontentloaded", timeout=90000)
    html = await page.content()
    
    # First, try to get structured data
    structured_data = extract_structured_data(html)
    
    # Second, always get HTML data as a primary source or fallback
    html_details = extract_details_from_html(html)

    # Combine data, giving preference to structured data if it exists
    if structured_data:
        job_data = {
            "job_title": structured_data.get("title", html_details["job_title"]),
            "company_name": structured_data.get("hiringOrganization", {}).get("name", html_details["company_name"]),
            "location": structured_data.get("jobLocation", {}).get("address", {}).get("addressLocality", html_details["location"]),
            "category": structured_data.get("occupationalCategory", html_details["category"]),
            "publication_date": structured_data.get("datePosted", html_details["publication_date"]),
            "expiring_date": structured_data.get("validThrough", html_details["expiring_date"]),
            "job_description": structured_data.get("description", html_details["job_description"]),
            "tasks_of_the_role": html_details["tasks_of_the_role"],
            "requirements": html_details["requirements"],
            "benefits": html_details["benefits"],
            "source_url": job_url,
        }
    else:
        # If no structured data, rely entirely on HTML extraction
        print(f"    -! Warning: Could not find structured data for {job_url}. Falling back to HTML parsing.")
        html_details["source_url"] = job_url
        job_data = html_details

    # Check for expired jobs using the extracted date
    expiry_date_str = job_data.get("expiring_date", "")
    if expiry_date_str and expiry_date_str.lower() != 'expirado':
        try:
            # Handle dates like '26.06.2025' from HTML
            expiry_date = datetime.strptime(expiry_date_str, "%d.%m.%Y").date()
            if expiry_date < datetime.now().date():
                print(f"    -! Expired job on {expiry_date_str}. Skipping.")
                return None
        except ValueError:
            try:
                # Handle dates like '2025-06-26' from JSON
                expiry_date = datetime.strptime(expiry_date_str, "%Y-%m-%d").date()
                if expiry_date < datetime.now().date():
                    print(f"    -! Expired job on {expiry_date_str}. Skipping.")
                    return None
            except ValueError:
                print(f"    -! Warning: Could not parse expiry date '{expiry_date_str}'. Proceeding anyway.")
                pass
    elif expiry_date_str.lower() == 'expirado':
        print(f"    -! Expired job marked as 'Expirado'. Skipping.")
        return None
        
    return job_data

async def get_all_job_links(page, existing_urls):
    """Gets all unique job links from all categories and pages, skipping known URLs."""
    new_job_urls = set()

    print("--- Phase 1: Discovering Categories ---")
    await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
    category_links = await page.locator('div.content-container-1-4 h2:has-text("Categoria") + ul a').all()
    category_urls = [await link.get_attribute('href') for link in category_links]
    print(f"Found {len(category_urls)} job categories.")

    print("\n--- Phase 2: Collecting Job Links ---")
    for category_url in category_urls:
        current_url = category_url
        print(f"  Scraping category: {current_url}")
        page_num = 1
        consecutive_failures = 0
        max_consecutive_failures = 3
        
        while True:
            if not current_url.startswith('http'):
                current_url = f"{BASE_URL}{current_url}"
            
            # Add retry logic for network errors
            retry_count = 0
            max_retries = 3
            page_loaded = False
            
            while retry_count < max_retries and not page_loaded:
                try:
                    await page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
                    page_loaded = True
                    consecutive_failures = 0  # Reset on successful load
                except Exception as e:
                    retry_count += 1
                    consecutive_failures += 1
                    print(f"    -! Error loading page (attempt {retry_count}/{max_retries}): {e}")
                    
                    if retry_count < max_retries:
                        print(f"    -> Retrying in 5 seconds...")
                        await asyncio.sleep(5)
                    else:
                        print(f"    -! Failed to load page after {max_retries} attempts. Skipping to next page/category.")
                        break
            
            if not page_loaded:
                if consecutive_failures >= max_consecutive_failures:
                    print(f"    -! Too many consecutive failures ({consecutive_failures}). Moving to next category.")
                    break
                else:
                    # Try to continue to next page in this category
                    page_num += 1
                    current_url = f"{current_url.split('/page/')[0]}/page/{page_num}/"
                    continue
            
            # Use the selector from the technical specification to find job links
            job_link_elements = await page.locator('li.clearfix h3.normal-text a').all()
            
            if not job_link_elements:
                print("    - No job links found on this page. Moving to next category.")
                break

            found_new_links_on_page = False
            for link in job_link_elements:
                job_href = await link.get_attribute('href')
                if job_href and job_href not in existing_urls and job_href not in new_job_urls:
                    new_job_urls.add(job_href)
                    found_new_links_on_page = True
            
            print(f"    - Page {page_num}: Found {len(job_link_elements)} links. Collected {len(new_job_urls)} new links so far.")
            
            next_button = page.locator('div.pagination a.nextpostslink')
            if await next_button.count() > 0:
                next_page_href = await next_button.get_attribute('href')
                current_url = next_page_href
                page_num += 1
                # Add a small delay between pages to be more respectful
                await asyncio.sleep(2)
            else:
                print("    - No more pages in this category.")
                break
    
    return list(new_job_urls)

async def main():
    # --- TEST MODE FOR A SINGLE URL ---
    # This section is for testing the scraping of a single page.
    # The normal multi-page scraping is temporarily bypassed.
    # test_url = "https://www.emprego.co.mz/vaga/agentes-de-atendimento-de-call-center-changana-2/"

    # print(f"--- Running in Test Mode ---")
    # print(f"Scraping single URL: {test_url}")
    
    # async with async_playwright() as p:
    #     browser = await p.chromium.launch(headless=True)
    #     context = await browser.new_context(user_agent=USER_AGENT)
    #     page = await context.new_page()

    #     job_details = await scrape_job_details(page, test_url)
        
    #     await browser.close()

    # if job_details:
    #     print("\n--- Scraping Complete: Extracted Data ---")
    #     # Use pprint for a clean, readable dictionary output
    #     from pprint import pprint
    #     pprint(job_details)
        
    #     # Optionally, save the single result to a test CSV
    #     df = pd.DataFrame([job_details])
    #     test_csv_file = "test_output.csv"
    #     print(f"\n--- Saving to {test_csv_file} ---")
    #     df.to_csv(test_csv_file, index=False, encoding='utf-8-sig')
    #     print("Done.")
    # else:
    #     print("\n--- Failed to scrape details from the test URL. ---")

    # --- ORIGINAL MAIN FUNCTION ---
    # The original logic is commented out below. Uncomment it to run the full scraper.
    existing_urls = get_existing_job_urls(OUTPUT_JSON_FILE)
    print(f"Found {len(existing_urls)} existing jobs in {OUTPUT_JSON_FILE}. These will be skipped.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)
        page = await context.new_page()

        job_links_to_scrape = await get_all_job_links(page, existing_urls)
        total_links = len(job_links_to_scrape)
        
        if not total_links:
            print("\nNo new job postings found. Exiting.")
            await browser.close()
            return

        print(f"\n--- Phase 3: Scraping Details for {total_links} New Jobs ---")
        all_new_jobs_data = []
        for i, job_url in enumerate(job_links_to_scrape):
            print(f"Processing job {i+1}/{total_links}...")
            try:
                job_details = await scrape_job_details(page, job_url)
                if job_details:
                    all_new_jobs_data.append(job_details)
            except Exception as e:
                print(f"  -! ERROR: Failed to scrape {job_url}. Reason: {e}")
        
        await browser.close()

    if all_new_jobs_data:
        print(f"\n--- Scraping Complete: Extracted {len(all_new_jobs_data)} new active jobs. ---")
        new_jobs_data = all_new_jobs_data
        
        # Save to JSON file
        if save_jobs_to_json(new_jobs_data, OUTPUT_JSON_FILE):
            print(f"Saved {len(new_jobs_data)} new active jobs to {OUTPUT_JSON_FILE}")
        else:
            print("\nFailed to save jobs to JSON file.")
    else:
        print("\nNo new active jobs were scraped in this run.")

def save_jobs_to_json(jobs_data, filename):
    """Save jobs data to JSON file."""
    try:
        # Load existing jobs if file exists
        existing_jobs = []
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                existing_jobs = json.load(f)
        
        # Combine existing and new jobs
        all_jobs = existing_jobs + jobs_data
        
        # Save to JSON file with proper formatting
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(all_jobs, f, ensure_ascii=False, indent=2)
        
        return True
    except Exception as e:
        print(f"Error saving to JSON: {e}")
        return False

if __name__ == '__main__':
    asyncio.run(main())