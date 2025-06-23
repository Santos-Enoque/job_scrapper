import asyncio
import json
import pandas as pd
from playwright.async_api import async_playwright
from selectolax.parser import HTMLParser
from datetime import datetime
import os

# --- Configuration ---
BASE_URL = "https://www.emprego.co.mz"
OUTPUT_CSV_FILE = "emprego_mz_jobs.csv"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# --- Helper Functions ---
def get_existing_job_urls(filename):
    """Reads a CSV file to get a set of already scraped job URLs."""
    if not os.path.exists(filename):
        return set()
    try:
        df = pd.read_csv(filename)
        if "source_url" in df.columns:
            return set(df["source_url"])
    except pd.errors.EmptyDataError:
        return set()
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

def extract_details_from_html(parser):
    """Extracts 'Funções' and 'Requisitos' which are not in the JSON-LD."""
    details = {
        "tasks_of_the_role": "N/A",
        "requirements": "N/A"
    }
    
    def get_list_items(heading_text):
        items = []
        # Find the heading (h6) and then its next sibling (ul)
        heading_node = parser.css_first(f'h6:contains("{heading_text}")')
        if heading_node:
            list_node = heading_node.next
            if list_node and list_node.tag == 'ul':
                for item in list_node.css('li'):
                    items.append(item.text(strip=True))
        return "\n".join(items) if items else "N/A"

    details["tasks_of_the_role"] = get_list_items("Funções")
    details["requirements"] = get_list_items("Requisitos")
    return details

# --- Main Scraping Logic ---
async def scrape_job_details(page, job_url):
    """Visits a single job page and extracts all required details."""
    print(f"  -> Scraping detail page: {job_url}")
    await page.goto(job_url, wait_until="domcontentloaded", timeout=90000)
    html = await page.content()
    
    structured_data = extract_structured_data(html)
    if not structured_data:
        print(f"    -! Warning: Could not find structured data for {job_url}.")
        return None

    # Check if the job has expired from the JSON data
    try:
        expiry_date_str = structured_data.get("validThrough", "")
        if expiry_date_str and expiry_date_str.lower() != 'expirado':
            expiry_date = datetime.strptime(expiry_date_str, "%Y-%m-%d").date()
            if expiry_date < datetime.now().date():
                print(f"    -! Expired job on {expiry_date_str}. Skipping.")
                return None
    except (ValueError, TypeError):
         # If validThrough is missing, malformed, or 'Expirado', we can still proceed
        pass

    html_details = extract_details_from_html(HTMLParser(html))

    job_data = {
        "job_title": structured_data.get("title", "N/A"),
        "company_name": structured_data.get("hiringOrganization", {}).get("name", "N/A"),
        "location": structured_data.get("jobLocation", {}).get("address", {}).get("addressLocality", "N/A"),
        "category": structured_data.get("occupationalCategory", "N/A"),
        "publication_date": structured_data.get("datePosted", "N/A"),
        "expiring_date": structured_data.get("validThrough", "N/A"),
        "job_description": structured_data.get("description", "N/A"),
        "tasks_of_the_role": html_details["tasks_of_the_role"],
        "requirements": html_details["requirements"],
        "source_url": job_url,
    }
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
        while True:
            if not current_url.startswith('http'):
                current_url = f"{BASE_URL}{current_url}"
            await page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
            
            # Corrected selector to find the job links within the list
            job_link_elements = await page.locator('ul.content-display > li > div > h3 > a').all()
            
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
            else:
                print("    - No more pages in this category.")
                break
    
    return list(new_job_urls)

async def main():
    existing_urls = get_existing_job_urls(OUTPUT_CSV_FILE)
    print(f"Found {len(existing_urls)} existing jobs in {OUTPUT_CSV_FILE}. These will be skipped.")

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
        new_df = pd.DataFrame(all_new_jobs_data)
        
        # Append to existing CSV if it exists, otherwise create new
        if os.path.exists(OUTPUT_CSV_FILE):
            new_df.to_csv(OUTPUT_CSV_FILE, mode='a', header=False, index=False, encoding='utf-8-sig')
            print(f"Appended new data to {OUTPUT_CSV_FILE}")
        else:
            new_df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8-sig')
            print(f"Created new data file at {OUTPUT_CSV_FILE}")
    else:
        print("\nNo new active jobs were scraped in this run.")

if __name__ == '__main__':
    asyncio.run(main())