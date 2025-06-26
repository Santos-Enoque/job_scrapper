import asyncio
import json
from playwright.async_api import async_playwright
from decouple import config
from scrape_un_jobs import scrape_job_details_un, USER_AGENT, CATEGORIES_FILE

# --- Configuration ---
VALID_JOB_URL = "https://unjobs.org/vacancies/1750200455931"
TEST_OUTPUT_FILE = "test_un_output.json"
GEMINI_API_KEY = config("GEMINI_API_KEY", default=None)

async def main():
    """
    Tests that the UN job scraper correctly extracts data from a valid job page.
    """
    if not GEMINI_API_KEY:
        print("Skipping test: GEMINI_API_KEY is not configured.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)
        page = await context.new_page()

        # Load categories for the scraper to use
        try:
            with open(CATEGORIES_FILE, 'r', encoding='utf-8') as f:
                categories = json.load(f)
        except FileNotFoundError:
            categories = []

        print(f"\n--- Testing UN Scraper on URL: {VALID_JOB_URL} ---")
        job_details = await scrape_job_details_un(page, VALID_JOB_URL, categories)

        await browser.close()

    # --- Assertions ---
    assert job_details is not None, "Scraper returned no data."
    assert job_details.get("job_title") not in ["", "Not Found"], "Job title was not extracted."
    assert job_details.get("company_name") not in ["", "Not Found"], "Company name was not extracted."
    assert job_details.get("location") not in ["", "Not Found"], "Location was not extracted."
    assert job_details.get("job_description"), "Job description is empty."
    assert job_details.get("category") != "Uncategorized", "AI categorization failed or defaulted."

    print("\n--- Assertions Passed: Scraper extracted key information successfully. ---")
    
    # Save the output for manual inspection
    with open(TEST_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(job_details, f, ensure_ascii=False, indent=2)
    print(f" -> Saved test output to {TEST_OUTPUT_FILE} for inspection.")

if __name__ == "__main__":
    asyncio.run(main()) 