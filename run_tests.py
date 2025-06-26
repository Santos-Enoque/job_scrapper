import asyncio
import os
import json
from playwright.async_api import async_playwright
from decouple import config
from scrape_emprego_mz import scrape_job_details, USER_AGENT

# Load environment variables
GEMINI_API_KEY = config("GEMINI_API_KEY", default=None)

EXPIRED_JOB_URL = "https://www.emprego.co.mz/vaga/arquitecto-de-sistemas-de-informacao/"
VALID_JOB_URL = "https://www.emprego.co.mz/vaga/analista-preparador-mecanico/"

async def run_test_suite():
    """
    Runs a suite of tests for the scraper, including the new AI-powered extraction.
    """
    print("--- Starting Scraper Test Suite ---")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)
        page = await context.new_page()

        # Test 1: Ensure expired jobs are skipped
        print(f"\n[Test 1] Checking expired URL: {EXPIRED_JOB_URL}")
        expired_job_details = await scrape_job_details(page, EXPIRED_JOB_URL)
        
        if expired_job_details is None:
            print("[SUCCESS] Expired job was correctly skipped.")
        else:
            print("[FAILURE] Expired job was not skipped.")
            await browser.close()
            return

        # Test 2: Ensure valid jobs are scraped correctly using Gemini
        print(f"\n[Test 2] Checking valid URL with AI: {VALID_JOB_URL}")
        
        if not GEMINI_API_KEY or GEMINI_API_KEY == "YOUR_API_KEY_HERE":
            print("[SKIPPED] GEMINI_API_KEY not found or not set. Skipping AI extraction test.")
        else:
            valid_job_details = await scrape_job_details(page, VALID_JOB_URL)
            
            if valid_job_details:
                # Check that all expected keys are present in the returned dictionary
                expected_keys = [
                    "job_title", "company_name", "location", "category",
                    "publication_date", "expiring_date", "job_description",
                    "tasks_of_the_role", "requirements", "source_url"
                ]
                
                if all(key in valid_job_details for key in expected_keys):
                    print("[SUCCESS] Valid job data was successfully extracted by Gemini and contains all expected keys.")
                    print("Extracted Title:", valid_job_details.get("job_title"))
                    
                    # Save the output to a JSON file for inspection
                    with open("test_output.json", "w", encoding="utf-8") as f:
                        json.dump(valid_job_details, f, ensure_ascii=False, indent=2)
                    print(" -> Saved AI output to test_output.json for inspection.")
                else:
                    print("[FAILURE] Gemini output was missing some expected keys.")
            else:
                print("[FAILURE] Valid job was not scraped by Gemini.")
            
        await browser.close()

    print("\n--- Test Suite Finished ---")

if __name__ == '__main__':
    asyncio.run(run_test_suite()) 