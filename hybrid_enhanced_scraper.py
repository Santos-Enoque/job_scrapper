import asyncio
import json
import os
import random
import time
from datetime import datetime
from typing import List, Dict, Optional, Any

# Core dependencies
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LLMExtractionStrategy, BrowserConfig, LLMConfig
from fake_useragent import UserAgent
from decouple import config
import google.generativeai as genai

# Enhanced configuration
class HybridScraperConfig:
    """Configuration class for the hybrid enhanced scraper"""
    
    def __init__(self):
        # API Keys
        self.gemini_api_key = config('GEMINI_API_KEY', default=None)
        self.evomi_api_key = config('EVOMI_API_KEY', default=None)  # Add your Evomi API key to .env
        
        # Scraping settings
        self.user_agent_generator = UserAgent()
        self.base_delay = 2  # Base delay between requests
        self.max_delay = 8   # Maximum delay between requests
        self.max_retries = 3
        self.timeout = 60
        
        # Proxy settings (for Evomi or other proxy services)
        self.use_proxy = bool(self.evomi_api_key)
        self.proxy_rotation = True
        
        # Configure Gemini
        if self.gemini_api_key:
            genai.configure(api_key=self.gemini_api_key)
            self.model = genai.GenerativeModel('gemini-1.5-flash')
        else:
            self.model = None
            
        print(f"🚀 Hybrid Enhanced Scraper initialized:")
        print(f"  - Gemini AI: {'✅' if self.model else '❌'}")
        print(f"  - Proxy Support: {'✅' if self.use_proxy else '❌'}")

class HybridJobScraper:
    """Hybrid job scraper with AI-powered extraction and multi-site support"""
    
    def __init__(self, config: HybridScraperConfig):
        self.config = config
        self.session_id = f"session_{int(time.time())}"
        self.results = []
        
    async def get_proxy_config(self) -> Optional[Dict]:
        """Get proxy configuration for Evomi or other proxy services"""
        if not self.config.use_proxy:
            return None
            
        # Evomi proxy configuration (adjust based on your Evomi setup)
        proxy_config = {
            "server": "premium-residential.evomi.com:8000",  # Example Evomi endpoint
            "username": config('EVOMI_USERNAME', default=''),
            "password": config('EVOMI_PASSWORD', default=''),
        }
        
        if proxy_config["username"] and proxy_config["password"]:
            return proxy_config
        return None
    
    def get_random_user_agent(self) -> str:
        """Get a random user agent to avoid detection"""
        return self.config.user_agent_generator.random
    
    async def intelligent_delay(self):
        """Implement intelligent delays to avoid rate limiting"""
        delay = random.uniform(self.config.base_delay, self.config.max_delay)
        await asyncio.sleep(delay)
    
    async def enhanced_extract_with_ai(self, url: str, extraction_prompt: str, stealth_level: str = "medium") -> List[Dict]:
        """Extract job data using Crawl4AI with configurable stealth levels"""
        
        print(f"🕷️  Extracting from: {url} (Stealth: {stealth_level})")
        
        # Configure browser settings based on stealth level
        if stealth_level == "low":
            extra_args = ["--no-sandbox", "--disable-dev-shm-usage"]
        elif stealth_level == "medium":
            extra_args = [
                "--no-sandbox", 
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security"
            ]
        else:  # high stealth
            extra_args = [
                "--no-sandbox", 
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding"
            ]
        
        browser_config = BrowserConfig(
            headless=True,
            user_agent=self.get_random_user_agent(),
            viewport_width=1920,
            viewport_height=1080,
            accept_downloads=False,
            proxy_config=await self.get_proxy_config(),
            extra_args=extra_args,
            ignore_https_errors=True
        )
        
        # AI extraction strategy
        llm_config = LLMConfig(
            provider="gemini/gemini-1.5-flash",
            api_token=self.config.gemini_api_key
        )
        
        extraction_strategy = LLMExtractionStrategy(
            llm_config=llm_config,
            instruction=extraction_prompt,
            schema={
                "type": "object",
                "properties": {
                    "jobs": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "job_title": {"type": "string"},
                                "company_name": {"type": "string"},
                                "location": {"type": "string"},
                                "category": {"type": "string"},
                                "publication_date": {"type": "string"},
                                "expiring_date": {"type": "string"},
                                "job_description": {"type": "string"},
                                "tasks_of_the_role": {"type": "string"},
                                "requirements": {"type": "string"},
                                "source_url": {"type": "string"}
                            }
                        }
                    }
                }
            },
            verbose=True
        )
        
        # Configure crawler run settings based on stealth level
        timeout_multiplier = {"low": 1, "medium": 1.5, "high": 2}
        delay_multiplier = {"low": 1, "medium": 2, "high": 3}
        
        crawler_run_config = CrawlerRunConfig(
            extraction_strategy=extraction_strategy,
            page_timeout=int(self.config.timeout * 1000 * timeout_multiplier[stealth_level]),
            wait_until="domcontentloaded" if stealth_level == "low" else "networkidle",
            delay_before_return_html=2.0 * delay_multiplier[stealth_level],
            verbose=True
        )
        
        async with AsyncWebCrawler(config=browser_config) as crawler:
            try:
                result = await crawler.arun(url=url, config=crawler_run_config)
                
                if result.success and result.extracted_content:
                    try:
                        # Handle different formats of extracted_content
                        if isinstance(result.extracted_content, list):
                            # If it's a list, try to get the first item
                            content = result.extracted_content[0] if result.extracted_content else ""
                        else:
                            content = result.extracted_content
                        
                        if isinstance(content, str):
                            extracted_data = json.loads(content)
                        else:
                            extracted_data = content
                        
                        jobs = extracted_data.get('jobs', []) if isinstance(extracted_data, dict) else []
                        
                        # Add source URL to each job if not present
                        for job in jobs:
                            if isinstance(job, dict) and not job.get('source_url'):
                                job['source_url'] = url
                        
                        print(f"✅ Successfully extracted {len(jobs)} jobs")
                        return jobs
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"❌ Failed to parse extracted content: {e}")
                        print(f"Debug - Content type: {type(result.extracted_content)}")
                        print(f"Debug - Content: {result.extracted_content}")
                        return []
                else:
                    print(f"❌ Extraction failed: {result.error_message if result else 'Unknown error'}")
                    return []
                    
            except Exception as e:
                print(f"❌ Crawler error: {e}")
                return []
    
    def save_results(self, jobs: List[Dict], filename: str):
        """Save scraped jobs to JSON file"""
        
        # Remove duplicates based on source_url
        unique_jobs = []
        seen_urls = set()
        
        for job in jobs:
            url = job.get('source_url', '')
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_jobs.append(job)
        
        # Save to file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(unique_jobs, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Saved {len(unique_jobs)} unique jobs to {filename}")
        return unique_jobs

# MMO Emprego Scraper Implementation  
class MMOEmpregoEnhancedScraper(HybridJobScraper):
    """Enhanced scraper for MMO Emprego using AI extraction"""
    
    def __init__(self):
        config = HybridScraperConfig()
        super().__init__(config)
        
        self.base_url = "https://emprego.mmo.co.mz"
        self.main_url = f"{self.base_url}/vagas-em-mocambique/"
        
    def get_mmo_extraction_prompt(self) -> str:
        """Get the extraction prompt for MMO Emprego"""
        return """
        You are an expert job data extractor. Analyze this MMO Emprego page and extract ALL job postings from the job listings section.

        I can see there's a section called "Vagas de emprego hoje" with multiple job listings. Each job listing follows this pattern:
        - Job title (like "Technical Lead Consultant")
        - Company name (like "YLABS") 
        - Location (like "Mozambique (Remote)" or "Maputo")
        - Publication status (like "nova" or "Publicado X dias atrás")
        - Expiration date (like "Expira: julho 5, 2025")
        - URL link (like "/vaga/technical-lead-consultant-ylabs/")

        For each job posting, extract:
        - job_title: The job title/position name
        - company_name: The company/organization name  
        - location: Job location
        - category: Infer category from job title (e.g., "Technology", "Administration", "Finance", etc.)
        - publication_date: Extract from "Publicado X dias atrás" or use "nova" for new posts
        - expiring_date: Extract the date after "Expira:"
        - job_description: Leave empty for now (will be filled from individual pages)
        - tasks_of_the_role: Leave empty for now 
        - requirements: Leave empty for now
        - source_url: Convert relative URLs to full URLs (add "https://emprego.mmo.co.mz" prefix)

        IMPORTANT: Return ONLY a valid JSON object in this exact format:
        {
          "jobs": [
            {
              "job_title": "string",
              "company_name": "string", 
              "location": "string",
              "category": "string",
              "publication_date": "string",
              "expiring_date": "string", 
              "job_description": "",
              "tasks_of_the_role": "",
              "requirements": "",
              "source_url": "string"
            }
          ]
        }

        Extract ALL visible job postings. Do not include any explanatory text - return only the JSON.
        """
    
    async def scrape_mmo_jobs(self, max_pages: int = 3) -> List[Dict]:
        """Main method to scrape MMO Emprego jobs"""
        
        print("🏢 Starting Enhanced MMO Emprego Scraper")
        print("=" * 60)
        
        all_jobs = []
        
        # Step 1: Extract jobs from main listing page
        print("\n📋 Step 1: Extracting jobs from main listings page...")
        main_page_jobs = await self.enhanced_extract_with_ai(
            self.main_url, 
            self.get_mmo_extraction_prompt(),
            stealth_level="low"  # MMO doesn't need high stealth
        )
        
        if main_page_jobs:
            all_jobs.extend(main_page_jobs)
        
        # Step 2: Try to handle pagination by extracting individual job URLs
        if main_page_jobs:
            print(f"\n🔗 Step 2: Extracting detailed job information...")
            
            # Get individual job URLs from the extracted jobs
            job_urls = [job.get('source_url') for job in main_page_jobs if job.get('source_url')]
            
            if job_urls:
                print(f"Found {len(job_urls)} job URLs for detailed extraction")
                
                # Extract detailed information from first few job pages (to avoid overwhelming)
                limited_urls = job_urls[:10]  # Limit for demo
                
                detail_extraction_prompt = """
                You are an expert job data extractor. Analyze this individual MMO Emprego job posting page and extract comprehensive job details.
                
                Extract the following information:
                - job_title: Complete job title
                - company_name: Company/organization name
                - location: Full location details
                - category: Job category/sector
                - publication_date: Posting date
                - expiring_date: Application deadline
                - job_description: Complete job description
                - tasks_of_the_role: All responsibilities and duties (combine into single string with newlines)
                - requirements: All qualifications, experience requirements, etc. (combine into single string with newlines)
                - source_url: Current page URL
                
                Be thorough and extract all relevant details from this job posting page.
                Return as JSON with a "jobs" array containing one comprehensive job object.
                """
                
                detailed_jobs = []
                for i, job_url in enumerate(limited_urls, 1):
                    print(f"  Processing detailed job {i}/{len(limited_urls)}: {job_url}")
                    
                    try:
                        job_details = await self.enhanced_extract_with_ai(
                            job_url, 
                            detail_extraction_prompt,
                            stealth_level="low"
                        )
                        
                        if job_details:
                            detailed_jobs.extend(job_details)
                        
                        # Small delay between requests
                        await self.intelligent_delay()
                        
                    except Exception as e:
                        print(f"    ❌ Error processing {job_url}: {e}")
                        continue
                
                if detailed_jobs:
                    # Replace basic jobs with detailed versions where available
                    detailed_urls = {job.get('source_url') for job in detailed_jobs}
                    
                    # Keep detailed jobs and non-detailed jobs
                    final_jobs = detailed_jobs + [job for job in main_page_jobs if job.get('source_url') not in detailed_urls]
                    all_jobs = final_jobs
        
        # Save results
        if all_jobs:
            filename = f"mmo_emprego_enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            unique_jobs = self.save_results(all_jobs, filename)
            return unique_jobs
        else:
            print("❌ No jobs were successfully extracted")
            return []

# UN Jobs Scraper (For future enhancement with stronger evasion)
class UNJobsEnhancedScraper(HybridJobScraper):
    """Enhanced scraper for UN Jobs (requires stronger anti-detection)"""
    
    def __init__(self):
        config = HybridScraperConfig()
        super().__init__(config)
        
        self.base_url = "https://unjobs.org"
        self.mozambique_url = f"{self.base_url}/duty_stations/mozambique"
    
    async def scrape_un_jobs_with_evasion(self) -> List[Dict]:
        """Scrape UN Jobs with maximum evasion techniques"""
        
        print("🌍 Starting Enhanced UN Jobs Scraper (High Stealth Mode)")
        print("=" * 60)
        
        extraction_prompt = """
        You are an expert job data extractor. Analyze this UN Jobs page and extract ALL job postings.
        
        For each job posting found, extract:
        - job_title: The job title/position name
        - company_name: The organization/agency name (usually UN agency)
        - location: Job location (city, country)
        - category: Job category/field
        - publication_date: When the job was posted
        - expiring_date: Application deadline
        - job_description: Brief job summary/description
        - tasks_of_the_role: Main responsibilities (combine into single string with newlines)
        - requirements: Job requirements/qualifications (combine into single string with newlines)
        - source_url: Full URL to the job posting
        
        Return the data as a JSON object with a "jobs" array containing all extracted job objects.
        """
        
        jobs = await self.enhanced_extract_with_ai(
            self.mozambique_url,
            extraction_prompt,
            stealth_level="high"  # Use maximum stealth for UN Jobs
        )
        
        if jobs:
            filename = f"un_jobs_enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            unique_jobs = self.save_results(jobs, filename)
            return unique_jobs
        else:
            print("❌ UN Jobs extraction failed - likely blocked by Cloudflare")
            return []

# Main execution function
async def main():
    """Main function to demonstrate the hybrid enhanced scraper"""
    
    print("🚀 Hybrid Enhanced Job Scraper with Crawl4AI")
    print("=" * 60)
    
    # Test with MMO Emprego (working site)
    print("\n🏢 Testing with MMO Emprego (should work)...")
    mmo_scraper = MMOEmpregoEnhancedScraper()
    mmo_jobs = await mmo_scraper.scrape_mmo_jobs()
    
    if mmo_jobs:
        print(f"\n✅ Successfully scraped {len(mmo_jobs)} MMO jobs!")
        print("\nSample MMO job titles:")
        for i, job in enumerate(mmo_jobs[:5], 1):
            title = job.get('job_title', 'N/A')[:50]
            company = job.get('company_name', 'N/A')[:30]
            print(f"  {i}. {title}... @ {company}")
    
    # Test with UN Jobs (likely blocked)
    print(f"\n🌍 Testing with UN Jobs (likely blocked by Cloudflare)...")
    un_scraper = UNJobsEnhancedScraper()
    un_jobs = await un_scraper.scrape_un_jobs_with_evasion()
    
    if un_jobs:
        print(f"\n✅ Successfully scraped {len(un_jobs)} UN jobs!")
        print("\nSample UN job titles:")
        for i, job in enumerate(un_jobs[:3], 1):
            title = job.get('job_title', 'N/A')[:50]
            company = job.get('company_name', 'N/A')[:30]
            print(f"  {i}. {title}... @ {company}")
    else:
        print("\n⚠️  UN Jobs blocked (as expected) - would need proxy/Evomi for bypass")
    
    total_jobs = len(mmo_jobs) + len(un_jobs)
    print(f"\n🎉 Total jobs scraped: {total_jobs}")
    
    return {"mmo_jobs": mmo_jobs, "un_jobs": un_jobs}

if __name__ == "__main__":
    asyncio.run(main()) 