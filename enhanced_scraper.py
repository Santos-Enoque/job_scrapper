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
class EnhancedScraperConfig:
    """Configuration class for the enhanced scraper"""
    
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
            
        print(f"Enhanced Scraper initialized:")
        print(f"  - Gemini AI: {'✅' if self.model else '❌'}")
        print(f"  - Proxy Support: {'✅' if self.use_proxy else '❌'}")

class EnhancedJobScraper:
    """Enhanced job scraper with AI-powered extraction and anti-detection features"""
    
    def __init__(self, config: EnhancedScraperConfig):
        self.config = config
        self.session_id = f"session_{int(time.time())}"
        self.results = []
        
    async def get_proxy_config(self) -> Optional[Dict]:
        """Get proxy configuration for Evomi or other proxy services"""
        if not self.config.use_proxy:
            return None
            
        # Evomi proxy configuration (adjust based on your Evomi setup)
        # You'll need to replace these with your actual Evomi credentials
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
    
    async def extract_jobs_with_ai(self, url: str, extraction_prompt: str) -> List[Dict]:
        """Extract job data using Crawl4AI with AI-powered extraction"""
        
        print(f"🕷️  Extracting jobs from: {url}")
        
        # Configure browser settings
        browser_config = BrowserConfig(
            headless=True,
            user_agent=self.get_random_user_agent(),
            viewport_width=1920,
            viewport_height=1080,
            accept_downloads=False,
            proxy_config=await self.get_proxy_config(),
            # Enhanced stealth settings
            extra_args=[
                "--no-sandbox", 
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security"
            ]
        )
        
        # AI extraction strategy
        llm_config = LLMConfig(
            provider="google_genai/gemini-1.5-flash",
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
        
        # Configure the crawler run settings
        crawler_run_config = CrawlerRunConfig(
            extraction_strategy=extraction_strategy,
            page_timeout=self.config.timeout * 1000,
            wait_until="networkidle",
            wait_for=None,
            delay_before_return_html=3.0,
            verbose=True
        )
        
        async with AsyncWebCrawler(config=browser_config) as crawler:
            try:
                # Crawl the page with AI extraction
                result = await crawler.arun(
                    url=url,
                    config=crawler_run_config
                )
                
                if result.success and result.extracted_content:
                    try:
                        extracted_data = json.loads(result.extracted_content)
                        jobs = extracted_data.get('jobs', [])
                        
                        # Add source URL to each job if not present
                        for job in jobs:
                            if not job.get('source_url'):
                                job['source_url'] = url
                        
                        print(f"✅ Successfully extracted {len(jobs)} jobs")
                        return jobs
                    except json.JSONDecodeError as e:
                        print(f"❌ Failed to parse extracted JSON: {e}")
                        return []
                else:
                    print(f"❌ Extraction failed: {result.error_message if result else 'Unknown error'}")
                    return []
                    
            except Exception as e:
                print(f"❌ Crawler error: {e}")
                return []
    
    async def scrape_job_listings(self, base_url: str, list_urls: List[str], extraction_prompt: str) -> List[Dict]:
        """Scrape multiple job listing pages"""
        
        all_jobs = []
        
        for i, url in enumerate(list_urls, 1):
            print(f"\n📋 Processing page {i}/{len(list_urls)}")
            
            try:
                jobs = await self.extract_jobs_with_ai(url, extraction_prompt)
                all_jobs.extend(jobs)
                
                # Intelligent delay between requests
                if i < len(list_urls):
                    await self.intelligent_delay()
                    
            except Exception as e:
                print(f"❌ Error processing {url}: {e}")
                continue
        
        print(f"\n🎉 Total jobs scraped: {len(all_jobs)}")
        return all_jobs
    
    async def scrape_individual_job_pages(self, job_urls: List[str], detail_extraction_prompt: str) -> List[Dict]:
        """Scrape individual job detail pages for more comprehensive data"""
        
        detailed_jobs = []
        
        for i, url in enumerate(job_urls, 1):
            print(f"\n🔍 Processing job detail {i}/{len(job_urls)}")
            
            try:
                jobs = await self.extract_jobs_with_ai(url, detail_extraction_prompt)
                if jobs:
                    detailed_jobs.extend(jobs)
                
                # Intelligent delay between requests
                if i < len(job_urls):
                    await self.intelligent_delay()
                    
            except Exception as e:
                print(f"❌ Error processing job detail {url}: {e}")
                continue
        
        return detailed_jobs
    
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

# UN Jobs Scraper Implementation
class UNJobsScraper(EnhancedJobScraper):
    """Specialized scraper for UN Jobs with Cloudflare bypass capabilities"""
    
    def __init__(self):
        config = EnhancedScraperConfig()
        super().__init__(config)
        
        self.base_url = "https://unjobs.org"
        self.mozambique_url = f"{self.base_url}/duty_stations/mozambique"
        
    def get_un_extraction_prompt(self) -> str:
        """Get the extraction prompt for UN Jobs"""
        return """
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
        
        Look for job listings, job cards, or job table rows. Extract ALL jobs found on the page.
        If you find pagination or "load more" functionality, note it but focus on current page jobs.
        
        Return the data as a JSON object with a "jobs" array containing all extracted job objects.
        """
    
    def get_un_detail_extraction_prompt(self) -> str:
        """Get the extraction prompt for individual UN job pages"""
        return """
        You are an expert job data extractor. Analyze this individual UN job posting page and extract comprehensive job details.
        
        Extract the following information:
        - job_title: Complete job title
        - company_name: Organization/UN agency name
        - location: Full location details
        - category: Job category/sector
        - publication_date: Posting date
        - expiring_date: Application deadline
        - job_description: Complete job description/summary
        - tasks_of_the_role: All responsibilities and duties (combine into single string with newlines)
        - requirements: All qualifications, experience requirements, etc. (combine into single string with newlines)
        - source_url: Current page URL
        
        Be thorough and extract all relevant details from this job posting page.
        Return as JSON with a "jobs" array containing one comprehensive job object.
        """
    
    async def discover_job_urls(self) -> List[str]:
        """Discover all job posting URLs from the UN Jobs Mozambique page"""
        
        discovery_prompt = """
        Analyze this UN Jobs page and extract ALL individual job posting URLs.
        Look for links to individual job postings, typically containing "/jobs/" or similar patterns.
        
        Return a JSON object with a "urls" array containing all job posting URLs found.
        Include full URLs (with domain) not relative paths.
        """
        
        # Configure browser settings for URL discovery
        browser_config = BrowserConfig(
            headless=True,
            user_agent=self.get_random_user_agent(),
            viewport_width=1920,
            viewport_height=1080,
            proxy_config=await self.get_proxy_config(),
            extra_args=[
                "--no-sandbox", 
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled"
            ]
        )
        
        llm_config = LLMConfig(
            provider="google_genai/gemini-1.5-flash",
            api_token=self.config.gemini_api_key
        )
        
        extraction_strategy = LLMExtractionStrategy(
            llm_config=llm_config,
            instruction=discovery_prompt,
            schema={
                "type": "object",
                "properties": {
                    "urls": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                }
            },
            verbose=True
        )
        
        # Configure crawler run settings
        crawler_run_config = CrawlerRunConfig(
            extraction_strategy=extraction_strategy,
            wait_until="networkidle",
            delay_before_return_html=5.0,
            verbose=True
        )
        
        async with AsyncWebCrawler(config=browser_config) as crawler:
            try:
                result = await crawler.arun(
                    url=self.mozambique_url,
                    config=crawler_run_config
                )
                
                if result.success and result.extracted_content:
                    try:
                        data = json.loads(result.extracted_content)
                        urls = data.get('urls', [])
                        print(f"🔍 Discovered {len(urls)} job URLs")
                        return urls
                    except json.JSONDecodeError:
                        print("❌ Failed to parse URL discovery results")
                        return []
                else:
                    print(f"❌ URL discovery failed: {result.error_message if result else 'Unknown error'}")
                    return []
                    
            except Exception as e:
                print(f"❌ Error discovering URLs: {e}")
                return []
    
    async def scrape_un_jobs(self) -> List[Dict]:
        """Main method to scrape UN Jobs for Mozambique"""
        
        print("🌍 Starting Enhanced UN Jobs Scraper for Mozambique")
        print("=" * 60)
        
        # Step 1: Try to extract jobs directly from the main page
        print("\n📋 Step 1: Extracting jobs from main listings page...")
        main_page_jobs = await self.extract_jobs_with_ai(
            self.mozambique_url, 
            self.get_un_extraction_prompt()
        )
        
        # Step 2: Discover individual job URLs for detailed extraction
        print("\n🔍 Step 2: Discovering individual job URLs...")
        job_urls = await self.discover_job_urls()
        
        # Step 3: Extract detailed information from individual job pages
        detailed_jobs = []
        if job_urls:
            print(f"\n📄 Step 3: Extracting detailed job information from {len(job_urls)} pages...")
            # Limit to first 20 jobs for testing
            limited_urls = job_urls[:20]
            detailed_jobs = await self.scrape_individual_job_pages(
                limited_urls,
                self.get_un_detail_extraction_prompt()
            )
        
        # Combine and deduplicate results
        all_jobs = main_page_jobs + detailed_jobs
        
        # Save results
        if all_jobs:
            filename = f"un_jobs_enhanced_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            unique_jobs = self.save_results(all_jobs, filename)
            return unique_jobs
        else:
            print("❌ No jobs were successfully extracted")
            return []

# Main execution function
async def main():
    """Main function to run the enhanced UN Jobs scraper"""
    
    print("🚀 Enhanced Job Scraper with Crawl4AI & Proxy Support")
    print("=" * 60)
    
    # Initialize and run the UN Jobs scraper
    scraper = UNJobsScraper()
    jobs = await scraper.scrape_un_jobs()
    
    if jobs:
        print(f"\n✅ Successfully scraped {len(jobs)} UN jobs for Mozambique!")
        print("\nSample job titles:")
        for i, job in enumerate(jobs[:5], 1):
            title = job.get('job_title', 'N/A')[:50]
            company = job.get('company_name', 'N/A')[:30]
            print(f"  {i}. {title}... @ {company}")
    else:
        print("\n❌ No jobs were successfully scraped.")
    
    return jobs

if __name__ == "__main__":
    asyncio.run(main()) 