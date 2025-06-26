import asyncio
import json
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LLMExtractionStrategy, BrowserConfig, LLMConfig
from decouple import config

async def debug_mmo_extraction():
    """Debug what content is being extracted from MMO"""
    
    print("🔍 Debug MMO Content Extraction")
    print("=" * 50)
    
    # Simple browser config
    browser_config = BrowserConfig(
        headless=True,
        viewport_width=1920,
        viewport_height=1080
    )
    
    # Simple prompt to see what we get
    debug_prompt = """
    Analyze this MMO Emprego page and tell me what you see.
    
    1. How many job listings are visible?
    2. What does a typical job listing look like?
    3. Extract just 2-3 sample job titles and companies if you can find them.
    
    Return your findings as a JSON object with:
    {
        "analysis": "description of what you see",
        "job_count": number,
        "sample_jobs": [
            {"title": "job title", "company": "company name"},
            ...
        ]
    }
    """
    
    # AI extraction strategy
    llm_config = LLMConfig(
        provider="gemini/gemini-1.5-flash",
        api_token=config('GEMINI_API_KEY')
    )
    
    extraction_strategy = LLMExtractionStrategy(
        llm_config=llm_config,
        instruction=debug_prompt,
        verbose=True
    )
    
    # Configure crawler run settings
    crawler_run_config = CrawlerRunConfig(
        extraction_strategy=extraction_strategy,
        wait_until="domcontentloaded",
        delay_before_return_html=2.0,
        verbose=True
    )
    
    async with AsyncWebCrawler(config=browser_config) as crawler:
        try:
            print("🕷️  Extracting from: https://emprego.mmo.co.mz/vagas-em-mocambique/")
            result = await crawler.arun(
                url="https://emprego.mmo.co.mz/vagas-em-mocambique/",
                config=crawler_run_config
            )
            
            if result.success:
                print(f"✅ Page loaded successfully")
                print(f"📄 HTML length: {len(result.html) if result.html else 0} chars")
                print(f"📝 Markdown length: {len(result.markdown) if result.markdown else 0} chars")
                
                if result.extracted_content:
                    print(f"🤖 AI Analysis:")
                    
                    # Handle different content formats
                    if isinstance(result.extracted_content, list):
                        content = result.extracted_content[0] if result.extracted_content else ""
                    else:
                        content = result.extracted_content
                    
                    try:
                        if isinstance(content, str):
                            analysis = json.loads(content)
                            print(json.dumps(analysis, indent=2, ensure_ascii=False))
                        else:
                            print(f"Content: {content}")
                    except json.JSONDecodeError:
                        print(f"Raw content: {content}")
                else:
                    print("❌ No extracted content from AI")
                    
                # Save raw markdown for inspection
                if result.markdown:
                    with open("debug_mmo_content.md", "w", encoding="utf-8") as f:
                        f.write(result.markdown)
                    print(f"💾 Saved raw markdown to debug_mmo_content.md")
                    
            else:
                print(f"❌ Failed to load page: {result.error_message}")
                
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(debug_mmo_extraction())
