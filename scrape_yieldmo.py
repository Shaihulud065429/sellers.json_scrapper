import asyncio
from ssp_scraper import SSPScraper
import logging
import aiohttp
import brotli

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('yieldmo_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def main():
    # Initialize scraper
    scraper = SSPScraper()
    
    try:
        # Yieldmo specific data
        ssp_name = "Yieldmo"
        source_url = "https://yieldmo.com/sellers.json"
        
        # Custom headers to simulate un navigateur (sans Accept-Encoding)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': 'https://yieldmo.com/',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # Fetch and process sellers.json with custom headers
        await scraper.init_session()
        async with scraper.session.get(source_url, headers=headers) as response:
            if response.status == 200:
                content = await response.text()
                entries = scraper.parse_sellers_json(content, ssp_name, source_url)
                scraper.results['sellers'].extend(entries)
                
                # Save results
                scraper.save_results()
                logger.info(f"Successfully scraped {len(entries)} entries from Yieldmo")
            else:
                logger.error(f"Failed to fetch Yieldmo sellers.json. Status code: {response.status}")
            
    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}")
    finally:
        await scraper.close_session()

if __name__ == "__main__":
    asyncio.run(main()) 