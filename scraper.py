import aiohttp
import asyncio
import json
import pandas as pd
from typing import List, Dict, Optional
from urllib.parse import urlparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AdTechScraper:
    def __init__(self):
        self.session = None
        self.results = {
            'ads_txt': [],
            'sellers_json': []
        }

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def fetch_file(self, domain: str, file_type: str) -> Optional[str]:
        """Fetch ads.txt or sellers.json file from a domain."""
        await self.init_session()
        
        # Normalize domain
        if not domain.startswith(('http://', 'https://')):
            domain = f'https://{domain}'
        
        # Remove trailing slash
        domain = domain.rstrip('/')
        
        urls = []
        if file_type == 'ads_txt':
            urls = [f'{domain}/ads.txt']
        elif file_type == 'sellers_json':
            urls = [
                f'{domain}/sellers.json',
                f'{domain}/.well-known/sellers.json'
            ]

        for url in urls:
            try:
                async with self.session.get(url, timeout=10) as response:
                    if response.status == 200:
                        return await response.text()
            except Exception as e:
                logger.warning(f"Error fetching {url}: {str(e)}")
                continue
        
        return None

    def parse_ads_txt(self, content: str, domain: str) -> List[Dict]:
        """Parse ads.txt content into structured data."""
        if not content:
            return []

        entries = []
        for line in content.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split(',')
            if len(parts) >= 3:
                entry = {
                    'domain': domain,
                    'ad_system_domain': parts[0].strip(),
                    'publisher_id': parts[1].strip(),
                    'account_type': parts[2].strip(),
                    'certification_authority_id': parts[3].strip() if len(parts) > 3 else None
                }
                entries.append(entry)

        return entries

    def parse_sellers_json(self, content: str, domain: str) -> List[Dict]:
        """Parse sellers.json content into structured data."""
        if not content:
            return []

        try:
            data = json.loads(content)
            entries = []
            
            if 'sellers' in data:
                for seller in data['sellers']:
                    entry = {
                        'domain': domain,
                        'seller_id': seller.get('seller_id'),
                        'name': seller.get('name'),
                        'domain': seller.get('domain'),
                        'seller_type': seller.get('seller_type'),
                        'is_confidential': seller.get('is_confidential'),
                        'is_passthrough': seller.get('is_passthrough')
                    }
                    entries.append(entry)
            
            return entries
        except json.JSONDecodeError:
            logger.error(f"Error parsing sellers.json for {domain}")
            return []

    async def process_domain(self, domain: str):
        """Process both ads.txt and sellers.json for a domain."""
        # Fetch and process ads.txt
        ads_txt_content = await self.fetch_file(domain, 'ads_txt')
        if ads_txt_content:
            self.results['ads_txt'].extend(self.parse_ads_txt(ads_txt_content, domain))

        # Fetch and process sellers.json
        sellers_json_content = await self.fetch_file(domain, 'sellers_json')
        if sellers_json_content:
            self.results['sellers_json'].extend(self.parse_sellers_json(sellers_json_content, domain))

    def save_results(self, output_dir: str = 'output'):
        """Save results to CSV files."""
        import os
        os.makedirs(output_dir, exist_ok=True)

        if self.results['ads_txt']:
            df_ads = pd.DataFrame(self.results['ads_txt'])
            df_ads.to_csv(f'{output_dir}/ads_txt_results.csv', index=False)

        if self.results['sellers_json']:
            df_sellers = pd.DataFrame(self.results['sellers_json'])
            df_sellers.to_csv(f'{output_dir}/sellers_json_results.csv', index=False)

async def main():
    # Example usage
    scraper = AdTechScraper()
    domains = [
        'example.com',
        'publisher1.com',
        'publisher2.com'
    ]

    try:
        for domain in domains:
            await scraper.process_domain(domain)
    finally:
        await scraper.close_session()

    scraper.save_results()

if __name__ == "__main__":
    asyncio.run(main()) 