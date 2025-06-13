import aiohttp
import asyncio
import json
import pandas as pd
from typing import List, Dict, Optional, Set
from urllib.parse import urlparse
import logging
from datetime import datetime
import re
from tqdm import tqdm
import time
import os
from aiohttp import TCPConnector
from google_sheets_uploader import GoogleSheetsUploader
from collections import Counter, defaultdict

# Google Sheets configuration
SPREADSHEET_ID = '16rptcM-d1tgxFid2NeS3BQjjOuxODNK7ZIng_DUDGag'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SSPScraper:
    def __init__(self):
        self.session = None
        self.results = {
            'sellers': [],
            'direct_media': [],
            'intermediaries': []
        }
        self.failed_requests = []
        self.new_domains_per_ssp = {}
        self.last_week_domains = self._load_last_week_domains()
        self.semaphore = asyncio.Semaphore(50)  # Limit concurrent connections

    def _load_last_week_domains(self) -> Dict[str, Set[str]]:
        """Load last week's domains for comparison."""
        try:
            if os.path.exists('last_week_domains.json'):
                with open('last_week_domains.json', 'r') as f:
                    return {k: set(v) for k, v in json.load(f).items()}
        except Exception as e:
            logger.error(f"Error loading last week's domains: {e}")
        return {}

    def _save_current_domains(self):
        """Save current domains for next week's comparison."""
        current_domains = {}
        for ssp, domains in self.new_domains_per_ssp.items():
            current_domains[ssp] = list(domains)
        
        with open('last_week_domains.json', 'w') as f:
            json.dump(current_domains, f)

    async def init_session(self):
        if not self.session:
            connector = TCPConnector(limit=50, force_close=True)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            self.session = aiohttp.ClientSession(connector=connector, headers=headers)

    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def fetch_file(self, url: str, timeout: int = 30) -> Optional[str]:
        """Fetch file content with retry mechanism."""
        await self.init_session()
        
        async with self.semaphore:  # Limit concurrent requests
            for attempt in range(3):
                try:
                    async with self.session.get(url, timeout=timeout) as response:
                        if response.status == 200:
                            return await response.text()
                        elif response.status == 404:
                            logger.info(f"File not found (404): {url}")
                            self.failed_requests.append(f"404: {url}")
                            return None
                        elif response.status == 429:  # Too Many Requests
                            wait_time = int(response.headers.get('Retry-After', 5))
                            logger.warning(f"Rate limited, waiting {wait_time} seconds")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.warning(f"HTTP {response.status} for {url}")
                            self.failed_requests.append(f"HTTP {response.status}: {url}")
                            return None
                except aiohttp.ClientConnectorError as e:
                    logger.warning(f"Connection error for {url}: {str(e)}")
                    self.failed_requests.append(f"Connection error: {url}")
                    return None
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                    if attempt < 2:
                        await asyncio.sleep(1)
                    continue
            
            self.failed_requests.append(f"Unreachable after retries: {url}")
            return None

    def parse_sellers_json(self, content: str, ssp_name: str, source_url: str) -> List[Dict]:
        """Parse sellers.json content into structured data."""
        if not content:
            return []

        try:
            data = json.loads(content)
            entries = []
            
            if 'sellers' in data:
                for seller in data['sellers']:
                    entry = {
                        'comment': '',
                        'domain': seller.get('domain', ''),
                        'is_confidential': seller.get('is_confidential', False),
                        'is_passthrough': seller.get('is_passthrough', False),
                        'name': seller.get('name', ''),
                        'seller_id': seller.get('seller_id', ''),
                        'seller_type': seller.get('seller_type', ''),
                        'website': seller.get('website', ''),
                        'Source URL': source_url,
                        'SSP name': ssp_name,
                        'Import_date': datetime.now().strftime('%Y-%m-%d'),
                        'Unique SSPs per Domain': 1  # Will be updated later
                    }
                    entries.append(entry)
                    
                    # Track new domains
                    if seller.get('domain'):
                        if ssp_name not in self.new_domains_per_ssp:
                            self.new_domains_per_ssp[ssp_name] = set()
                        self.new_domains_per_ssp[ssp_name].add(seller['domain'])
            
            return entries
        except json.JSONDecodeError:
            logger.error(f"Error parsing sellers.json for {ssp_name}")
            return []

    async def check_ads_txt(self, domain: str) -> Dict:
        """Vérifie ads.txt sur différentes variantes d'URL pour un domaine."""
        if not domain:  # Gère None ou domaine vide
            return {
                'domain': '',
                'ads_txt_exists': False,
                'unreachable': True,
                'has_smilewanted': False,
                'smilewanted_line': '',
                'owner_domain': '',
                'manager_domain': '',
                'contact': ''
            }

        # Normalisation du domaine
        domain = domain.strip()
        if domain.startswith('http://'):
            domain = domain[7:]
        elif domain.startswith('https://'):
            domain = domain[8:]
        domain = domain.rstrip('/')
        # Génère les variantes d'URL à tester
        base_domains = [domain]
        if not domain.startswith('www.'):
            base_domains.append('www.' + domain)
        else:
            base_domains.append(domain[4:])
        ads_txt_urls = [f'https://{d}/ads.txt' for d in base_domains]

        result = {
            'domain': domain,
            'ads_txt_exists': False,
            'unreachable': False,
            'has_smilewanted': False,
            'smilewanted_line': '',
            'owner_domain': '',
            'manager_domain': '',
            'contact': ''
        }

        found = False
        for url in ads_txt_urls:
            content = await self.fetch_file(url)
            if content:
                result['ads_txt_exists'] = True
                lines = content.split('\n')
                # Recherche Smilewanted
                smilewanted_patterns = ['smilewanted', 'smile wanted', 'SMILEWANTED']
                for line in lines:
                    if any(pattern in line.lower() for pattern in smilewanted_patterns):
                        result['has_smilewanted'] = True
                        result['smilewanted_line'] = line.strip()
                        break
                # Recherche OWNERDOMAIN, MANAGERDOMAIN, CONTACT
                for line in lines:
                    line = line.strip()
                    line_lower = line.lower()
                    owner_match = re.match(r".*ownerdomain=([^#\s]+)", line_lower)
                    manager_match = re.match(r".*managerdomain=([^#\s]+)", line_lower)
                    contact_match = re.match(r".*contact=([^#\s]+)", line_lower)
                    if owner_match:
                        result['owner_domain'] = owner_match.group(1).strip()
                    elif manager_match:
                        result['manager_domain'] = manager_match.group(1).strip()
                    elif contact_match:
                        result['contact'] = contact_match.group(1).strip()
                found = True
                logger.info(f"ads.txt trouvé pour {domain} à l'URL : {url}")
                break
        if not found:
            result['unreachable'] = True
        return result

    async def check_sellers_json(self, domain: str) -> Dict:
        """Vérifie la présence de sellers.json sur différentes variantes d'URL pour un domaine."""
        if not domain:  # Gère None ou domaine vide
            return {
                'domain': '',
                'sellers_json_url': '',
                'total_sellers': 0,
                'publisher_sellers': 0,
                'unreachable': True
            }

        # Normalisation du domaine
        domain = domain.strip()
        if domain.startswith('http://'):
            domain = domain[7:]
        elif domain.startswith('https://'):
            domain = domain[8:]
        domain = domain.rstrip('/')
        # Génère les variantes d'URL à tester
        base_domains = [domain]
        if not domain.startswith('www.'):
            base_domains.append('www.' + domain)
        else:
            base_domains.append(domain[4:])
        sellers_json_urls = []
        for d in base_domains:
            sellers_json_urls.append(f'https://{d}/sellers.json')
            sellers_json_urls.append(f'https://{d}/.well-known/sellers.json')

        result = {
            'domain': domain,
            'sellers_json_url': '',
            'total_sellers': 0,
            'publisher_sellers': 0,
            'unreachable': False
        }

        found = False
        for url in sellers_json_urls:
            content = await self.fetch_file(url)
            if content:
                try:
                    data = json.loads(content)
                    if 'sellers' in data:
                        result['sellers_json_url'] = url
                        result['total_sellers'] = len(data['sellers'])
                        result['publisher_sellers'] = sum(
                            1 for seller in data['sellers']
                            if seller.get('seller_type', '').upper() == 'PUBLISHER'
                        )
                        found = True
                        logger.info(f"sellers.json trouvé pour {domain} à l'URL : {url}")
                        break
                except json.JSONDecodeError:
                    continue
        if not found:
            result['unreachable'] = True

        return result

    def save_results(self):
        """Save all results to CSV files."""
        os.makedirs('output', exist_ok=True)
        
        # Save main sellers data
        if self.results['sellers']:
            df_sellers = pd.DataFrame(self.results['sellers'])
            # Update Unique SSPs per Domain
            domain_counts = df_sellers.groupby('domain')['SSP name'].nunique()
            df_sellers['Unique SSPs per Domain'] = df_sellers['domain'].map(domain_counts)
            df_sellers.to_csv('output/sellers_data.csv', index=False)
        
        # Save direct media data
        if self.results['direct_media']:
            df_direct = pd.DataFrame(self.results['direct_media'])
            df_direct.to_csv('output/direct_media.csv', index=False)
        
        # Save intermediaries data
        if self.results['intermediaries']:
            df_inter = pd.DataFrame(self.results['intermediaries'])
            df_inter.to_csv('output/intermediaries.csv', index=False)
        
        # Save new domains report
        from datetime import datetime
        week_str = datetime.now().strftime('%Y-%W')
        new_domains_report = []
        for ssp, domains in self.new_domains_per_ssp.items():
            last_week_domains = self.last_week_domains.get(ssp, set())
            new_domains = domains - last_week_domains
            new_domains_report.append({
                'Week': week_str,
                'SSP': ssp,
                'Total Domains': len(domains),
                'New Domains This Week': len(new_domains)
            })
        df_new_domains = pd.DataFrame(new_domains_report)
        df_new_domains.to_csv('output/new_domains_report.csv', index=False)
        
        # Save failed requests
        if self.failed_requests:
            with open('output/failed_requests.txt', 'w') as f:
                f.write('\n'.join(self.failed_requests))

async def process_domains_batch(scraper, domains, process_func, desc):
    """Process a batch of domains concurrently."""
    # Filter out None or empty domains
    valid_domains = [d for d in domains if d and isinstance(d, str) and d.strip()]
    
    if not valid_domains:
        return []
    
    tasks = []
    for domain in valid_domains:
        task = asyncio.create_task(process_func(domain))
        tasks.append(task)
    
    results = []
    for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=desc):
        try:
            result = await future
            results.append(result)
        except Exception as e:
            logger.error(f"Error processing domain: {str(e)}")
            continue
    
    return results

async def main():
    # Read SSP list
    ssp_df = pd.read_csv('List of SSP.csv', sep=';')
    scraper = SSPScraper()
    
    try:
        # Process sellers.json files
        ssp_tasks = []
        for _, row in ssp_df.iterrows():
            if pd.isna(row['Sellers.JSON']):
                continue
            
            task = asyncio.create_task(scraper.fetch_file(row['Sellers.JSON']))
            ssp_tasks.append((row['Name'], row['Sellers.JSON'], task))
        
        # Process all SSPs concurrently
        for ssp_name, source_url, task in tqdm(ssp_tasks, desc="Processing SSPs"):
            content = await task
            if content:
                entries = scraper.parse_sellers_json(content, ssp_name, source_url)
                scraper.results['sellers'].extend(entries)
        
        # --- Majority seller_type assignment per domain ---
        domain_type_counter = defaultdict(list)
        for entry in scraper.results['sellers']:
            if entry['domain']:
                domain_type_counter[entry['domain']].append(entry['seller_type'].upper())
        # Compute majority type per domain
        domain_majority_type = {}
        for domain, types in domain_type_counter.items():
            type_counts = Counter(types)
            majority_type = type_counts.most_common(1)[0][0]
            domain_majority_type[domain] = majority_type
        # Update all entries to use the majority type
        for entry in scraper.results['sellers']:
            if entry['domain'] in domain_majority_type:
                entry['seller_type'] = domain_majority_type[entry['domain']]
        # --- End majority seller_type assignment ---
        
        # Process direct media and intermediaries
        all_domains = set()
        for entry in scraper.results['sellers']:
            if entry['domain']:
                all_domains.add(entry['domain'])
        
        # Split domains into direct media and intermediaries
        direct_media = set()
        intermediaries = set()
        for entry in scraper.results['sellers']:
            if entry['seller_type'].upper() == 'PUBLISHER':
                direct_media.add(entry['domain'])
            else:
                intermediaries.add(entry['domain'])
        
        # Process direct media and intermediaries concurrently
        direct_media_results = await process_domains_batch(
            scraper, direct_media, scraper.check_ads_txt, "Processing Direct Media"
        )
        scraper.results['direct_media'].extend(direct_media_results)
        
        intermediary_results = await process_domains_batch(
            scraper, intermediaries, scraper.check_sellers_json, "Processing Intermediaries"
        )
        scraper.results['intermediaries'].extend(intermediary_results)
        
        # Save all results
        scraper.save_results()
        
        # Upload to Google Sheets
        try:
            uploader = GoogleSheetsUploader(SPREADSHEET_ID)
            uploader.upload_all_data()
            # Append this week's new domains report to the sheet
            if os.path.exists('output/new_domains_report.csv'):
                df_new = pd.read_csv('output/new_domains_report.csv', low_memory=False)
                uploader.append_dataframe(df_new, 'New Domains Report')
            logger.info("Successfully uploaded data to Google Sheets")
        except Exception as e:
            logger.error(f"Error uploading to Google Sheets: {str(e)}")
        
    finally:
        await scraper.close_session()

# Test asynchrone pour Mediavine
async def test_mediavine():
    scraper = SSPScraper()
    try:
        print("Test sellers.json pour https://www.mediavine.com ...")
        sellers_result = await scraper.check_sellers_json("www.mediavine.com")
        print("Résultat sellers.json :", sellers_result)
        print("Test ads.txt pour https://www.mediavine.com ...")
        ads_result = await scraper.check_ads_txt("www.mediavine.com")
        print("Résultat ads.txt :", ads_result)
    finally:
        await scraper.close_session()

if __name__ == "__main__":
    asyncio.run(test_mediavine()) 