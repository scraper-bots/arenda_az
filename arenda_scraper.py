#!/usr/bin/env python3
"""
Arenda.az Real Estate Scraper
Usage: python arenda_scraper.py
"""

import asyncio
import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector
from bs4 import BeautifulSoup
import csv
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import re
import signal
from dataclasses import dataclass, asdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class Listing:
    """Data class for real estate listing"""
    listing_id: str = ""
    url: str = ""
    title: str = ""
    property_type: str = ""
    price: str = ""
    price_azn: str = ""
    location: str = ""
    address: str = ""
    rooms: str = ""
    area: str = ""
    floor: str = ""
    total_floors: str = ""
    description: str = ""
    features: str = ""
    agent_name: str = ""
    phone: str = ""
    date_posted: str = ""
    listing_code: str = ""
    view_count: str = ""
    has_document: str = ""
    is_credit_available: str = ""
    latitude: str = ""
    longitude: str = ""
    scraped_at: str = ""


class ScraperState:
    """Manages scraper state for crash recovery"""

    def __init__(self, state_file: str = "scraper_state.json"):
        self.state_file = Path(state_file)
        self.state = self._load_state()

    def _load_state(self) -> Dict:
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading state: {e}")
                return self._initial_state()
        return self._initial_state()

    def _initial_state(self) -> Dict:
        return {
            'last_page': 0,
            'processed_listings': [],
            'failed_listings': [],
            'total_scraped': 0,
            'last_update': None
        }

    def save(self):
        try:
            self.state['last_update'] = datetime.now().isoformat()
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving state: {e}")

    def add_processed(self, listing_id: str):
        if listing_id not in self.state['processed_listings']:
            self.state['processed_listings'].append(listing_id)
            self.state['total_scraped'] += 1
            self.save()

    def add_failed(self, listing_id: str, url: str):
        failed_entry = {'id': listing_id, 'url': url, 'time': datetime.now().isoformat()}
        self.state['failed_listings'].append(failed_entry)
        self.save()

    def is_processed(self, listing_id: str) -> bool:
        return listing_id in self.state['processed_listings']

    def set_last_page(self, page: int):
        self.state['last_page'] = page
        self.save()


class CSVWriter:
    """Thread-safe CSV writer"""

    def __init__(self, filename: str = "arenda_listings.csv"):
        self.filename = Path(filename)
        self.lock = asyncio.Lock()
        self._initialize_file()

    def _initialize_file(self):
        if not self.filename.exists():
            try:
                with open(self.filename, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=self._get_headers())
                    writer.writeheader()
            except Exception as e:
                logger.error(f"Error initializing CSV: {e}")

    def _get_headers(self) -> List[str]:
        return [field.name for field in Listing.__dataclass_fields__.values()]

    async def write_row(self, listing: Listing):
        async with self.lock:
            try:
                with open(self.filename, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(f, fieldnames=self._get_headers())
                    writer.writerow(asdict(listing))
            except Exception as e:
                logger.error(f"Error writing to CSV: {e}")
                raise


class ArendaScraper:
    """Main scraper class"""

    def __init__(self, base_url: str = "https://arenda.az", max_concurrent: int = 5):
        self.base_url = base_url
        self.max_concurrent = max_concurrent
        self.state = ScraperState()
        self.csv_writer = CSVWriter()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session: Optional[ClientSession] = None
        self.running = True
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    async def _create_session(self) -> ClientSession:
        timeout = ClientTimeout(total=30, connect=10)
        connector = TCPConnector(limit=100, limit_per_host=10, ttl_dns_cache=300)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'az,en-US;q=0.9,en;q=0.8',
        }
        return ClientSession(timeout=timeout, connector=connector, headers=headers)

    async def _fetch_with_retry(self, url: str, max_retries: int = 3) -> Optional[str]:
        for attempt in range(max_retries):
            try:
                async with self.semaphore:
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            # Try different encodings
                            try:
                                return await response.text(encoding='utf-8')
                            except:
                                try:
                                    return await response.text(encoding='windows-1254')
                                except:
                                    content = await response.read()
                                    return content.decode('utf-8', errors='ignore')
                        elif response.status == 404:
                            logger.warning(f"Page not found: {url}")
                            return None
                        else:
                            logger.warning(f"HTTP {response.status} for {url}")
            except asyncio.TimeoutError:
                logger.warning(f"Timeout for {url}, attempt {attempt + 1}/{max_retries}")
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}, attempt {attempt + 1}/{max_retries}")

            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
        return None

    def _extract_listing_links(self, html: str) -> List[tuple]:
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        for li in soup.find_all('li', class_='new_elan_box'):
            listing_id = li.get('id', '').replace('elan_', '')
            link = li.find('a', href=True)
            if link and listing_id:
                url = f"{self.base_url}{link['href']}" if link['href'].startswith('/') else link['href']
                listings.append((listing_id, url))
        return listings

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text).strip()

    def _extract_number(self, text: str) -> str:
        if not text:
            return ""
        match = re.search(r'[\d\s]+', text)
        return match.group().strip() if match else ""

    async def _parse_listing_detail(self, html: str, url: str) -> Optional[Listing]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            listing = Listing()
            listing.url = url
            listing.listing_id = url.split('-')[-1] if '-' in url else ""
            listing.scraped_at = datetime.now().isoformat()

            # Property type
            title_elem = soup.find('h2', class_='elan_in_title_link')
            if title_elem:
                listing.property_type = self._clean_text(title_elem.get_text())

            # Title
            full_title = soup.find('p', class_='elan_elan_nov')
            if full_title:
                listing.title = self._clean_text(full_title.get_text())

            # Price
            price_elem = soup.find('span', class_='elan_price_new')
            if price_elem:
                price_text = self._clean_text(price_elem.get_text())
                listing.price = price_text
                listing.price_azn = self._extract_number(price_text)

            # Location
            location_elem = soup.find('p', class_='elan_unvan')
            if location_elem:
                listing.location = self._clean_text(location_elem.get_text())

            address_elem = soup.find('span', class_='elan_unvan_txt')
            if address_elem:
                listing.address = self._clean_text(address_elem.get_text())

            # Properties
            property_list = soup.find('ul', class_='elan_property_list')
            if property_list:
                props = property_list.find_all('li')
                for prop in props:
                    text = self._clean_text(prop.get_text())
                    if 'otaq' in text:
                        listing.rooms = self._extract_number(text)
                    elif 'm2' in text or 'm²' in text:
                        listing.area = self._extract_number(text)
                    elif 'mərtəbə' in text:
                        floor_match = re.search(r'(\d+)\s*/\s*(\d+)', text)
                        if floor_match:
                            listing.floor = floor_match.group(1)
                            listing.total_floors = floor_match.group(2)

            # Description
            desc_elem = soup.find('div', class_='elan_info_txt')
            if desc_elem:
                desc_p = desc_elem.find('p')
                if desc_p:
                    listing.description = self._clean_text(desc_p.get_text())

            # Features
            features_list = []
            property_lists = soup.find('ul', class_='property_lists')
            if property_lists:
                features = property_lists.find_all('li')
                for feature in features:
                    feature_text = self._clean_text(feature.get_text())
                    if feature_text:
                        features_list.append(feature_text)
            listing.features = ', '.join(features_list)

            # Agent info
            agent_info = soup.find('div', class_='new_elan_user_info')
            if agent_info:
                agent_name = agent_info.find('p')
                if agent_name:
                    listing.agent_name = self._clean_text(agent_name.get_text())
                phone_link = agent_info.find('a', class_='elan_in_tel')
                if phone_link:
                    listing.phone = self._clean_text(phone_link.get_text())

            # Date, code, views
            date_box = soup.find('div', class_='elan_date_box')
            if date_box:
                paragraphs = date_box.find_all('p')
                for p in paragraphs:
                    text = self._clean_text(p.get_text())
                    if 'Elanın tarixi:' in text:
                        listing.date_posted = text.replace('Elanın tarixi:', '').strip()
                    elif 'Elanın kodu:' in text:
                        listing.listing_code = text.replace('Elanın kodu:', '').strip()
                    elif 'Baxış sayı:' in text:
                        listing.view_count = text.replace('Baxış sayı:', '').strip()

            # Document & Credit
            listing.has_document = 'Bəli' if soup.find('button', class_='kupca_ico') else 'Xeyr'
            listing.is_credit_available = 'Bəli' if soup.find('button', class_='kreditle_ico') else 'Xeyr'

            # Coordinates
            lat_input = soup.find('input', {'name': 'lat'})
            lon_input = soup.find('input', {'name': 'lon'})
            if lat_input and lat_input.get('value'):
                listing.latitude = lat_input['value']
            if lon_input and lon_input.get('value'):
                listing.longitude = lon_input['value']

            return listing
        except Exception as e:
            logger.error(f"Error parsing listing: {e}")
            return None

    async def _scrape_listing(self, listing_id: str, url: str) -> Optional[Listing]:
        if self.state.is_processed(listing_id):
            logger.info(f"Skipping already processed: {listing_id}")
            return None

        try:
            logger.info(f"Scraping {listing_id}: {url}")
            html = await self._fetch_with_retry(url)
            if not html:
                self.state.add_failed(listing_id, url)
                return None

            listing = await self._parse_listing_detail(html, url)
            if listing:
                await self.csv_writer.write_row(listing)
                self.state.add_processed(listing_id)
                logger.info(f"✓ Scraped {listing_id}")
                return listing
            else:
                self.state.add_failed(listing_id, url)
                return None
        except Exception as e:
            logger.error(f"Error scraping {listing_id}: {e}")
            self.state.add_failed(listing_id, url)
            return None

    async def _scrape_page(self, page: int, total_pages: int, start_time: float, page_times: list) -> List[Listing]:
        if not self.running:
            return []

        page_start = asyncio.get_event_loop().time()
        url = f"{self.base_url}/filtirli-axtaris/{page}/?home_search=1&lang=1&site=1&home_s=1"

        html = await self._fetch_with_retry(url)
        if not html:
            return []

        listing_links = self._extract_listing_links(html)

        if not listing_links:
            return []

        tasks = [self._scrape_listing(lid, lurl) for lid, lurl in listing_links]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        listings = [r for r in results if isinstance(r, Listing)]

        # Calculate timing
        page_time = asyncio.get_event_loop().time() - page_start
        page_times.append(page_time)

        # Calculate progress
        pages_done = page
        pages_left = total_pages - page
        total_scraped = self.state.state['total_scraped']

        # Calculate ETA
        avg_time = sum(page_times) / len(page_times)
        eta_seconds = avg_time * pages_left
        eta_minutes = eta_seconds / 60

        # Format ETA
        if eta_minutes < 60:
            eta_str = f"{eta_minutes:.1f}m"
        else:
            eta_hours = eta_minutes / 60
            eta_str = f"{eta_hours:.1f}h"

        logger.info(f"Page {page}/{total_pages} | Scraped: {len(listings)}/{len(listing_links)} | Total: {total_scraped} | Left: {pages_left} pages | ETA: {eta_str} | Avg: {avg_time:.1f}s/page")

        self.state.set_last_page(page)
        return listings

    def _get_max_page(self, html: str) -> int:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            pagination = soup.find('ul', class_='pagination')
            if pagination:
                page_links = pagination.find_all('a', class_='page-numbers')
                pages = [int(link.get_text().strip()) for link in page_links
                        if link.get_text().strip().isdigit()]
                return max(pages) if pages else 1
        except Exception as e:
            logger.error(f"Error extracting max page: {e}")
        return 1

    async def scrape(self, start_page: int = 1, end_page: Optional[int] = 554):
        """Main scraping method"""
        try:
            self.session = await self._create_session()
            start_time = asyncio.get_event_loop().time()
            page_times = []

            if self.state.state['last_page'] > 0:
                start_page = self.state.state['last_page'] + 1
                logger.info(f"🔄 Resuming from page {start_page}")

            logger.info(f"🚀 Starting scrape: Page {start_page} to {end_page} ({end_page - start_page + 1} pages)")

            for page in range(start_page, end_page + 1):
                if not self.running:
                    logger.info("⏸️  Paused by user")
                    break
                try:
                    await self._scrape_page(page, end_page, start_time, page_times)
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.error(f"❌ Error on page {page}: {e}")

            elapsed = asyncio.get_event_loop().time() - start_time
            logger.info(f"✅ Completed! Total: {self.state.state['total_scraped']} | Time: {elapsed/60:.1f}m")
        finally:
            if self.session:
                await self.session.close()


if __name__ == "__main__":
    logger.info("Starting scraper...")
    scraper = ArendaScraper(max_concurrent=5)
    asyncio.run(scraper.scrape())
