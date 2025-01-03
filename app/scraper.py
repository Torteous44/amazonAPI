import aiohttp
import asyncio
from lxml import html
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from collections import deque
import cachetools
from aiohttp import ClientTimeout, TCPConnector
import requests

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProxyRotator:
    def __init__(self):
        self.proxies = deque(self.get_free_proxies())
        self.failed_proxies = set()
        self.min_proxies = 10
        logger.info(f"Initialized ProxyRotator with {len(self.proxies)} proxies")
    
    def get_free_proxies(self) -> List[str]:
        proxies = []
        sources = [
            'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
            'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt',
            'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt'
        ]
        
        for source in sources:
            try:
                response = requests.get(source)
                if response.status_code == 200:
                    new_proxies = [f"http://{proxy.strip()}" for proxy in response.text.splitlines() if proxy.strip()]
                    proxies.extend(new_proxies)
                    logger.info(f"Fetched {len(new_proxies)} proxies from {source}")
            except Exception as e:
                logger.error(f"Error fetching proxies from {source}: {e}")
                
        return list(set(proxies))
    
    def get_proxy(self) -> Optional[str]:
        if len(self.proxies) < self.min_proxies:
            logger.info("Refreshing proxy list...")
            self.proxies.extend(self.get_free_proxies())
        
        while self.proxies:
            proxy = self.proxies[0]
            self.proxies.rotate(-1)
            if proxy not in self.failed_proxies:
                return proxy
        return None
    
    def mark_failed(self, proxy: str):
        if proxy in self.proxies:
            self.proxies.remove(proxy)
            self.failed_proxies.add(proxy)
            logger.warning(f"Marked proxy as failed: {proxy}")

class AsyncProductScraper:
    def __init__(self, max_concurrent: int = 20, cache_ttl: int = 3600):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.timeout = ClientTimeout(total=30)
        self.proxy_rotator = ProxyRotator()
        self.session = None
        self.cache = cachetools.TTLCache(maxsize=1000, ttl=cache_ttl)
        logger.info(f"Initialized AsyncProductScraper with {max_concurrent} concurrent connections")

    async def __aenter__(self):
        """Context manager entry"""
        await self.get_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.session:
            await self.session.close()
            self.session = None

    async def get_session(self):
        if not self.session or self.session.closed:
            connector = TCPConnector(limit=100, force_close=True, enable_cleanup_closed=True)
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=self.timeout,
                headers=self.get_headers()
            )
            logger.info("Created new aiohttp session")
        return self.session

    def get_headers(self):
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }

    async def scrape_products(self, asins: List[str]) -> List[Dict[Any, Any]]:
        try:
            session = await self.get_session()
            tasks = [self._scrape_with_semaphore(asin, session) for asin in asins]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return [r for r in results if not isinstance(r, Exception)]
        finally:
            if self.session:
                await self.session.close()
                self.session = None

    async def _scrape_with_semaphore(self, asin: str, session) -> Dict[Any, Any]:
        async with self.semaphore:
            if asin in self.cache:
                logger.info(f"Cache hit for ASIN: {asin}")
                return self.cache[asin]

            for retry in range(3):
                try:
                    data = await self._scrape_single_product(asin, session)
                    if data and 'error' not in data:
                        self.cache[asin] = data
                        return data
                except Exception as e:
                    logger.warning(f"Attempt {retry + 1} failed for ASIN {asin}: {str(e)}")
                    if retry == 2:
                        return {"asin": asin, "error": str(e)}
                    await asyncio.sleep(1 * (retry + 1))
            
            return {"asin": asin, "error": "Max retries exceeded"}

    async def _scrape_single_product(self, asin: str, session) -> Dict[Any, Any]:
        url = f"https://www.amazon.com/dp/{asin}"
        async with session.get(url) as response:
            if response.status != 200:
                raise Exception(f"Status {response.status}")
            
            content = await response.text()
            tree = html.fromstring(content)
            
            # Parallel extraction with better error handling
            extraction_tasks = await asyncio.gather(
                self._extract_with_retry(self._extract_price, tree),
                self._extract_with_retry(self._extract_rating, tree),
                self._extract_with_retry(self._extract_total_reviews, tree),
                self._extract_with_retry(self._extract_bullets, tree),
                self._extract_with_retry(self._extract_reviews, tree),
                self._extract_with_retry(self._extract_customers_say, tree),
                self._extract_with_retry(self._extract_sentiments, tree),
                return_exceptions=True
            )
            
            return {
                "asin": asin,
                "price": extraction_tasks[0],
                "average_rating": extraction_tasks[1],
                "total_reviews": extraction_tasks[2],
                "feature_bullets": extraction_tasks[3],
                "reviews": extraction_tasks[4],
                "customers_say": extraction_tasks[5],
                "customer_sentiments": extraction_tasks[6]
            }

    async def _extract_with_retry(self, extract_func, tree, max_retries=2):
        """Wrapper for extraction functions with retry logic"""
        for attempt in range(max_retries):
            try:
                return await extract_func(tree)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Extraction failed: {str(e)}")
                    return "Not Available"
                await asyncio.sleep(0.1)

    async def _extract_price(self, tree) -> str:
        price_selectors = [
            '//span[@class="a-offscreen"]/text()',
            '//span[@id="priceblock_ourprice"]/text()',
            '//span[contains(@class, "a-price-whole")]/text()',
        ]
        for selector in price_selectors:
            price = tree.xpath(selector)
            if price and price[0].strip():
                return price[0].strip()
        return 'Not Available'

    async def _extract_rating(self, tree) -> str:
        rating_selectors = [
            '//span[@data-hook="rating-out-of-text"]/text()',
            '//span[@class="a-icon-alt"]/text()',
            '//div[@id="averageCustomerReviews"]//span[@class="a-icon-alt"]/text()',
        ]
        for selector in rating_selectors:
            rating = tree.xpath(selector)
            if rating and rating[0].strip():
                return rating[0].split()[0]
        return 'Not Available'

    async def _extract_reviews(self, tree) -> list:
        reviews = []
        review_elements = tree.xpath('//div[@data-hook="review"]')
        
        for review in review_elements[:5]:  # Limit to first 5 reviews
            title = review.xpath('.//a[@data-hook="review-title"]/span/text()')
            rating = review.xpath('.//i[@data-hook="review-star-rating"]/span/text()')
            body = review.xpath('.//span[@data-hook="review-body"]/span/text()')
            date = review.xpath('.//span[@data-hook="review-date"]/text()')
            
            if any([title, rating, body, date]):  # Only add if we found some data
                reviews.append({
                    'title': title[0].strip() if title else 'Not Available',
                    'rating': rating[0].strip() if rating else 'Not Available',
                    'body': ' '.join(body).strip() if body else 'Not Available',
                    'date': date[0].strip() if date else 'Not Available'
                })
        
        # Try alternative selectors if no reviews found
        if not reviews:
            review_items = tree.xpath('//div[contains(@id, "customer_review-")]')
            for item in review_items[:5]:
                title = item.xpath('.//span[@data-hook="review-title"]//text()')
                rating = item.xpath('.//i[contains(@data-hook, "review-star-rating")]//text()')
                body = item.xpath('.//span[@data-hook="review-body"]//text()')
                date = item.xpath('.//span[@data-hook="review-date"]//text()')
                
                reviews.append({
                    'title': ' '.join(title).strip() if title else 'Not Available',
                    'rating': rating[0].strip() if rating else 'Not Available',
                    'body': ' '.join(body).strip() if body else 'Not Available',
                    'date': date[0].strip() if date else 'Not Available'
                })
                
        return reviews if reviews else 'Not Available'

    async def _extract_bullets(self, tree) -> list:
        bullet_selectors = [
            '//div[@id="feature-bullets"]//li/span[@class="a-list-item"]/text()',
            '//div[@id="feature-bullets"]//ul/li/span/text()',
            '//div[contains(@class, "feature-bullets")]//span[@class="a-list-item"]/text()',
        ]
        for selector in bullet_selectors:
            bullets = tree.xpath(selector)
            if bullets:
                return [bullet.strip() for bullet in bullets if bullet.strip()]
        return 'Not Available'

    async def _extract_total_reviews(self, tree) -> str:
        review_selectors = [
            '//span[@id="acrCustomerReviewText"]/text()',
            '//div[@id="averageCustomerReviews"]//span[contains(@class, "a-size-base")]/text()',
            '//div[@data-hook="total-review-count"]//span/text()',
            '//span[contains(@class, "totalReviewCount")]/text()',
            '//a[@data-hook="see-all-reviews-link-foot"]/text()'
        ]
        
        for selector in review_selectors:
            review_count = tree.xpath(selector)
            if review_count:
                count_text = ''.join(c for c in review_count[0] if c.isdigit() or c == ',')
                if count_text:
                    return count_text
        return 'Not Available'

    async def _extract_customers_say(self, tree) -> str:
        selectors = [
            '//p[@class="a-spacing-small"]/span/text()',
            '//div[@id="product-summary"]//p/text()',
            '//p[contains(@class, "a-spacing-small")]/span/text()'
        ]
        
        for selector in selectors:
            customers_say = tree.xpath(selector)
            if customers_say and customers_say[0].strip():
                return customers_say[0].strip()
        return 'Not Available'

    async def _extract_sentiments(self, tree) -> List[str]:
        sentiments = tree.xpath('//a[contains(@class, "_Y3Itc_aspect-link_TtdmS")]/@aria-label')
        return [s.strip() for s in sentiments] if sentiments else 'Not Available'
