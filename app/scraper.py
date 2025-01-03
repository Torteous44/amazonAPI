import aiohttp
import asyncio
from lxml import html
from typing import List, Dict, Any
from datetime import datetime
import random
import requests
from aiohttp import ClientTimeout, TCPConnector
from aiohttp_socks import ProxyConnector
from app.utils import get_amazon_url

class ProxyRotator:
    def __init__(self):
        self.proxies = self.get_free_proxies()
        self.current = 0
        
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
                    proxies.extend([f"http://{proxy.strip()}" for proxy in response.text.splitlines() if proxy.strip()])
            except:
                continue
                
        return list(set(proxies))  # Remove duplicates
        
    def get_proxy(self) -> str:
        if not self.proxies:
            self.proxies = self.get_free_proxies()
        proxy = self.proxies[self.current]
        self.current = (self.current + 1) % len(self.proxies)
        return proxy

class AsyncProductScraper:
    def __init__(self, max_concurrent: int = 20):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.timeout = ClientTimeout(total=30)
        self.proxy_rotator = ProxyRotator()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }

    async def scrape_products(self, asins: List[str]) -> List[Dict[Any, Any]]:
        connector = TCPConnector(limit=100, force_close=True)
        async with aiohttp.ClientSession(connector=connector, timeout=self.timeout, headers=self.headers) as session:
            tasks = [self._scrape_with_semaphore(asin, session) for asin in asins]
            return await asyncio.gather(*tasks, return_exceptions=True)

    async def _scrape_with_semaphore(self, asin: str, session) -> Dict[Any, Any]:
        async with self.semaphore:
            try:
                url = get_amazon_url(asin)
                async with session.get(url) as response:
                    if response.status != 200:
                        return {"error": f"Status {response.status}", "asin": asin}
                    
                    content = await response.text()
                    tree = html.fromstring(content)
                    
                    data = {"asin": asin}
                    data.update({
                        "price": await self._extract_price(tree),
                        "average_rating": await self._extract_rating(tree),
                        "total_reviews": await self._extract_total_reviews(tree),
                        "feature_bullets": await self._extract_bullets(tree),
                        "reviews": await self._extract_reviews(tree),
                        "customers_say": await self._extract_customers_say(tree),
                        "customer_sentiments": await self._extract_sentiments(tree)
                    })
                    return data
            except Exception as e:
                print(f"Error scraping {asin}: {str(e)}")
                return {"asin": asin, "error": str(e)}

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
