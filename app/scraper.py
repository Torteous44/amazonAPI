import aiohttp
import asyncio
import random
from lxml import html
from typing import Dict, Any, List, Optional
from datetime import datetime
import time
from fake_useragent import UserAgent
from aiohttp_socks import ProxyConnector
from app.utils import get_amazon_url
import requests
from aiohttp import ClientTimeout
from itertools import cycle

class TooManyRequestsException(Exception):
    pass

class ProxyRotator:
    def __init__(self, proxies: List[str]):
        self.proxies = proxies
        self.current_index = 0
        self.last_rotation = time.time()
        self.errors = {}

    def get_proxy(self) -> str:
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        return proxy

    def mark_error(self, proxy: str):
        self.errors[proxy] = self.errors.get(proxy, 0) + 1
        if self.errors[proxy] >= 3:  # Remove proxy after 3 failures
            if proxy in self.proxies:
                self.proxies.remove(proxy)
                del self.errors[proxy]

class SessionManager:
    def __init__(self):
        self.ua = UserAgent()
        self.sessions: Dict[str, aiohttp.ClientSession] = {}
        self.last_request_time: Dict[str, float] = {}
        self.min_delay = 2.0
        self.max_delay = 5.0

    async def get_session(self, proxy: str) -> aiohttp.ClientSession:
        if proxy not in self.sessions:
            connector = ProxyConnector.from_url(proxy)
            self.sessions[proxy] = aiohttp.ClientSession(connector=connector)
        return self.sessions[proxy]

    async def close_all(self):
        for session in self.sessions.values():
            await session.close()
        self.sessions.clear()

    def get_delay(self) -> float:
        # Sophisticated delay pattern using normal distribution
        base_delay = random.normalvariate((self.min_delay + self.max_delay) / 2, 1)
        return max(self.min_delay, min(base_delay, self.max_delay))

async def scrape_product_data(asin: str, proxy_rotator: Optional[ProxyRotator] = None) -> Dict[Any, Any]:
    """Scrape product details from Amazon using the ASIN."""
    url = get_amazon_url(asin)
    print(f"Constructed URL: {url}")

    session_manager = SessionManager()
    max_retries = 3

    try:
        for attempt in range(max_retries):
            try:
                # Get proxy and create session properly
                proxy = proxy_rotator.get_proxy() if proxy_rotator else None
                async with (await session_manager.get_session(proxy) if proxy 
                          else aiohttp.ClientSession()) as session:
                    
                    # Enhanced headers for desktop version
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-User': '?1',
                        'Cache-Control': 'max-age=0',
                    }

                    # Dynamic cookies
                    cookies = {
                        'session-id': f'{random.randint(100000000, 999999999)}',
                        'session-id-time': str(int(datetime.now().timestamp())),
                        'i18n-prefs': 'USD',
                        'sp-cdn': f'L5Z9:{random.randint(100000, 999999)}',
                    }

                    # Sophisticated delay pattern
                    await asyncio.sleep(session_manager.get_delay())

                    async with session.get(url, headers=headers, cookies=cookies, timeout=30) as response:
                        content = await response.text()


                        
                        if 'Robot Check' in content:
                            print("CAPTCHA detected!")
                            continue

                        if response.status == 429:
                            if proxy_rotator:
                                proxy_rotator.mark_error(proxy)
                            wait_time = (2 ** attempt + random.uniform(1, 3)) * 5
                            await asyncio.sleep(wait_time)
                            continue

                        if any(phrase in content for phrase in ['Robot Check', 'CAPTCHA', 'Enter the characters you see below']):
                            if proxy_rotator:
                                proxy_rotator.mark_error(proxy)
                            if attempt == max_retries - 1:
                                raise TooManyRequestsException("CAPTCHA detected")
                            continue

                        tree = html.fromstring(content)
                        data = {}

                        # Multiple price selectors with more variations
                        price = None
                        price_selectors = [
                            '//span[@class="a-offscreen"]/text()',
                            '//span[@id="priceblock_ourprice"]/text()',
                            '//span[@id="priceblock_dealprice"]/text()',
                            '//span[contains(@class, "a-price-whole")]/text()',
                            '//span[@class="a-price"]//span[@class="a-offscreen"]/text()',
                            '//span[@id="price_inside_buybox"]/text()',
                            '//span[@class="a-price"]//text()',
                            '//span[contains(@class, "price")]/text()'
                        ]
                        
                        for selector in price_selectors:
                            price_elem = tree.xpath(selector)
                            if price_elem and price_elem[0].strip():
                                price = price_elem[0].strip()
                                break
                        
                        data['price'] = price if price else 'Not Available'

                        # Multiple rating selectors
                        rating = None
                        rating_selectors = [
                            '//span[@data-hook="rating-out-of-text"]/text()',
                            '//span[@class="a-icon-alt"]/text()',
                            '//div[@id="averageCustomerReviews"]//span[@class="a-icon-alt"]/text()',
                            '//span[contains(@class, "stars-")]/@title'
                        ]
                        
                        for selector in rating_selectors:
                            rating_elem = tree.xpath(selector)
                            if rating_elem and rating_elem[0].strip():
                                rating = rating_elem[0].split()[0]
                                break

                        data['average_rating'] = rating if rating else 'Not Available'



                        total_reviews = None
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
                                    total_reviews = count_text
                                    break

                        data['total_reviews'] = total_reviews if total_reviews else 'Not Available'

                        # Multiple feature bullet selectors
                        bullets = None
                        bullet_selectors = [
                            '//div[@id="feature-bullets"]//li/span[@class="a-list-item"]/text()',
                            '//div[@id="feature-bullets"]//ul/li/span/text()',
                            '//div[contains(@class, "feature-bullets")]//span[@class="a-list-item"]/text()',
                            '//ul[contains(@class, "a-unordered-list")]//span[@class="a-list-item"]/text()'
                        ]
                        
                        for selector in bullet_selectors:
                            bullets = tree.xpath(selector)
                            if bullets:
                                break
                        
                        data['feature_bullets'] = [bullet.strip() for bullet in bullets if bullet.strip()] if bullets else 'Not Available'

                        reviews = []
                        review_items = tree.xpath('//div[contains(@id, "customer_review-")]')
                        for item in review_items:
                            review_data = {}
                            title = item.xpath('.//span[@data-hook="review-title"]//text()')
                            rating = item.xpath('.//i[contains(@data-hook, "review-star-rating")]//text()')
                            body = item.xpath('.//span[@data-hook="review-body"]//text()')
                            date = item.xpath('.//span[@data-hook="review-date"]//text()')
                            
                            review_data['title'] = ' '.join(title).strip() if title else 'Not Available'
                            review_data['rating'] = rating[0].strip() if rating else 'Not Available'
                            review_data['body'] = ' '.join(body).strip() if body else 'Not Available'
                            review_data['date'] = date[0].strip() if date else 'Not Available'
                            reviews.append(review_data)
                        
                        data['reviews'] = reviews if reviews else 'Not Available'

                        customers_say = tree.xpath('//div[@id="product-summary"]//p/text()')
                        data['customers_say'] = customers_say[0].strip() if customers_say else 'Not Available'

                        sentiments = tree.xpath('//a[contains(@class, "_Y3Itc_aspect-link_TtdmS")]/@aria-label')
                        data['customer_sentiments'] = [s.strip() for s in sentiments] if sentiments else 'Not Available'

                        return data

            except TooManyRequestsException as e:
                print(f"Failed after {max_retries} attempts due to CAPTCHA")
                return {"error": str(e), "asin": asin, "timestamp": datetime.now().isoformat()}
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"Failed after {max_retries} attempts")
                    return {"error": str(e), "asin": asin, "timestamp": datetime.now().isoformat()}
                await asyncio.sleep(2 ** attempt)

        return {"error": "Max retries exceeded", "asin": asin, "timestamp": datetime.now().isoformat()}

    except Exception as e:
        return {"error": str(e), "asin": asin, "timestamp": datetime.now().isoformat()}
    
    finally:
        await session_manager.close_all()

def get_free_proxies() -> List[str]:
    """Fetch free proxies from multiple sources."""
    proxies = []
    
    # ProxyScrape API
    try:
        response = requests.get('https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all')
        proxies.extend([f"http://{proxy}" for proxy in response.text.split()])
    except Exception as e:
        print(f"Error fetching from ProxyScrape: {e}")

    # Additional free proxy sources
    sources = [
        'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
        'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt',
        'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt',
    ]
    
    for source in sources:
        try:
            response = requests.get(source)
            if response.status_code == 200:
                proxies.extend([f"http://{proxy.strip()}" for proxy in response.text.splitlines() if proxy.strip()])
        except Exception as e:
            print(f"Error fetching from {source}: {e}")

    return list(set(proxies))  # Remove duplicates

# Initialize proxy rotator with free proxies
proxy_rotator = ProxyRotator(get_free_proxies())

class AsyncProductScraper:
    def __init__(self, max_concurrent: int = 10, timeout: int = 30):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.timeout = ClientTimeout(total=timeout)
        self.session_manager = SessionManager()
        
    async def scrape_products(self, asins: List[str]) -> List[Dict[Any, Any]]:
        """Scrape multiple products concurrently"""
        async with self.session_manager.get_session() as session:
            tasks = [
                self._scrape_with_semaphore(asin, session) 
                for asin in asins
            ]
            return await asyncio.gather(*tasks)
    
    async def _scrape_with_semaphore(self, asin: str, session) -> Dict[Any, Any]:
        """Scrape single product with semaphore control"""
        async with self.semaphore:
            return await scrape_product_data(asin, session=session)

# Example usage:
async def main():
    asins = ["B000example1", "B000example2", "B000example3"]
    scraper = AsyncProductScraper(max_concurrent=5)
    results = await scraper.scrape_products(asins)
    print(results)

if __name__ == "__main__":
    asyncio.run(main())
