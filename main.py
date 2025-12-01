import logging
from pathlib import Path
import time
import random
import pandas as pd
import numpy as np

from playwright.sync_api import sync_playwright, Browser, BrowserContext


# create CSV if it does not exist
CSV_FILE = "amazon_products.csv"
CSV_COLUMNS = [
    "Image", "Title", "Avg Review", "Review Count", "Has Prime",
    "Price", "Delivery", "Availability", "Specifications", "URL"
]
if not Path(CSV_FILE).exists():
    df = pd.DataFrame(columns=[CSV_COLUMNS])
    df.to_csv(CSV_FILE, index=False)


class AmazonScraper:
    """Scrape Amazon without proxy."""

    def __init__(self):
        self.playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    def get_secure_wait_time(self, min_seconds=1, max_seconds=3):
        import secrets
        wait_time = secrets.SystemRandom().uniform(min_seconds, max_seconds)
        print(f"wait: {wait_time} seconds")
        return wait_time

    def open_browser(self, headless: bool = False):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=headless,
            channel="chrome"
        )
        self.context = self.browser.new_context()
        return True

    def accept_cookies(self, page):
        buttons = [
            "#sp-cc-accept",                # main Amazon NL accept button
            "text=Accepteer",
            "text=Alle cookies accepteren",
            "text=Akkoord",
            "text=Accepteren",
            "text=Accept",
            "text=Accept all",
            "text=Agree",
            "button:has-text('Accepteer')",
            "button:has-text('Accept')",
        ]

        for b in buttons:
            try:
                loc = page.locator(b)
                if loc.is_visible():
                    loc.click()
                    logging.info(f"Cookie accepted with: {b}")
                    break
            except:
                pass

    def search_keyword(self, page, keyword: str):
        box = page.locator("#twotabsearchtextbox")
        box.wait_for(state="visible", timeout=10000)
        box.click()
        box.fill(keyword)
        page.keyboard.press("Enter")

    def human_scroll(self, page, steps=3):
        """Do small random scrolls to simulate user activity."""
        for _ in range(steps):
            # small vertical scroll
            distance = random.randint(200, 700)
            page.evaluate(f"window.scrollBy(0, {distance})")
            time.sleep(self.get_secure_wait_time(1, 2.5))
        # small back up sometimes
        if random.random() < 0.3:
            page.evaluate("window.scrollBy(0, -200)")
            time.sleep(self.get_secure_wait_time(1.3, 1.8))

    def get_all_product_links(self, page, limit: int | None = None, max_scrolls: int = 10):
        """
        Scroll a bit and collect unique product links with /dp/.
        If limit provided, return up to limit links.
        """
        seen = set()
        prev_count = 0

        for scroll_round in range(max_scrolls):
            # collect current links
            links = page.locator("a.a-link-normal[href*='/dp/']")
            count = links.count()
            for i in range(count):
                href = links.nth(i).get_attribute("href")
                if not href:
                    continue
                if "/dp/" in href:
                    # normalize to absolute url
                    if href.startswith("http"):
                        full = href
                    else:
                        full = "https://www.amazon.nl" + href.split("?")[0]
                    seen.add(full)

            # if we've reached limit, break
            if limit and len(seen) >= limit:
                break

            # stop if no new links after a round
            if len(seen) == prev_count:
                break
            prev_count = len(seen)

            # scroll a bit to load more items and act human
            self.human_scroll(page, steps=random.randint(2, 7))
            time.sleep(self.get_secure_wait_time(1.7, 2.8))

        urls = list(seen)
        if limit:
            return urls[:limit]
        return urls
    
    def go_to_next_search_page(self, page):
        """
        Try to click the pagination 'next' button. Return True if navigated.
        """
        try:
            # common next button
            next_btn = page.locator("a.s-pagination-next, a.s-pagination-item.s-pagination-next")
            if next_btn.count() and next_btn.first.is_visible():
                next_btn.first.click()
                logging.info("Clicked pagination next button")
                return True

            # fallback: find link with text 'Volgende' or 'Next'
            fallback = page.locator("a:has-text('Volgende'), a:has-text('Next')")
            if fallback.count() and fallback.first.is_visible():
                fallback.first.click()
                logging.info("Clicked fallback next link")
                return True

        except Exception as e:
            logging.debug(f"Next page click failed: {e}")
        return False
    
    def open_product_page(self, page, url: str):
        page.goto(url)
        # act like a user: small scroll and wait
        self.human_scroll(page, steps=random.randint(2, 4))
        time.sleep(self.get_secure_wait_time(2, 5))

    def scrape_product_data(self, page, url):
        """Scrape the product data and append to CSV."""
        self.human_scroll(page, steps=random.randint(2, 4))
        time.sleep(self.get_secure_wait_time(2.5, 5.5))

        data = {col: np.nan for col in CSV_COLUMNS}
        data["URL"] = url

        fields = {
            "Image": [
                '//*[@id="landingImage"]'
            ],
            "Title": [
                '//*[@id="productTitle"]'
            ],
            "Avg Review": [
                '//*[@id="acrPopover"]/span/a/span'
            ],
            "Review Count": [
                '//*[@id="acrCustomerReviewText"]'
            ],
            
            # Multi fallbacks
            "Has Prime": [
                '//*[@id="abb-message"]'
            ],
            "Price": [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]',
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]'
            ],

            "Delivery": [
                '//*[@id="mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE"]/span'
            ],
            "Availability": [
                '//*[@id="availability"]'
            ],
            "Specifications": [
                '//*[@id="productDetails_feature_div"]'
            ],
        }

        for key, xpaths in fields.items():
            try:
                # Normalize to list
                xpaths = xpaths if isinstance(xpaths, list) else [xpaths]

                val = np.nan
                for xp in xpaths:
                    elem = page.locator(f'xpath={xp}')
                    if elem.count() > 0:
                        # Price: must start with currency
                        if key == "Price":
                            text = elem.first.inner_text().strip()
                            if not text.startswith("€"):
                                continue  # skip "-22%" or other junk
                        if key == "Image":
                            val = elem.first.get_attribute("src")
                        else:
                            val = elem.first.inner_text().strip()
                        break  # stop at first match

                data[key] = val
            except:
                data[key] = np.nan

        # append row to CSV
        df_row = pd.DataFrame([data])
        df_row.to_csv(CSV_FILE, mode='a', header=False, index=False)
        logging.info(f"Product data saved: {data.get('Title', 'Unknown')}")

    def close_browser(self):
        if self.context:
            logging.info("Closing browser context")
            self.context.close()

        if self.browser:
            logging.info("Closing browser")
            self.browser.close()

        if self.playwright:
            self.playwright.stop()

        logging.info("Browser closed successfully")


if __name__ == "__main__":
    scraper = AmazonScraper()

    try:
        if scraper.open_browser(headless=False):
            page = scraper.context.new_page()

            # Visit URL
            logging.info("Navigated to Amazon")
            page.goto("https://www.amazon.nl/gp/bestsellers/?ref_=nav_cs_bestsellers")
            time.sleep(scraper.get_secure_wait_time(2, 5))

            # Accept Coockies
            logging.info("Accept Cookies")
            scraper.accept_cookies(page)
            time.sleep(scraper.get_secure_wait_time(2, 5))

            # Click on Searchbar and Search keywords 
            keywords = ['cup', 'skincare', 'charger']
            for kw in keywords:
                logging.info(f"Searched keyword: {kw}")
                scraper.search_keyword(page, kw)
                time.sleep(scraper.get_secure_wait_time(3, 7))

                # traverse pages and collect links up to max_pages
                max_pages = 5
                collected = set()
                for pnum in range(max_pages):
                    logging.info(f"Collecting links on page {pnum+1}")
                    found = scraper.get_all_product_links(page, limit=None, max_scrolls=6)
                    for u in found:
                        collected.add(u)

                    # try to go to next page
                    moved = scraper.go_to_next_search_page(page)
                    if not moved:
                        logging.info("No next page found, stopping pagination")
                        break
                    # wait after navigation
                    time.sleep(scraper.get_secure_wait_time(3, 7))

                product_urls = list(collected)
                logging.info(f"Found {len(product_urls)} unique product urls across pages")

                # visit each product page
                for url in product_urls:
                    logging.info(f"Visiting: {url}")
                    scraper.open_product_page(page, url)
                    time.sleep(scraper.get_secure_wait_time(2, 5))

                    logging.info("Scrape data")
                    scraper.scrape_product_data(page, url)
                    time.sleep(scraper.get_secure_wait_time(2, 5))

                # clear for next keyword
                product_urls = []
                collected.clear()


            logging.info("==================== SCRAPER COMPLETED! ====================")
            time.sleep(scraper.get_secure_wait_time(3, 7))

    except Exception as e:
        logging.error(f"Error during scraping: {e}")

    finally:
        scraper.close_browser()
