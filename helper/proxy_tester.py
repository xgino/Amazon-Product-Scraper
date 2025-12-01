import asyncio
import logging
from pathlib import Path
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm

logging.basicConfig(level=logging.INFO, format="%(message)s")

class ProxyTester:
    def __init__(self, proxy_file="socks5.txt", test_url="https://www.google.com", max_concurrent=10):
        self.proxy_file = Path(proxy_file)
        self.test_url = test_url
        self.max_concurrent = max_concurrent
        self.lock = asyncio.Lock()
        self.tested_count = 0
        self.total_count = 0
        self.working_count = 0
    
    def load_proxies(self):
        """Load proxies from file"""
        if not self.proxy_file.exists():
            return []
        return [line.strip() for line in self.proxy_file.read_text().splitlines() 
                if line.strip() and not line.startswith('#')]
    
    async def update_file_remove_proxy(self, proxy):
        """Remove bad proxy from file immediately"""
        async with self.lock:
            try:
                lines = self.proxy_file.read_text().splitlines()
                new_lines = [line for line in lines if line.strip() != proxy and not line.strip().startswith(proxy.split()[0])]
                self.proxy_file.write_text('\n'.join(new_lines) + '\n')
            except Exception as e:
                logging.error(f"Error removing proxy: {e}")
    
    async def update_file_mark_good(self, proxy):
        """Mark proxy as tested and working"""
        async with self.lock:
            try:
                lines = self.proxy_file.read_text().splitlines()
                # Remove old entry
                new_lines = [line for line in lines if not line.strip().startswith(proxy.split()[0])]
                # Add with checkmark
                new_lines.append(f"{proxy}")
                self.proxy_file.write_text('\n'.join(new_lines) + '\n')
            except Exception as e:
                logging.error(f"Error marking proxy: {e}")
    
    async def test_proxy(self, proxy, semaphore, pbar):
        """Test single proxy with concurrency limit"""
        async with semaphore:
            try:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(
                        headless=True,
                        proxy={"server": f"socks5://{proxy}"},
                        timeout=15000
                    )
                    page = await browser.new_page()
                    await page.goto(self.test_url, timeout=15000, wait_until="domcontentloaded")
                    await browser.close()
                    
                    self.working_count += 1
                    pbar.set_postfix({"✅ Working": self.working_count, "Status": "GOOD"})
                    await self.update_file_mark_good(proxy)
                    pbar.update(1)
                    return proxy
                    
            except Exception as e:
                pbar.set_postfix({"✅ Working": self.working_count, "Status": "BAD"})
                await self.update_file_remove_proxy(proxy)
                pbar.update(1)
                return None
    
    async def test_all_proxies(self):
        """Test all proxies with limited concurrency and progress bar"""
        proxies = self.load_proxies()
        
        if not proxies:
            logging.error("No proxies found in file!")
            return
        
        self.total_count = len(proxies)
        self.tested_count = 0
        self.working_count = 0
        
        print(f"🔍 Testing {len(proxies)} proxies (max {self.max_concurrent} concurrent)...\n")
        
        # Semaphore limits concurrent browser instances
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # Create progress bar
        with tqdm(total=len(proxies), desc="Testing proxies", unit="proxy", colour="green") as pbar:
            # Test all proxies with concurrency limit
            results = await asyncio.gather(*[self.test_proxy(p, semaphore, pbar) for p in proxies])
        
        # Count working proxies
        working = [p for p in results if p]
        
        print(f"\n📊 Final Results: {len(working)}/{len(proxies)} proxies working")
        print(f"✅ Check {self.proxy_file} for updated list")

if __name__ == "__main__":
    tester = ProxyTester(max_concurrent=10)
    asyncio.run(tester.test_all_proxies())