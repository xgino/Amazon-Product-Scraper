# 🛒 Amazon Product Scraper

A robust Python scraper designed to gather product data from Amazon while simulating human behavior to avoid detection. This tool searches for keywords, paginates through results, visits individual product pages, and processes the data into a clean CSV format.

![Amazon Scraper Demo](demo/Amazon%20Scraper%20Demo.gif)

Demo Gif

## 🚀 Features

* **Human-Like Interaction:** Simulates scrolling, random wait times, and natural mouse movements using `Playwright` to minimize bot detection.
* **Memory Efficient:** Clears processed URLs dynamically to optimize RAM usage during long scraping sessions.
* **Pagination:** Automatically navigates through search result pages to maximize data collection.
* **Data Cleaning:** Includes a Jupyter Notebook (`cleanup.ipynb`) to format raw data into a polished dataset.

## 📊 Data Collected

The scraper extracts the following fields:

| Core Data | Specifications | Meta Data |
| :--- | :--- | :--- |
| Title | Brand | ASIN |
| Price (€) | Model Number | Best Sellers Rank |
| Avg Review | Product Dimensions | Availability |
| Review Count | Item Weight | Delivery |
| Has Prime | Image URL | Product URL |

## 🛠️ Installation

### 1. Clone the repository
```bash
git clone [https://github.com/yourusername/amazon-scraper.git](https://github.com/yourusername/amazon-scraper.git)
cd amazon-scraper
```

### 2. Create a Virtual Environment
macOS / Linux:
```bash
python3 -m venv venv
source venv/bin/activate
```

Windows:
```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Install Requirements
```bash
pip install -r requirements.txt
playwright install
```

## ⚙️ Usage
### Step 1: Run the Scraper
Before running the script, open `main.py` to customize your search settings:

**Set Search Keywords** Update the `keywords` list to define what the bot will type into the Amazon search bar.
```bash
# Click on Searchbar and Search keywords 
keywords = ['cup', 'skincare', 'charger']
```

**Set Pagination Limit** Adjust `max_pages` to control how many result pages the bot will traverse (how many times it clicks "Next").
```bash
# traverse pages and collect links up to max_pages
max_pages = 5
```

**Run the Scraper** Once configured, execute the script:
```bash
python main.py
```

#### What happens?
1. Opens a browser (Chromium) and accepts cookies.
2. Searches for your keywords.
3. Scrolls and collects product links (handling pagination).
4. Visits each product page individually to scrape detailed data.
5. Saves raw data to `amazon_products.csv`.


### Step 2: Clean the Data
Once scraping is complete, run the cleanup notebook to format prices, parse specifications, and remove duplicates.

```bash
jupyter notebook cleanup.ipynb
```

Run all cells to generate the final `product.csv`.


## ⚠️ Disclaimer & Legal
Please Read Before Use: This tool is intended for educational purposes only.

- Respect Local Laws: Ensure you comply with the laws and regulations regarding web scraping in your jurisdiction (e.g., GDPR in Europe, CFAA in the US).
- Terms of Service: Scraping Amazon may violate their Terms of Service. Use this tool responsibly.
- Rate Limiting: The script includes delays to reduce server load, but aggressive scraping can lead to IP bans.

The author assumes no responsibility for any misuse of this software or any consequences resulting from its use.


## ☕ Support
If this project saved you time, feel free to buy me a coffee! 

[![Ko-fi](https://img.shields.io/badge/Ko--fi-FF5E5B?logo=ko-fi&logoColor=white)](https://ko-fi.com/xgino)

Every sip helps fuel a new line of code. Thank you for your support, and keep coding!



Made with ❤️ and Python.
