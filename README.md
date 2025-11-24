# Airbnb Host & Listing Scraper (Human Imitation)

A robust web scraper designed to extract Airbnb host profiles, listings, reviews, and high-resolution photo galleries. This tool uses **human-imitation techniques** (mouse movements, stealth browsing) to reduce bot detection and handle dynamic content.

## ⚡ Features
- **Stealth Browsing:** Playwright + stealth plugins to mimic natural user behavior  
- **Deep Scraping:** Host bio, reviews, guidebooks, and travel history  
- **Listing Hydration:** Amenities, pricing, descriptions, and metadata  
- **Photo Gallery Scraper:** Smart scrolling to collect 100+ listing photos  
- **Local Database:** All data stored in a SQLite database  



## 🛠️ Installation

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd airbnbscrapper
````

### 2. Create a virtual environment

It is recommended to run the project in an isolated environment.

#### Windows

```bash
python -m venv venvo
.\venvo\Scripts\activate
```

#### macOS / Linux

```bash
python3 -m venv venvo
source venvo/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

If this is your first time using Playwright, install the browser:

```bash
playwright install
```

---

## 🚀 How to Run

To scrape a specific host, run the `host_agent` module with the Airbnb profile URL:

```bash
python -m airbnb_host.host_agent https://www.airbnb.com/users/profile/[HOST_ID]
```

**Example:**

```bash
python -m airbnb_host.host_agent https://www.airbnb.com/users/profile/146394245xxxxxxxxx
```

The scraper will launch a browser, navigate the host profile and listings, and store all extracted data in the SQLite database.

---

## 📊 Exporting Data

You can export scraped data into CSV files using the export modules.

### Export Host Profile

```bash
python -m airbnb_host.export.HostExport
```

### Export Listings

```bash
python -m airbnb_host.export.ListingExport
```

### Export Guidebooks

```bash
python -m airbnb_host.export.GuidebookExport
```

### Export Travel History

```bash
python -m airbnb_host.export.TravelExport
```

### Export Host Reviews

```bash
python -m airbnb_host.export.HostReviewExport
```

### Export Listing Reviews

```bash
python -m airbnb_host.export.ListingReviewExport
```

