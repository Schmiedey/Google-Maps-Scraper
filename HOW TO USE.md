# Maps Lead Studio - Google Maps Scraper

A high-performance lead extraction engine built with Python, Playwright, and Streamlit. Effortlessly extract business data, contact emails, and social media presence directly from Google Maps.

## 🚀 Quick Start (Install & Run)

Follow these steps to get the environment ready and the app running in minutes.

# Create a virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On macOS/Linux
# .venv\Scripts\activate   # On Windows
```
.
### 2. Install Dependencies
```bash
# Upgrade pip and install requirements
pip install --upgrade pip
pip install -r requirements.txt

# Install Playwright browsers (Chromium)
python -m playwright install chromium
```

### 3. Launch the Application
```bash
# Start the Streamlit command center
streamlit run app.py
```
The app will open automatically at **http://localhost:8501**.

---

## 🛠 Features

- **🎯 Precision Search**: Search for any business type in any location globally.
- **📧 Email Enrichment**: Automatically scans business websites for validated contact emails.
- **📱 Social Presence**: Detects Facebook, Instagram, Twitter, and LinkedIn profiles.
- **📋 Smart Deduplication**: Built-in SQLite database prevents duplicate leads across runs.
- **🏷 Business Classification**: Automatically categorizes leads (Restaurant, Retail, Healthcare, etc.).
- **📊 Interactive Dashboard**: Monitor scraping progress, success rates, and live statistics.
- **💾 Export**: Clean CSV exports optimized for CRMs like Salesforce, HubSpot, or Pipedrive.

---

## 💻 CLI Usage
You can also run the scraper directly from the terminal without the UI:
```bash
python main.py -s "pizza in Chicago" -t 10 --output my_leads.csv
```

---

## 📦 Project Structure
- `app.py`: The Streamlit dashboard and user interface.
- `main.py`: Core scraping engine and data processing logic.
- `results/`: Default folder for saved CSVs and scraping reports.
- `requirements.txt`: Project dependencies.

---

## 📄 License
This project is for educational purposes. Please use responsibly and adhere to Google's terms of service.
