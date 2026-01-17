from __future__ import annotations
import logging
from typing import List, Optional, Dict, Callable
from playwright.sync_api import sync_playwright, Page
from dataclasses import dataclass, asdict, fields
import argparse
import platform
import time
import os
import re
import csv
import sqlite3
import hashlib
import requests
from urllib.parse import urljoin, urlparse
from tqdm import tqdm
from functools import wraps

@dataclass
class Place:
    name: str = ""
    address: str = ""
    website: str = ""
    phone_number: str = ""
    email: str = ""  # New field for extracted email
    reviews_count: Optional[int] = None
    reviews_average: Optional[float] = None
    store_shopping: str = "No"
    in_store_pickup: str = "No"
    store_delivery: str = "No"
    place_type: str = ""
    opens_at: str = ""
    introduction: str = ""
    # New fields for social media and classification
    facebook: str = ""
    instagram: str = ""
    twitter: str = ""
    linkedin: str = ""
    business_category: str = ""

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )

def normalize_key(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip().lower()

def build_fingerprint(place: Place) -> str:
    key = "|".join([
        normalize_key(place.name),
        normalize_key(place.address),
        normalize_key(place.phone_number),
        normalize_key(place.website),
    ])
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def init_dedup_db(db_path: str) -> sqlite3.Connection:
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS leads (
            fingerprint TEXT PRIMARY KEY,
            name TEXT,
            address TEXT,
            website TEXT,
            phone_number TEXT,
            email TEXT,
            updated_at TEXT
        )
        """
    )
    conn.commit()
    return conn

def is_duplicate(conn: sqlite3.Connection, fingerprint: str, email: str) -> bool:
    row = conn.execute(
        "SELECT email FROM leads WHERE fingerprint = ?",
        (fingerprint,),
    ).fetchone()
    if not row:
        return False
    existing_email = (row[0] or "").strip()
    if existing_email:
        return True
    return not bool(email)

def upsert_lead(conn: sqlite3.Connection, fingerprint: str, place: Place) -> None:
    conn.execute(
        """
        INSERT INTO leads (fingerprint, name, address, website, phone_number, email, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fingerprint) DO UPDATE SET
            name = excluded.name,
            address = excluded.address,
            website = excluded.website,
            phone_number = excluded.phone_number,
            email = CASE
                WHEN leads.email IS NULL OR leads.email = '' THEN excluded.email
                ELSE leads.email
            END,
            updated_at = excluded.updated_at
        """,
        (
            fingerprint,
            place.name,
            place.address,
            place.website,
            place.phone_number,
            place.email,
            time.strftime('%Y-%m-%d %H:%M:%S'),
        ),
    )
    conn.commit()

@dataclass
class ScrapingStats:
    total_searched: int = 0
    successful_scrapes: int = 0
    failed_scrapes: int = 0
    duplicates_skipped: int = 0
    emails_found: int = 0
    websites_visited: int = 0
    social_media_found: int = 0
    start_time: str = ""
    end_time: str = ""
    average_time_per_business: float = 0.0
    target_leads: int = 0

def retry_on_failure(max_retries=3, delay=2):
    """Decorator to retry functions on failure with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logging.warning(
                        f"Attempt {attempt + 1} failed: {e}. Retrying in {current_delay}s..."
                    )
                    time.sleep(current_delay)
                    current_delay *= 2  # Exponential backoff
            return None
        return wrapper
    return decorator

def classify_business_type(name: str, introduction: str, place_type: str) -> str:
    """Classify business into categories based on name, description, and type."""
    text = f"{name} {introduction} {place_type}".lower()

    categories = {
        'restaurant': ['restaurant', 'cafe', 'diner', 'food', 'pizza', 'burger', 'bar', 'grill', 'kitchen', 'bistro'],
        'retail': ['store', 'shop', 'boutique', 'market', 'mall', 'retail', 'clothing', 'fashion'],
        'service': ['salon', 'spa', 'repair', 'cleaning', 'consulting', 'service', 'agency', 'studio'],
        'healthcare': ['hospital', 'clinic', 'dentist', 'pharmacy', 'medical', 'health', 'wellness', 'therapy'],
        'entertainment': ['theater', 'cinema', 'museum', 'park', 'gym', 'fitness', 'entertainment', 'venue'],
        'accommodation': ['hotel', 'motel', 'inn', 'resort', 'lodging', 'bnb', 'guesthouse'],
        'education': ['school', 'university', 'college', 'academy', 'training', 'education'],
        'automotive': ['car', 'auto', 'automotive', 'repair', 'mechanic', 'dealership'],
        'finance': ['bank', 'finance', 'insurance', 'accounting', 'financial', 'credit']
    }

    for category, keywords in categories.items():
        if any(keyword in text for keyword in keywords):
            return category.title()

    return "Other"

def extract_social_media(page: Page) -> Dict[str, str]:
    """Extract social media links from business page."""
    social_media = {}

    try:
        # Look for social media links in the page content
        page_content = page.content()

        social_patterns = {
            'facebook': r'facebook\.com/[^"\s]+',
            'instagram': r'instagram\.com/[^"\s]+',
            'twitter': r'twitter\.com/[^"\s]+|x\.com/[^"\s]+',
            'linkedin': r'linkedin\.com/[^"\s]+'
        }

        for platform, pattern in social_patterns.items():
            matches = re.findall(pattern, page_content)
            if matches:
                # Clean up the URL and ensure it starts with https://
                url = matches[0].strip('/')
                if not url.startswith(('http://', 'https://')):
                    url = f"https://{url}"
                social_media[platform] = url

    except Exception as e:
        logging.warning(f"Failed to extract social media: {e}")

    return social_media

def extract_text(page: Page, xpath: str) -> str:
    try:
        if page.locator(xpath).count() > 0:
            return page.locator(xpath).inner_text()
    except Exception as e:
        logging.warning(f"Failed to extract text for xpath {xpath}: {e}")
    return ""

@retry_on_failure(max_retries=3, delay=1)
def extract_emails_from_website(website_url: str, *, email_filter_mode: str = "strict") -> str:
    """
    Extract email addresses from a website, filtering out support emails.
    Returns the first valid business email found, or empty string if none found.
    """
    if not website_url or website_url == "None Found":
        return ""

    try:
        # Normalize URL
        if not website_url.startswith(('http://', 'https://')):
            website_url = 'https://' + website_url

        # Parse the base URL
        parsed_url = urlparse(website_url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

        # Use requests to get the page content
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        # Try multiple pages: homepage, contact page, about page
        pages_to_try = [
            website_url,  # Homepage
            urljoin(base_url, '/contact'),
            urljoin(base_url, '/contact-us'),
            urljoin(base_url, '/about'),
            urljoin(base_url, '/about-us'),
        ]

        all_emails = []

        for page_url in pages_to_try:
            try:
                response = requests.get(page_url, headers=headers, timeout=10)
                response.raise_for_status()

                # Extract emails using regex - more strict pattern
                email_pattern = r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
                emails = re.findall(email_pattern, response.text)

                # Add to collection
                all_emails.extend(emails)

                # If we found emails on this page, we can stop trying other pages
                if emails:
                    break

            except Exception:
                continue  # Try next page

        email_filter_mode = (email_filter_mode or "strict").lower()
        if email_filter_mode not in {"strict", "balanced", "none"}:
            email_filter_mode = "strict"

        if email_filter_mode == "none":
            if all_emails:
                logging.info(f"Found email for {website_url}: {all_emails[0]}")
                return all_emails[0]
            logging.info(f"No email found for {website_url}")
            return ""

        filtered_emails = []
        support_keywords = [
            'support', 'help', 'info', 'contact', 'admin', 'noreply', 'no-reply',
            'sales', 'feedback', 'abuse', 'webmaster'
        ]
        placeholders = ['user@domain.com', 'example.com', 'yourname@', 'email@domain.com']
        invalid_substrings = ['.jpg', '.png', '.gif', '.pdf', '.zip', '@mobile', '@desktop']

        for email in all_emails:
            email_lower = email.lower()
            # Skip obvious placeholders
            if any(placeholder in email_lower for placeholder in placeholders):
                continue
            # Skip emails with file extensions or invalid characters
            if any(invalid in email_lower for invalid in invalid_substrings):
                continue
            # Skip emails that don't have proper domain structure
            if email.count('@') != 1 or email.count('.') < 1:
                continue
            if email_filter_mode == "strict":
                if any(keyword in email_lower for keyword in support_keywords):
                    continue
            if email not in filtered_emails:
                filtered_emails.append(email)

        if filtered_emails:
            logging.info(f"Found email for {website_url}: {filtered_emails[0]}")
            return filtered_emails[0]
        logging.info(f"No valid email found for {website_url}")
        return ""

    except Exception as e:
        logging.warning(f"Failed to extract email from {website_url}: {e}")
        return ""

def extract_place(
    page: Page,
    extract_emails: bool = True,
    email_filter_mode: str = "strict",
) -> Place:
    # XPaths
    name_xpath = '//div[@class="TIHn2 "]//h1[@class="DUwDvf lfPIob"]'
    address_xpath = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
    website_xpath = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
    phone_number_xpath = '//button[contains(@data-item-id, "phone:tel:")]//div[contains(@class, "fontBodyMedium")]'
    reviews_count_xpath = '//div[@class="TIHn2 "]//div[@class="fontBodyMedium dmRWX"]//div//span//span//span[@aria-label]'
    reviews_average_xpath = '//div[@class="TIHn2 "]//div[@class="fontBodyMedium dmRWX"]//div//span[@aria-hidden]'
    info1 = '//div[@class="LTs0Rc"][1]'
    info2 = '//div[@class="LTs0Rc"][2]'
    info3 = '//div[@class="LTs0Rc"][3]'
    opens_at_xpath = '//button[contains(@data-item-id, "oh")]//div[contains(@class, "fontBodyMedium")]'
    opens_at_xpath2 = '//div[@class="MkV9"]//span[@class="ZDu9vd"]//span[2]'
    place_type_xpath = '//div[@class="LBgpqf"]//button[@class="DkEaL "]'
    intro_xpath = '//div[@class="WeS02d fontBodyMedium"]//div[@class="PYvSYb "]'

    place = Place()
    place.name = extract_text(page, name_xpath)
    place.address = extract_text(page, address_xpath)
    place.website = extract_text(page, website_xpath)
    # Extract email from website if available
    place.email = (
        extract_emails_from_website(place.website, email_filter_mode=email_filter_mode)
        if extract_emails
        else ""
    )
    place.phone_number = extract_text(page, phone_number_xpath)
    place.place_type = extract_text(page, place_type_xpath)
    place.introduction = extract_text(page, intro_xpath) or "None Found"

    # Extract social media links
    social_media = extract_social_media(page)
    place.facebook = social_media.get('facebook', '')
    place.instagram = social_media.get('instagram', '')
    place.twitter = social_media.get('twitter', '')
    place.linkedin = social_media.get('linkedin', '')

    # Classify business type
    place.business_category = classify_business_type(
        place.name, place.introduction, place.place_type
    )

    # Reviews Count
    reviews_count_raw = extract_text(page, reviews_count_xpath)
    if reviews_count_raw:
        try:
            temp = reviews_count_raw.replace('\xa0', '').replace('(','').replace(')','').replace(',','')
            place.reviews_count = int(temp)
        except Exception as e:
            logging.warning(f"Failed to parse reviews count: {e}")
    # Reviews Average
    reviews_avg_raw = extract_text(page, reviews_average_xpath)
    if reviews_avg_raw:
        try:
            temp = reviews_avg_raw.replace(' ','').replace(',','.')
            place.reviews_average = float(temp)
        except Exception as e:
            logging.warning(f"Failed to parse reviews average: {e}")
    # Store Info
    for idx, info_xpath in enumerate([info1, info2, info3]):
        info_raw = extract_text(page, info_xpath)
        if info_raw:
            temp = info_raw.split('·')
            if len(temp) > 1:
                check = temp[1].replace("\n", "").lower()
                if 'shop' in check:
                    place.store_shopping = "Yes"
                if 'pickup' in check:
                    place.in_store_pickup = "Yes"
                if 'delivery' in check:
                    place.store_delivery = "Yes"
    # Opens At
    opens_at_raw = extract_text(page, opens_at_xpath)
    if opens_at_raw:
        opens = opens_at_raw.split('⋅')
        if len(opens) > 1:
            place.opens_at = opens[1].replace("\u202f","")
        else:
            place.opens_at = opens_at_raw.replace("\u202f","")
    else:
        opens_at2_raw = extract_text(page, opens_at_xpath2)
        if opens_at2_raw:
            opens = opens_at2_raw.split('⋅')
            if len(opens) > 1:
                place.opens_at = opens[1].replace("\u202f","")
            else:
                place.opens_at = opens_at2_raw.replace("\u202f","")
    return place

def scrape_places(
    search_for: str,
    total: int,
    *,
    include_without_email: bool = False,
    extract_emails: bool = True,
    email_filter_mode: str = "strict",
    headless: bool = True,
    max_scroll_attempts: int = 20,
    max_listings: Optional[int] = None,
    dedup_enabled: bool = True,
    dedup_db_path: Optional[str] = None,
    show_tqdm: bool = True,
    progress_callback: Optional[Callable[[Dict[str, object]], None]] = None,
) -> tuple[List[Place], ScrapingStats]:
    setup_logging()
    places: List[Place] = []
    stats = ScrapingStats()
    stats.start_time = time.strftime('%Y-%m-%d %H:%M:%S')
    stats.target_leads = total
    listings_processed = 0
    listings_total = 0

    def send_progress(
        message: str,
        listing_index: Optional[int] = None,
        current_found: Optional[int] = None,
    ) -> None:
        if not progress_callback:
            return
        progress_callback({
            "message": message,
            "processed": listings_processed,
            "found": len(places),
            "target": total,
            "successful": stats.successful_scrapes,
            "failed": stats.failed_scrapes,
            "duplicates_skipped": stats.duplicates_skipped,
            "emails_found": stats.emails_found,
            "websites_visited": stats.websites_visited,
            "listing_index": listing_index,
            "listings_total": listings_total,
            "current_found": current_found,
        })

    if max_listings is None:
        # Default to target if not specified to avoid scanning the entire list by default
        max_listings = total
    else:
        max_listings = max(max_listings, total)

    dedup_conn = None
    if dedup_enabled:
        if not dedup_db_path:
            dedup_db_path = os.path.join("results", "dedup.sqlite")
        dedup_conn = init_dedup_db(dedup_db_path)

    start_time = time.time()

    try:
        with sync_playwright() as p:
            send_progress("Launching browser")
            if platform.system() == "Windows":
                browser_path = r"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
                browser = p.chromium.launch(
                    executable_path=browser_path, 
                    headless=headless,
                    args=["--disable-blink-features=AutomationControlled"]
                )
            else:
                browser = p.chromium.launch(
                    headless=headless,
                    args=["--disable-blink-features=AutomationControlled"]
                )
            # Set a common user agent to avoid bot detection and inconsistent UI
            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            context = browser.new_context(user_agent=user_agent)
            page = context.new_page()
            try:
                logging.info("Navigating to Google Maps (English)...")
                send_progress("Loading Google Maps")
                # Force English and a standard viewport
                page.set_viewport_size({"width": 1280, "height": 720})
                page.goto("https://www.google.com/maps?hl=en", timeout=60000)
                # Use a more reliable way to wait for the page to be ready
                page.wait_for_load_state("domcontentloaded")
                page.wait_for_timeout(3000)
                
                # More aggressive cookie consent handling
                try:
                    consent_buttons = [
                        'button:has-text("Accept all")',
                        'button:has-text("I agree")',
                        'button:has-text("Alle akzeptieren")', # German variant
                        '#L2AGLb', # Specific ID for Google consent
                        '//button[@aria-label="Accept all"]'
                    ]
                    for selector in consent_buttons:
                        btn = page.locator(selector)
                        if btn.is_visible(timeout=3000):
                            btn.click()
                            logging.info(f"Clicked consent button: {selector}")
                            page.wait_for_timeout(2000)
                            break
                except Exception:
                    pass

                logging.info(f"Searching for: {search_for}")
                send_progress(f"Searching for: {search_for}")
                
                # Try multiple search box selectors
                search_box = None
                search_box_selectors = [
                    '//input[@id="searchboxinput"]',
                    'input[name="q"]',
                    '#searchboxinput',
                    '.searchboxinput'
                ]
                
                for selector in search_box_selectors:
                    try:
                        loc = page.locator(selector)
                        if loc.is_visible(timeout=5000):
                            search_box = loc
                            logging.info(f"Found search box with selector: {selector}")
                            break
                    except Exception:
                        continue

                if not search_box:
                    logging.error("Search box not found after multiple attempts. Saving state.")
                    page.screenshot(path="debug_search_box_error.png")
                    with open("debug_page_content.html", "w") as f:
                        f.write(page.content())
                    raise Exception("Could not find Google Maps search input. See debug files.")

                search_box.fill(search_for)
                page.keyboard.press("Enter")
                
                # Wait for results to start appearing
                try:
                    page.wait_for_selector('//a[contains(@href, "https://www.google.com/maps/place")]', timeout=15000)
                except Exception:
                    logging.warning("No results found or page load slow. Continuing to check...")

                page.hover('//a[contains(@href, "https://www.google.com/maps/place")]')
                previously_counted = 0
                scroll_attempts = 0
                while scroll_attempts < max_scroll_attempts: # Limit scroll attempts to avoid infinite loop
                    page.mouse.wheel(0, 5000)
                    page.wait_for_timeout(2000) # Give time for content to load
                    
                    found = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').count()
                    logging.info(f"Currently Found: {found}")
                    send_progress(f"Currently Found: {found}", current_found=found)
                    
                    if max_listings is not None and found >= max_listings:
                        break
                    if found == previously_counted:
                        scroll_attempts += 1
                    else:
                        scroll_attempts = 0 # Reset if we found new results
                    
                    previously_counted = found
                
                listings = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').all()
                # Wrap in parent element if needed, but only if they exist
                listings = [listing.locator("xpath=..") for listing in listings]
                logging.info(f"Total listings available: {len(listings)}")
                listings_total = len(listings)
                send_progress(f"Listings available: {listings_total}")

                with tqdm(total=total, desc="Finding leads", unit="lead", disable=not show_tqdm) as pbar:
                    for idx, listing in enumerate(listings):
                        if len(places) >= total:
                            logging.info(f"Reached target of {total} leads, stopping.")
                            break
                        if max_listings is not None and listings_processed >= max_listings:
                            logging.info(f"Reached scan limit of {max_listings} listings, stopping.")
                            break
                        listings_processed += 1
                        try:
                            send_progress(f"Processing listing {idx + 1}", listing_index=idx + 1)
                            listing.click()
                            page.wait_for_selector('//div[@class="TIHn2 "]//h1[@class="DUwDvf lfPIob"]', timeout=10000)
                            time.sleep(1.5)  # Give time for details to load
                            place = extract_place(
                                page,
                                extract_emails=extract_emails,
                                email_filter_mode=email_filter_mode,
                            )
                            if extract_emails and place.website and place.website != "None Found":
                                stats.websites_visited += 1
                            should_save = bool(place.name) and (include_without_email or bool(place.email))
                            fingerprint = None
                            if should_save and dedup_conn:
                                fingerprint = build_fingerprint(place)
                                if is_duplicate(dedup_conn, fingerprint, place.email):
                                    stats.duplicates_skipped += 1
                                    logging.info(f"Duplicate skipped: {place.name}")
                                    send_progress(f"Duplicate skipped: {place.name}", listing_index=idx + 1)
                                    continue
                            if should_save:
                                places.append(place)
                                stats.successful_scrapes += 1

                                # Update statistics
                                if place.email:
                                    stats.emails_found += 1
                                if any([place.facebook, place.instagram, place.twitter, place.linkedin]):
                                    stats.social_media_found += 1
                                if place.email:
                                    logging.info(f"Lead found with email: {place.name} - {place.email}")
                                    send_progress(f"Saved lead: {place.name}", listing_index=idx + 1)
                                else:
                                    logging.info(f"Lead saved without email: {place.name}")
                                    send_progress(f"Saved lead without email: {place.name}", listing_index=idx + 1)
                                if dedup_conn:
                                    upsert_lead(dedup_conn, fingerprint or build_fingerprint(place), place)
                                pbar.update(1)
                            elif place.name:
                                stats.failed_scrapes += 1
                                logging.info(f"Business '{place.name}' found but no valid email, skipping.")
                                send_progress(f"Skipped (no email): {place.name}", listing_index=idx + 1)
                            else:
                                stats.failed_scrapes += 1
                                logging.warning(f"No name found for listing {idx+1}, skipping.")
                                send_progress(f"Skipped listing {idx + 1} with missing name", listing_index=idx + 1)
                        except Exception as e:
                            stats.failed_scrapes += 1
                            logging.warning(f"Failed to extract listing {idx+1}: {e}")
                            send_progress(f"Error on listing {idx + 1}: {e}", listing_index=idx + 1)
            finally:
                try:
                    browser.close()
                except Exception as e:
                    logging.warning(f"Failed to close browser cleanly: {e}")
    finally:
        if dedup_conn:
            dedup_conn.close()

    # Calculate final statistics
    end_time = time.time()
    stats.end_time = time.strftime('%Y-%m-%d %H:%M:%S')
    stats.total_searched = listings_processed
    stats.average_time_per_business = (end_time - start_time) / listings_processed if listings_processed else 0

    return places, stats

def save_places_to_csv(places: List[Place], output_path: str = "result.csv", append: bool = False):
    """Save places to CSV. Uses pandas if available, otherwise falls back to the csv module."""
    rows = [asdict(place) for place in places]
    if not rows:
        logging.warning("No data to save; list of places is empty.")
        return

    try:
        import pandas as pd
        df = pd.DataFrame(rows)
        column_order = [field.name for field in fields(Place)]
        for column in column_order:
            if column not in df.columns:
                df[column] = ""
        df = df.reindex(columns=column_order)
        file_exists = os.path.isfile(output_path)
        if append and file_exists:
            with open(output_path, newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                existing_header = next(reader, None)
            if existing_header:
                missing = [col for col in existing_header if col not in df.columns]
                for col in missing:
                    df[col] = ""
                if existing_header != list(df.columns):
                    logging.warning(
                        "Output columns differ from existing file. "
                        "Aligning to existing header to keep CSV consistent."
                    )
                df = df.reindex(columns=existing_header)
        mode = "a" if append else "w"
        header = not (append and file_exists)
        df.to_csv(output_path, index=False, mode=mode, header=header)
        logging.info(f"Saved {len(df)} places to {output_path} (append={append}) using pandas")
    except Exception as e:
        logging.warning(f"Pandas unavailable or failed ({e}); falling back to csv writer.")
        # Use csv.DictWriter as a safe fallback
        column_order = [field.name for field in fields(Place)]
        file_exists = os.path.isfile(output_path)
        mode = 'a' if append else 'w'
        header_needed = not (append and file_exists)
        with open(output_path, mode=mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=column_order)
            if header_needed:
                writer.writeheader()
            for row in rows:
                # Ensure all keys exist
                out = {k: row.get(k, "") for k in column_order}
                writer.writerow(out)
        logging.info(f"Saved {len(rows)} places to {output_path} (append={append}) using csv module")

def generate_report(stats: ScrapingStats, output_path: str):
    """Generate a scraping report."""
    report_path = output_path.replace('.csv', '_report.txt')

    success_rate = (stats.successful_scrapes / stats.total_searched * 100) if stats.total_searched > 0 else 0
    email_rate = (stats.emails_found / stats.successful_scrapes * 100) if stats.successful_scrapes > 0 else 0
    social_rate = (stats.social_media_found / stats.successful_scrapes * 100) if stats.successful_scrapes > 0 else 0

    report = f"""
🗺️ Google Maps Scraper Report
{'='*50}

📊 SCRAPING SUMMARY
Started: {stats.start_time}
Completed: {stats.end_time}
Duration: {stats.average_time_per_business * stats.total_searched:.1f} seconds

🎯 BUSINESS RESULTS
Target leads: {stats.target_leads}
Leads found: {stats.successful_scrapes}
Businesses processed: {stats.total_searched}
Failed extractions: {stats.failed_scrapes}
Success rate: {success_rate:.1f}%

📧 EMAIL EXTRACTION
Emails found: {stats.emails_found}
Email success rate: {email_rate:.1f}%
Websites visited: {stats.websites_visited}
Duplicates skipped: {stats.duplicates_skipped}

📱 SOCIAL MEDIA
Businesses with social media: {stats.social_media_found}
Social media rate: {social_rate:.1f}%

⏱️  PERFORMANCE
Average time per business: {stats.average_time_per_business:.2f}s
Total processing time: {stats.average_time_per_business * stats.total_searched:.1f}s

📁 Output file: {output_path}
📄 Report file: {report_path}
"""

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"\n📊 Scraping Report Generated:")
    print(f"   Target Leads: {stats.target_leads}")
    print(f"   Leads Found: {stats.successful_scrapes}")
    print(f"   Success Rate: {success_rate:.1f}%")
    print(f"   Emails Found: {stats.emails_found}")
    print(f"   Duplicates Skipped: {stats.duplicates_skipped}")
    print(f"   Social Media: {stats.social_media_found}")
    print(f"   Report saved to: {report_path}")

def get_user_input(results_folder: str = "results"):
    """Get user input interactively."""
    print("=" * 60)
    print("🗺️  Google Maps Scraper - Interactive Mode")
    print("=" * 60)
    print("This tool will scrape business information from Google Maps.")
    print("It will also visit each business website to extract contact emails.")
    print("Features: Progress bars, social media extraction, business classification, and detailed statistics.")
    print("Runs in headless mode (no visible browser windows).")
    print("Support emails are automatically filtered out.\n")
    
    # Get search query
    search_for = input("📍 Enter your search query (e.g., 'restaurants in New York'): ").strip()
    if not search_for:
        search_for = "turkish stores in toronto Canada"
        print(f"   ℹ️  Using default: {search_for}")
    
    # Get number of results
    while True:
        total_input = input("\n🔢 How many results to scrape? (default: 10, max recommended: 100): ").strip()
        if not total_input:
            total = 10
            break
        try:
            total = int(total_input)
            if total <= 0:
                print("   ⚠️  Please enter a positive number.")
                continue
            if total > 100:
                confirm = input(f"   ⚠️  {total} is quite large. This may take a while. Continue? (y/N): ").strip().lower()
                if confirm not in ['y', 'yes']:
                    continue
            break
        except ValueError:
            print("   ⚠️  Please enter a valid number.")
    
    # Get output file path
    default_filename = "results.csv"
    output_path = input(f"\n💾 Output file name (default: {default_filename}): ").strip()
    if not output_path:
        output_path = default_filename
    elif not output_path.endswith('.csv'):
        output_path += '.csv'
    
    # Prepend results folder to the path
    output_path = os.path.join(results_folder, output_path)
    
    # Check if file exists and ask about append mode
    file_exists = os.path.exists(output_path)
    if file_exists:
        print(f"   ℹ️  File '{output_path}' already exists.")
        append_input = input("   📝 Append to existing file or overwrite? (a/O): ").strip().lower()
        append = append_input in ['a', 'append']
    else:
        append = False
    
    return search_for, total, output_path, append

def main():
    # Create results folder if it doesn't exist
    results_folder = "results"
    os.makedirs(results_folder, exist_ok=True)
    
    # Check if command line arguments are provided
    import sys
    if len(sys.argv) > 1:
        # Use original argument parsing for backward compatibility
        parser = argparse.ArgumentParser()
        parser.add_argument("-s", "--search", type=str, help="Search query for Google Maps")
        parser.add_argument("-t", "--total", type=int, help="Total number of results to scrape")
        parser.add_argument("-o", "--output", type=str, default=os.path.join(results_folder, "result.csv"), help="Output CSV file path")
        parser.add_argument("--append", action="store_true", help="Append results to the output file instead of overwriting")
        parser.add_argument("--include-without-email", action="store_true", help="Save listings even if no email is found")
        parser.add_argument("--no-email-extraction", action="store_true", help="Skip visiting websites to extract emails")
        parser.add_argument("--email-filter-mode", choices=["strict", "balanced", "none"], default="strict", help="Email filtering mode")
        parser.add_argument("--max-scroll-attempts", type=int, default=20, help="Max scroll attempts to load listings")
        parser.add_argument("--max-listings", type=int, default=None, help="Max listings to scan before stopping")
        parser.add_argument("--save-everything", action="store_true", help="Save all listings and keep unfiltered emails")
        parser.add_argument("--dedup-db", type=str, default=None, help="SQLite DB path for deduplication")
        parser.add_argument("--no-dedup", action="store_true", help="Disable deduplication")
        args = parser.parse_args()
        search_for = args.search or "turkish stores in toronto Canada"
        total = args.total or 1
        # If output path doesn't start with results folder, prepend it
        output_path = args.output
        if not output_path.startswith(results_folder + os.sep) and not os.path.isabs(output_path):
            output_path = os.path.join(results_folder, output_path)
        append = args.append
        include_without_email = args.include_without_email
        extract_emails = not args.no_email_extraction
        email_filter_mode = args.email_filter_mode
        max_scroll_attempts = args.max_scroll_attempts
        max_listings = args.max_listings
        dedup_db_path = args.dedup_db
        dedup_enabled = not args.no_dedup

        if args.save_everything:
            include_without_email = True
            extract_emails = True
            email_filter_mode = "none"
    else:
        # Use interactive prompts
        search_for, total, output_path, append = get_user_input(results_folder)
        include_without_email = False
        extract_emails = True
        email_filter_mode = "strict"
        max_scroll_attempts = 20
        max_listings = None
        dedup_db_path = None
        dedup_enabled = True
    
    print(f"\n🚀 Starting scrape...")
    print(f"   Search: {search_for}")
    print(f"   Results: {total}")
    print(f"   Output: {output_path}")
    print(f"   Mode: {'Append' if append else 'Overwrite'}")
    print("\n" + "=" * 50)
    
    places, stats = scrape_places(
        search_for,
        total,
        include_without_email=include_without_email,
        extract_emails=extract_emails,
        email_filter_mode=email_filter_mode,
        max_scroll_attempts=max_scroll_attempts,
        max_listings=max_listings,
        dedup_enabled=dedup_enabled,
        dedup_db_path=dedup_db_path,
    )
    save_places_to_csv(places, output_path, append=append)
    generate_report(stats, output_path)

if __name__ == "__main__":
    main()
