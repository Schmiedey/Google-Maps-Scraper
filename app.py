from __future__ import annotations
import os
import time
import csv

import streamlit as st
st.set_page_config(page_title="Maps Lead Studio", layout="wide")

import os
import time
import csv
import logging
import warnings

from main import scrape_places, save_places_to_csv, generate_report

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_RESULTS_DIR_NAME = "../results"
DEFAULT_RESULTS_DIR = os.path.join(BASE_DIR, DEFAULT_RESULTS_DIR_NAME)


def repair_csv_columns(path: str) -> str:
    with open(path, newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    if not rows:
        return ""

    header = rows[0]
    if not header:
        return ""

    data_rows = rows[1:]
    if not data_rows:
        return ""

    row_lengths = {len(row) for row in data_rows}
    max_len = max(row_lengths)
    min_len = min(row_lengths)

    if max_len == min_len == len(header):
        return ""

    if "in_store_pickup" not in header:
        return ""

    extra_fields = max_len - len(header)
    if extra_fields not in {1, 2}:
        return ""

    if any(length not in {len(header), len(header) + extra_fields} for length in row_lengths):
        return ""

    insert_at = header.index("in_store_pickup")
    new_header = header[:insert_at] + ["store_shopping"] + header[insert_at:]

    if extra_fields == 2:
        insert_delivery_at = new_header.index("in_store_pickup") + 1
        new_header = (
            new_header[:insert_delivery_at]
            + ["store_delivery"]
            + new_header[insert_delivery_at:]
        )

    new_rows = [new_header]
    for row in data_rows:
        if len(row) == len(header):
            row = row[:insert_at] + [""] + row[insert_at:]
            if extra_fields == 2:
                insert_delivery_at = new_header.index("store_delivery")
                row = row[:insert_delivery_at] + [""] + row[insert_delivery_at:]
        new_rows.append(row)

    if path.lower().endswith(".csv"):
        repaired_path = path[:-4] + "_repaired.csv"
    else:
        repaired_path = path + "_repaired.csv"

    with open(repaired_path, "w", newline="", encoding="utf-8") as handle:
        csv.writer(handle).writerows(new_rows)

    return repaired_path


def load_csv_safe(path: str) -> tuple:
    import pandas as pd
    try:
        return pd.read_csv(path), ""
    except pd.errors.ParserError as exc:
        repaired_path = repair_csv_columns(path)
        if repaired_path:
            df = pd.read_csv(repaired_path)
            warning = (
                "Detected inconsistent columns in the CSV. "
                f"Created a repaired file at {repaired_path}."
            )
            return df, warning
        raise exc


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Serif:wght@600;700&family=JetBrains+Mono:wght@400;600&display=swap');
        :root {
            --ink: #111827;
            --muted: #475569;
            --bg-1: #f8fafc;
            --bg-2: #e2e8f0;
            --bg-3: #fff7ed;
            --panel: #ffffff;
            --panel-muted: #f8fafc;
            --border: #d1d5db;
            --accent: #0284c7;
            --accent-strong: #0369a1;
            --accent-warm: #f97316;
            --focus: rgba(2, 132, 199, 0.25);
            color-scheme: light;
        }
        header[data-testid="stHeader"] {
            display: none;
        }
        section[data-testid="stSidebar"] {
            top: 0;
        }
        .block-container {
            max-width: 1200px;
        }
        .block-container {
            padding-top: 2rem;
            padding-bottom: 3rem;
        }
        h1, h2, h3, h4 {
            font-family: "IBM Plex Serif", serif;
            letter-spacing: 0.2px;
        }
        p, label, input, textarea, button, small {
            font-family: "IBM Plex Sans", sans-serif;
            color: var(--ink);
        }
        code, pre {
            font-family: "JetBrains Mono", ui-monospace, monospace;
        }
        small, div[data-testid="stCaption"] {
            color: var(--muted);
        }
        label {
            font-weight: 600;
        }
        .hero {
            display: grid;
            grid-template-columns: minmax(0, 1.2fr) minmax(0, 0.8fr);
            gap: 1.8rem;
            padding: 2.2rem 2.4rem;
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 28px;
            box-shadow: 0 20px 45px rgba(15, 23, 42, 0.08);
            margin-bottom: 1.6rem;
        }
        .hero-eyebrow {
            text-transform: uppercase;
            letter-spacing: 0.22em;
            font-size: 0.72rem;
            color: var(--muted);
        }
        .hero-title {
            font-size: 2.6rem;
            font-weight: 700;
            margin: 0.4rem 0 0.6rem 0;
        }
        .hero-subtitle {
            font-size: 1.05rem;
            color: var(--muted);
            max-width: 36rem;
        }
        .hero-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
            background: var(--accent);
            color: #ffffff;
            padding: 0.45rem 1.1rem;
            border-radius: 999px;
            font-size: 0.85rem;
            font-weight: 600;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }
        .hero-card {
            display: flex;
            flex-direction: column;
            gap: 0.75rem;
            background: var(--panel-muted);
            color: var(--ink);
            border-radius: 20px;
            padding: 1.4rem 1.6rem;
            border: 1px solid var(--border);
        }
        .hero-card h4 {
            margin: 0;
            font-size: 1.1rem;
            color: var(--ink);
        }
        .hero-card p {
            margin: 0;
            color: var(--muted);
        }
        .hero-stat {
            display: flex;
            justify-content: space-between;
            padding: 0.55rem 0.75rem;
            border-radius: 12px;
            background: #ffffff;
            border: 1px solid var(--border);
            font-size: 0.9rem;
        }
        .section-title {
            font-size: 1.3rem;
            font-weight: 600;
            margin-bottom: 0.4rem;
        }
        .section-subtitle {
            color: var(--muted);
            font-size: 0.95rem;
        }
        .panel {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 22px;
            padding: 1.4rem 1.6rem;
            box-shadow: 0 18px 30px rgba(15, 23, 42, 0.05);
        }
        .pill {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            border-radius: 999px;
            padding: 0.3rem 0.8rem;
            background: rgba(2, 132, 199, 0.12);
            color: var(--accent-strong);
            font-size: 0.8rem;
            font-weight: 600;
        }
        section[data-testid="stSidebar"],
        section[data-testid="stSidebar"] > div,
        div[data-testid="stSidebar"] {
            background: var(--panel) !important;
            border-right: 1px solid var(--border);
        }
        section[data-testid="stSidebar"] .stMarkdown,
        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] p,
        section[data-testid="stSidebar"] span,
        section[data-testid="stSidebar"] small,
        div[data-testid="stSidebar"] .stMarkdown,
        div[data-testid="stSidebar"] label,
        div[data-testid="stSidebar"] p,
        div[data-testid="stSidebar"] span {
            color: var(--ink);
        }
        section[data-testid="stSidebar"] div[data-baseweb="input"],
        section[data-testid="stSidebar"] div[data-baseweb="textarea"],
        section[data-testid="stSidebar"] div[data-baseweb="select"] {
            background: var(--panel);
            border: 1px solid var(--border);
        }
        section[data-testid="stSidebar"] div[data-baseweb="input"] input,
        section[data-testid="stSidebar"] div[data-baseweb="textarea"] textarea {
            color: var(--ink) !important;
            background: var(--panel) !important;
        }
        section[data-testid="stSidebar"] input,
        section[data-testid="stSidebar"] textarea {
            background: var(--panel) !important;
            color: var(--ink) !important;
        }
        .stButton > button {
            background: var(--accent);
            color: #ffffff;
            border: none;
            padding: 0.7rem 1.6rem;
            border-radius: 999px;
            font-weight: 600;
            letter-spacing: 0.01em;
            box-shadow: 0 10px 18px rgba(2, 132, 199, 0.22);
        }
        .stButton > button:hover {
            background: var(--accent-strong);
            color: #ffffff;
        }
        button[kind="primary"] {
            background: var(--accent) !important;
            color: #ffffff !important;
            border: none !important;
        }
        button[kind="primary"]:hover {
            background: var(--accent-strong) !important;
        }
        .stDownloadButton > button {
            background: var(--panel);
            color: var(--ink);
            border: 1px solid var(--border);
            padding: 0.6rem 1.2rem;
            border-radius: 999px;
            font-weight: 600;
        }
        .stDownloadButton > button:hover {
            border-color: var(--accent);
            color: var(--accent-strong);
        }
        div[data-testid="stMetric"] {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 0.7rem 0.9rem;
            box-shadow: 0 12px 22px rgba(15, 23, 42, 0.06);
        }
        div[data-testid="stMetric"] label {
            font-size: 0.82rem;
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.12em;
        }
        div[data-testid="stMetric"] [data-testid="stMetricValue"] {
            color: var(--ink) !important;
            font-weight: 600;
        }
        div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
            color: var(--muted) !important;
        }
        div[data-baseweb="input"],
        div[data-baseweb="textarea"],
        div[data-baseweb="select"] {
            background: var(--panel) !important;
            border: 1px solid var(--border) !important;
            border-radius: 14px;
        }
        div[data-baseweb="input"] > div,
        div[data-baseweb="textarea"] > div {
            background: var(--panel) !important;
        }
        div[data-baseweb="input"]:focus-within,
        div[data-baseweb="textarea"]:focus-within {
            border-color: var(--accent);
            box-shadow: 0 0 0 3px var(--focus);
        }
        div[data-baseweb="input"] input,
        div[data-baseweb="textarea"] textarea {
            background: var(--panel) !important;
            color: var(--ink) !important;
            padding: 0.55rem 0.7rem;
            line-height: 1.4;
            font-size: 0.95rem;
            caret-color: var(--accent);
        }
        div[data-baseweb="input"] input::placeholder,
        div[data-baseweb="textarea"] textarea::placeholder {
            color: #94a3b8;
        }
        div[data-baseweb="input"] input {
            overflow-x: auto;
            white-space: nowrap;
            text-overflow: clip;
        }
        div[data-baseweb="input"] input,
        div[data-baseweb="textarea"] textarea {
            -webkit-text-fill-color: var(--ink);
        }
        div[data-baseweb="textarea"] textarea {
            white-space: pre-wrap;
            overflow: auto;
        }
        div[data-baseweb="input"] input::-webkit-scrollbar,
        div[data-baseweb="textarea"] textarea::-webkit-scrollbar {
            height: 6px;
        }
        div[data-baseweb="input"] input::-webkit-scrollbar-thumb,
        div[data-baseweb="textarea"] textarea::-webkit-scrollbar-thumb {
            background: #94a3b8;
            border-radius: 999px;
        }
        div[data-testid="stProgress"] > div > div {
            background: var(--accent);
        }
        div[data-testid="stTabs"] button {
            font-weight: 600;
        }
        div[data-testid="stCodeBlock"] {
            background: #f8fafc !important;
            color: var(--ink) !important;
            border-radius: 12px;
            border: 1px solid var(--border);
        }
        div[data-testid="stCodeBlock"] code {
            color: var(--ink) !important;
        }
        div[data-testid="stCodeBlock"] pre,
        div[data-testid="stCode"] pre,
        div[data-testid="stCode"] code {
            background: #f8fafc !important;
            color: var(--ink) !important;
        }
        div[data-testid="stCode"] {
            background: #f8fafc !important;
            color: var(--ink) !important;
            border-radius: 12px;
            border: 1px solid var(--border);
        }
        section[data-testid="stSidebar"] div[data-testid="stCodeBlock"],
        section[data-testid="stSidebar"] div[data-testid="stCode"] {
            background: #f8fafc !important;
            color: var(--ink) !important;
            border: 1px solid var(--border);
        }
        div[data-testid="stAlert"] {
            background: #fff7ed;
            border: 1px solid #fdba74;
            color: #9a3412;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def normalize_results_dir(path_value: str) -> str:
    cleaned = os.path.expanduser(path_value.strip()) if path_value else ""
    if not cleaned:
        return DEFAULT_RESULTS_DIR
    if os.path.isabs(cleaned):
        return cleaned
    return os.path.join(BASE_DIR, cleaned)


def normalize_output_name(name: str) -> str:
    cleaned = name.strip() if name else ""
    if not cleaned:
        cleaned = "results.csv"
    if not cleaned.lower().endswith(".csv"):
        cleaned += ".csv"
    return cleaned


def render_hero() -> None:
    st.markdown(
        """
        <div class="hero">
            <div>
                <div class="hero-eyebrow">Google Maps Scraper</div>
                <div class="hero-title">Maps Lead Command Center</div>
                <div class="hero-subtitle">
                    Orchestrate high-quality lead extraction with real-time progress,
                    enriched contact data, and instantly shareable exports.
                </div>
                <div style="margin-top: 0.9rem; display: flex; gap: 0.5rem; flex-wrap: wrap;">
                    <span class="pill">Live scraping</span>
                    <span class="pill">Email enrichment</span>
                    <span class="pill">Instant CSV export</span>
                </div>
            </div>
            <div class="hero-card">
                <div class="hero-badge">Playwright + pandas</div>
                <h4>Run quality at a glance</h4>
                <p>Monitor throughput, completion, and deliverables while the scraper runs.</p>
                <div class="hero-stat">
                    <span>Live progress</span>
                    <strong>Streaming</strong>
                </div>
                <div class="hero-stat">
                    <span>Data pipeline</span>
                    <strong>Clean CSV</strong>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    apply_theme()
    render_hero()

    if "last_output_path" not in st.session_state:
        st.session_state["last_output_path"] = ""
        st.session_state["last_report_path"] = ""
        st.session_state["last_stats"] = None
        st.session_state["last_output_df"] = None
        st.session_state["last_duration"] = None
        st.session_state["last_output_warning"] = ""

    with st.sidebar:
        st.markdown("## Run setup")
        st.caption("Configure the query, targets, and capture options.")
        with st.form("scrape_form"):
            search_for = st.text_input(
                "Search query",
                value="turkish stores in toronto Canada",
                help="Try: coffee shops in Seattle",
            )
            total = st.number_input(
                "Target leads",
                min_value=1,
                max_value=500,
                value=25,
                step=1,
            )
            output_name = st.text_input("Output file name", value="results.csv")
            append = st.checkbox("Append to existing file", value=False)

            st.markdown("### Data capture")
            save_everything = st.checkbox(
                "Save everything (all listings + unfiltered emails)",
                value=False,
                help="Overrides other capture settings to keep all listings and raw emails.",
            )
            include_without_email = st.checkbox(
                "Save listings without emails",
                value=False,
                disabled=save_everything,
            )
            extract_emails = st.checkbox(
                "Extract emails from websites",
                value=True,
                disabled=save_everything,
            )
            email_filter_label = st.selectbox(
                "Email filtering",
                ["Strict (recommended)", "Balanced", "None (save everything)"],
                index=0,
                disabled=save_everything,
            )

            st.markdown("### Browser")
            headless = st.checkbox("Run browser headless", value=True)
            max_scroll_attempts = st.slider(
                "Max scroll attempts",
                min_value=5,
                max_value=40,
                value=20,
            )
            unlimited_scan = st.checkbox(
                "Scan until target emails found (no max listings)",
                value=True,
            )
            default_scan_limit = min(max(int(total) * 25, 50), 2000)
            max_listings = st.number_input(
                "Max listings to scan",
                min_value=total,
                max_value=2000,
                value=default_scan_limit,
                step=10,
                help="Increase this if emails are rare.",
                disabled=unlimited_scan,
            )

            st.markdown("### Storage")
            results_dir_input = st.text_input(
                "Results folder",
                value=DEFAULT_RESULTS_DIR_NAME,
                help="Relative paths are resolved against the app folder.",
            )
            show_output_path = st.checkbox("Show output path", value=True)
            st.markdown("### Deduplication")
            dedup_enabled = st.checkbox("Enable deduplication", value=True)
            dedup_db_input = st.text_input(
                "Dedup DB filename",
                value="dedup.sqlite",
                help="Stored inside Results folder unless an absolute path is used.",
                disabled=not dedup_enabled,
            )

            submitted = st.form_submit_button("Start scrape", type="primary")

        results_dir = normalize_results_dir(results_dir_input)
        output_name = normalize_output_name(output_name)
        if os.path.isabs(output_name):
            output_path = output_name
        else:
            output_path = os.path.join(results_dir, output_name)

        if dedup_enabled:
            if os.path.isabs(dedup_db_input):
                dedup_db_path = dedup_db_input
            else:
                dedup_db_path = os.path.join(results_dir, dedup_db_input)
        else:
            dedup_db_path = None

        if save_everything:
            include_without_email = True
            extract_emails = True
            email_filter_mode = "none"
        else:
            email_filter_mode = {
                "Strict (recommended)": "strict",
                "Balanced": "balanced",
                "None (save everything)": "none",
            }.get(email_filter_label, "strict")

        st.markdown("### Output path")
        if show_output_path:
            st.code(output_path)
        else:
            st.caption("Output path hidden.")
        if os.path.exists(output_path) and not append:
            st.warning("Output file exists and will be overwritten.")
        if not extract_emails and not include_without_email:
            st.warning("Enable email extraction or allow listings without emails.")

    tabs = st.tabs(["Live run", "Results", "Data preview"])

    with tabs[0]:
        st.markdown("<div class=\"panel\">", unsafe_allow_html=True)
        st.markdown("<div class=\"section-title\">Live run</div>", unsafe_allow_html=True)
        st.markdown(
            "<div class=\"section-subtitle\">"
            "Track progress, throughput, and scraper signals in real time."
            "</div>",
            unsafe_allow_html=True,
        )

        progress_bar = st.progress(0)
        status_line = st.empty()
        listing_status = st.empty()

        col1, col2, col3, col4 = st.columns(4)
        processed_placeholder = col1.empty()
        found_placeholder = col2.empty()
        emails_placeholder = col3.empty()
        failed_placeholder = col4.empty()

        processed_placeholder.metric("Listings processed", 0)
        found_placeholder.metric("Leads saved", 0)
        emails_placeholder.metric("Emails found", 0)
        failed_placeholder.metric("Failed listings", 0)

        stat_col1, stat_col2, stat_col3 = st.columns(3)
        websites_placeholder = stat_col1.empty()
        duplicates_placeholder = stat_col2.empty()
        listings_total_placeholder = stat_col3.empty()
        websites_placeholder.metric("Websites visited", 0)
        duplicates_placeholder.metric("Duplicates skipped", 0)
        listings_total_placeholder.metric("Listings available", 0)

        log_box = st.empty()
        st.markdown("</div>", unsafe_allow_html=True)

    if submitted:
        if not extract_emails and not include_without_email:
            status_line.write("Fix the data capture options before running.")
            st.stop()

        os.makedirs(results_dir, exist_ok=True)
        log_lines = []
        start_ts = time.time()

        def progress_callback(payload: dict) -> None:
            processed = int(payload.get("processed", 0))
            found = int(payload.get("found", 0))
            target = int(payload.get("target", total)) or 1
            message = payload.get("message", "")
            listing_index = payload.get("listing_index")
            listings_total = payload.get("listings_total")
            websites_visited = int(payload.get("websites_visited", 0))
            current_found = payload.get("current_found")
            duplicates_skipped = int(payload.get("duplicates_skipped", 0))

            progress_bar.progress(min(found / target, 1.0))
            if message:
                status_line.write(message)
                log_lines.append(message)
            if listing_index and listings_total:
                listing_status.write(f"Listing {listing_index} of {listings_total}")
            elif listings_total:
                listing_status.write(f"Listings available: {listings_total}")

            processed_placeholder.metric("Listings processed", processed)
            found_placeholder.metric("Leads saved", found)
            emails_placeholder.metric("Emails found", int(payload.get("emails_found", 0)))
            failed_placeholder.metric("Failed listings", int(payload.get("failed", 0)))
            websites_placeholder.metric("Websites visited", websites_visited)
            duplicates_placeholder.metric("Duplicates skipped", duplicates_skipped)
            if current_found is not None:
                listings_total_placeholder.metric("Currently found", int(current_found))
            else:
                listings_total_placeholder.metric("Listings available", int(listings_total or 0))

            if log_lines:
                log_box.code("\n".join(log_lines[-12:]), language="text")

        places, stats = scrape_places(
            search_for,
            int(total),
            include_without_email=include_without_email,
            extract_emails=extract_emails,
            email_filter_mode=email_filter_mode,
            headless=headless,
            max_scroll_attempts=max_scroll_attempts,
            max_listings=None if unlimited_scan else int(max_listings),
            dedup_enabled=dedup_enabled,
            dedup_db_path=dedup_db_path,
            show_tqdm=False,
            progress_callback=progress_callback,
        )

        save_places_to_csv(places, output_path, append=append)
        generate_report(stats, output_path)

        duration = time.time() - start_ts
        progress_bar.progress(1.0)
        status_line.write(f"Scrape complete in {duration:.1f}s")

        st.session_state["last_output_path"] = output_path
        st.session_state["last_report_path"] = output_path.replace(".csv", "_report.txt")
        st.session_state["last_stats"] = stats
        st.session_state["last_duration"] = duration
        if os.path.exists(output_path):
            df, warning = load_csv_safe(output_path)
            st.session_state["last_output_df"] = df
            st.session_state["last_output_warning"] = warning

    with tabs[1]:
        if st.session_state.get("last_stats"):
            stats = st.session_state["last_stats"]
            success_rate = (
                stats.successful_scrapes / stats.total_searched * 100
                if stats.total_searched
                else 0
            )
            email_rate = (
                stats.emails_found / stats.successful_scrapes * 100
                if stats.successful_scrapes
                else 0
            )
            social_rate = (
                stats.social_media_found / stats.successful_scrapes * 100
                if stats.successful_scrapes
                else 0
            )
            total_time = st.session_state.get("last_duration")
            if total_time is None:
                total_time = stats.average_time_per_business * stats.total_searched

            st.markdown("<div class=\"panel\">", unsafe_allow_html=True)
            st.markdown("<div class=\"section-title\">Results snapshot</div>", unsafe_allow_html=True)
            st.markdown(
                "<div class=\"section-subtitle\">"
                "Key metrics and overall performance for the latest run."
                "</div>",
                unsafe_allow_html=True,
            )
            res_col1, res_col2, res_col3, res_col4 = st.columns(4)
            res_col1.metric("Leads saved", stats.successful_scrapes)
            res_col2.metric("Listings processed", stats.total_searched)
            res_col3.metric("Emails found", stats.emails_found)
            res_col4.metric("Success rate", f"{success_rate:.1f}%")

            st.caption(
                f"Email success rate: {email_rate:.1f}% | "
                f"Social profiles found: {stats.social_media_found}"
            )
            st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("<div style=\"height: 1rem;\"></div>", unsafe_allow_html=True)
            st.markdown("<div class=\"panel\">", unsafe_allow_html=True)
            st.markdown("<div class=\"section-title\">Analytics</div>", unsafe_allow_html=True)
            st.markdown(
                "<div class=\"section-subtitle\">"
                "Quality signals across email and social enrichment."
                "</div>",
                unsafe_allow_html=True,
            )
            ana_col1, ana_col2, ana_col3, ana_col4 = st.columns(4)
            ana_col1.metric("Email rate", f"{email_rate:.1f}%")
            ana_col2.metric("Social rate", f"{social_rate:.1f}%")
            ana_col3.metric("Avg time / business", f"{stats.average_time_per_business:.1f}s")
            ana_col4.metric("Total runtime", f"{total_time:.1f}s")

            ana_col5, ana_col6, ana_col7, ana_col8 = st.columns(4)
            ana_col5.metric("Websites visited", stats.websites_visited)
            ana_col6.metric("Duplicates skipped", stats.duplicates_skipped)
            ana_col7.metric("Target leads", stats.target_leads)
            ana_col8.metric("Failed listings", stats.failed_scrapes)

            import pandas as pd
            rate_df = pd.DataFrame(
                {
                    "Metric": ["Success", "Email", "Social"],
                    "Percent": [success_rate, email_rate, social_rate],
                }
            ).set_index("Metric")
            st.bar_chart(rate_df, height=240)
            st.markdown("</div>", unsafe_allow_html=True)

            output_path = st.session_state.get("last_output_path", "")
            report_path = st.session_state.get("last_report_path", "")

            if output_path:
                st.markdown("<div class=\"panel\">", unsafe_allow_html=True)
                st.markdown("<div class=\"section-title\">Output files</div>", unsafe_allow_html=True)
                st.code(output_path)
                if report_path:
                    st.code(report_path)

                if output_path and os.path.exists(output_path):
                    with open(output_path, "rb") as file_handle:
                        st.download_button(
                            "Download CSV",
                            data=file_handle,
                            file_name=os.path.basename(output_path),
                            mime="text/csv",
                        )

                if report_path and os.path.exists(report_path):
                    with open(report_path, "rb") as file_handle:
                        st.download_button(
                            "Download report",
                            data=file_handle,
                            file_name=os.path.basename(report_path),
                            mime="text/plain",
                        )
                st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.markdown(
                "<div class=\"panel\">"
                "<div class=\"section-title\">Results</div>"
                "<div class=\"section-subtitle\">Run a scrape to populate results.</div>"
                "</div>",
                unsafe_allow_html=True,
            )

    with tabs[2]:
        output_df = st.session_state.get("last_output_df")
        if output_df is not None and not output_df.empty:
            st.markdown("<div class=\"panel\">", unsafe_allow_html=True)
            st.markdown("<div class=\"section-title\">Data preview</div>", unsafe_allow_html=True)
            st.markdown(
                "<div class=\"section-subtitle\">"
                "Review the top rows before exporting."
                "</div>",
                unsafe_allow_html=True,
            )
            preview_warning = st.session_state.get("last_output_warning")
            if preview_warning:
                st.warning(preview_warning)
            st.dataframe(output_df.head(200), use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.markdown(
                "<div class=\"panel\">"
                "<div class=\"section-title\">Data preview</div>"
                "<div class=\"section-subtitle\">No data loaded yet.</div>"
                "</div>",
                unsafe_allow_html=True,
            )


if __name__ == "__main__":
    main()
