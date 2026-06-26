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
        /* Modern Font Stack */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
        
        html, body, [class*="css"] {
            font-family: 'Inter', system-ui, -apple-system, sans-serif;
        }

        /* Main Container Styling */
        .stApp {
            background-color: #0e1117;
            color: #ffffff;
        }

        /* Sidebar Styling */
        section[data-testid="stSidebar"] {
            background-color: #161b22;
        }

        /* Inputs and Select Boxes */
        .stTextInput input, .stSelectbox div[data-baseweb="select"] > div, .stTextArea textarea {
            background-color: #0d1117;
            color: #c9d1d9;
            border: 1px solid #30363d;
            border-radius: 6px;
        }
        
        /* Focus states */
        .stTextInput input:focus, .stTextArea textarea:focus {
            border-color: #58a6ff;
            box-shadow: 0 0 0 1px #58a6ff;
        }

        /* Buttons */
        .stButton button {
            background-color: #238636;
            color: #ffffff;
            border: 1px solid rgba(240,246,252,0.1);
            border-radius: 6px;
            padding: 0.5rem 1rem;
            font-weight: 600;
            transition: all 0.2s ease;
        }
        .stButton button:hover {
            background-color: #2ea043;
            border-color: #8b949e;
        }
        
        /* Metric Cards */
        [data-testid="stMetric"] {
            background-color: #161b22;
            padding: 1rem;
            border-radius: 8px;
            border: 1px solid #30363d;
            box-shadow: 0 1px 3px rgba(0,0,0,0.12);
        }
        [data-testid="stMetricLabel"] {
            color: #8b949e;
            font-size: 0.875rem;
        }
        [data-testid="stMetricValue"] {
            color: #58a6ff;
            font-weight: 700;
        }

        /* Expanders */
        .streamlit-expanderHeader {
            background-color: #161b22;
            border-radius: 6px;
            border: 1px solid #30363d;
        }
        
        /* Headers */
        h1, h2, h3, h4 {
            color: #f0f6fc;
            font-weight: 700;
            letter-spacing: -0.025em;
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
            st.markdown("### Search Parameters")
            
            # Niches
            niche_options = [
                "Restaurants", "Dentists", "Gyms", "Real Estate Agents", 
                "Plumbers", "Electricians", "Cafes", "Hotels",
                "Lawyers", "Accounting Firms"
            ]
            selected_niches = st.multiselect("Select Niches", niche_options, default=None)
            custom_niches_text = st.text_area(
                "Custom Niches (one per line)", 
                height=68, 
                help="Enter custom business types here if not in the list."
            )
            
            # Locations
            location_options = [
                "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", 
                "Phoenix, AZ", "Philadelphia, PA", "San Antonio, TX", "San Diego, CA",
                "Dallas, TX", "San Jose, CA"
            ]
            selected_locations = st.multiselect("Select Locations", location_options, default=None)
            custom_locations_text = st.text_area(
                "Custom Locations (one per line)", 
                height=68, 
                help="Enter custom locations here, e.g., 'Miami, FL'."
            )
           
            total = st.number_input(
                "Target leads (per search)",
                min_value=1,
                max_value=500,
                value=25,
                step=1,
                help="Maximum results to fetch for EACH generated search term."
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
        # Prepare Niches and Locations
        niches = []
        if selected_niches:
            niches.extend(selected_niches)
        if custom_niches_text.strip():
            niches.extend([x.strip() for x in custom_niches_text.split('\n') if x.strip()])
        
        locations = []
        if selected_locations:
            locations.extend(selected_locations)
        if custom_locations_text.strip():
            locations.extend([x.strip() for x in custom_locations_text.split('\n') if x.strip()])
            
        # Ensure unique and cleaned
        niches = sorted(list(set(niches)))
        locations = sorted(list(set(locations)))

        if not niches or not locations:
             status_line.error("Please provide at least one niche and one location.")
             st.stop()

        if not extract_emails and not include_without_email:
            status_line.error("Fix the data capture options before running.")
            st.stop()

        os.makedirs(results_dir, exist_ok=True)
        log_lines = []
        start_ts = time.time()
        
        # Build search Queries
        search_queries = []
        for n in niches:
            for l in locations:
                search_queries.append(f"{n} in {l}")
        
        total_queries = len(search_queries)
        overall_stats = None
        all_places = []
        
        # Initialize progress tracking
        current_query_index = 0
        status_line.write(f"Starting batch of {total_queries} searches...")

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
            
            # Weighted progress: previous completed terms + current phrase progress
            if total_queries > 0:
                base_progress = current_query_index / total_queries
                # Cap the internal progress for this phrase at 0.99 so we rely on term completion to push it
                term_progress = min(found / target, 0.99)
                total_progress = base_progress + (term_progress / total_queries)
                progress_bar.progress(min(total_progress, 1.0))

            if message:
                current_query_text = search_queries[current_query_index] if current_query_index < len(search_queries) else "Done"
                prefix = f"[{current_query_index + 1}/{total_queries}] ({current_query_text}) "
                log_lines.append(prefix + message)
            
            # Only update live status text, don't flood logs
            status_line.write(f"Running query {current_query_index + 1}/{total_queries}: {search_queries[current_query_index] if current_query_index < len(search_queries) else ''} ...")
            
            if listing_index and listings_total:
                listing_status.write(f"Listing {listing_index} of {listings_total}")
            elif listings_total:
                listing_status.write(f"Listings available: {listings_total}")

            # Note: Metrics below show stats for the *current* scrape_places call only if passed from it
            # We would need a better aggregator for global stats live, but for now we show current run activity
            processed_placeholder.metric("Listings processed (current)", processed)
            found_placeholder.metric("Leads saved (current)", found)
            emails_placeholder.metric("Emails (current)", int(payload.get("emails_found", 0)))
            
            if log_lines:
                log_box.code("\n".join(log_lines[-12:]), language="text")

        # Run Loop
        for i, query in enumerate(search_queries):
            current_query_index = i
            log_lines.append(f"--- Starting: {query} ---")
            
            places, stats = scrape_places(
                query,
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
            
            all_places.extend(places)
            
            # Aggregate stats
            if overall_stats is None:
                overall_stats = stats
            else:
                overall_stats.total_searched += stats.total_searched
                overall_stats.successful_scrapes += stats.successful_scrapes
                overall_stats.failed_scrapes += stats.failed_scrapes
                overall_stats.duplicates_skipped += stats.duplicates_skipped
                overall_stats.emails_found += stats.emails_found
                overall_stats.websites_visited += stats.websites_visited
                overall_stats.social_media_found += stats.social_media_found
                overall_stats.target_leads += stats.target_leads # Add expected targets

            # Small pause between queries
            time.sleep(1.5)

        # Finalize
        # Append is handled per-batch by save_places_to_csv? No, we should probably save once at the end or incrementally.
        # Original code saved once. We have 'all_places' now.
        # But 'append' flag logic in loop: if we save once at end, 'append' flag applies to the file on disk.
        
        save_places_to_csv(all_places, output_path, append=append)
        
        # Recalculate average time based on total wall clock for the batch?
        # Or simple average of averages? Averages of averages is bad.
        # Recalc proper average from total items vs total duration
        duration = time.time() - start_ts
        if overall_stats and overall_stats.total_searched > 0:
            overall_stats.average_time_per_business = duration / overall_stats.total_searched
            
        if overall_stats:
            generate_report(overall_stats, output_path)

        progress_bar.progress(1.0)
        status_line.write(f"Batch complete in {duration:.1f}s. Scraped {len(search_queries)} queries.")

        st.session_state["last_output_path"] = output_path
        st.session_state["last_report_path"] = output_path.replace(".csv", "_report.txt")
        st.session_state["last_stats"] = overall_stats
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
