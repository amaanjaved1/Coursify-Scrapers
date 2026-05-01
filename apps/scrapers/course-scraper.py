import os
import re
from typing import Optional
from supabase import create_client, Client
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_log_codes() -> set[str]:
    raw = os.getenv("COURSE_SCRAPER_LOG_CODES", "")
    codes = {part.strip() for part in raw.split(",") if part.strip()}
    return codes


def _format_log_value(value, full_text: bool, truncate_len: int = 400) -> str:
    if value is None:
        return "None"
    if isinstance(value, list):
        return f"list(len={len(value)})"
    text = str(value)
    if full_text or len(text) <= truncate_len:
        return text
    return f"{text[:truncate_len]}... (len={len(text)})"


def _should_log_course(course_code: str, log_rows: bool, log_codes: set[str]) -> bool:
    if log_rows:
        return True
    return bool(log_codes) and course_code in log_codes


def _log_course_row(prefix: str, row_data: dict, full_text: bool) -> None:
    print(f"{prefix} {row_data.get('course_code')} | {row_data.get('course_name')}")
    print(f"  course_units: {_format_log_value(row_data.get('course_units'), full_text)}")
    print(
        f"  course_description: {_format_log_value(row_data.get('course_description'), full_text)}"
    )
    print(
        f"  course_requirements: {_format_log_value(row_data.get('course_requirements'), full_text)}"
    )
    print(f"  learning_hours: {_format_log_value(row_data.get('learning_hours'), full_text)}")
    print(
        f"  course_equivalencies: {_format_log_value(row_data.get('course_equivalencies'), full_text)}"
    )
    print(
        f"  offering_faculty: {_format_log_value(row_data.get('offering_faculty'), full_text)}"
    )
    outcomes = row_data.get("course_learning_outcomes") or []
    print(f"  course_learning_outcomes_count: {len(outcomes)}")


def _fix_sentence_spacing(text: str) -> str:
    """
    Insert a space after . ! ? when the next character is a letter and HTML/text
    extraction glued sentences (e.g. 'hello.there' -> 'hello. there').
    Skips typical decimals like 3.00 (digit before the period).
    """
    if not text:
        return text
    s = text
    # Word/close-bracket + period + lowercase letter
    s = re.sub(r"(?<=[a-zA-Z\)\]])\.(?=[a-z])", ". ", s)
    # Lowercase + period + uppercase (e.g. 'experience.NOTE', 'Dr.Smith' -> 'Dr. Smith')
    s = re.sub(r"(?<=[a-z])\.(?=[A-Z])", ". ", s)
    s = re.sub(r"(?<=[a-zA-Z\)\]])\!(?=[A-Za-z])", "! ", s)
    s = re.sub(r"(?<=[a-zA-Z\)\]])\?(?=[A-Za-z])", "? ", s)
    return s


def _normalize_whitespace(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    s = " ".join(text.split())
    if not s:
        return None
    s = _fix_sentence_spacing(s)
    return s


def _strip_leading_label(text: Optional[str], *labels: str) -> Optional[str]:
    """Remove the first matching leading label (e.g. 'Units: ', 'Requirements: ')."""
    if not text:
        return None
    s = text.strip()
    for label in labels:
        if s.lower().startswith(label.lower()):
            return _normalize_whitespace(s[len(label) :])
    return _normalize_whitespace(s)


def _courseblock_text_lines(course) -> list[str]:
    """Non-empty lines from a .courseblock for fallback parsing."""
    raw = course.get_text(separator="\n")
    lines = []
    for ln in raw.split("\n"):
        s = " ".join(ln.split()).strip()
        if s:
            lines.append(s)
    return lines


def _fallback_units_from_lines(lines: list[str]) -> Optional[str]:
    for line in lines:
        if line.lower().startswith("units:"):
            return _strip_leading_label(line, "Units:", "Units :")
        m = re.search(r"\bUnits:\s*(\S+)", line, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def _fallback_line_after_prefix(lines: list[str], prefix: str) -> Optional[str]:
    for line in lines:
        if line.startswith(prefix):
            return _normalize_whitespace(line[len(prefix) :].strip())
    return None


def _fallback_description_from_lines(lines: list[str], course_code: str) -> Optional[str]:
    """First narrative block after the title line until a structured field line."""
    struct_prefixes = (
        "Learning Hours:",
        "Requirements:",
        "Offering Faculty:",
        "Course Equivalencies:",
        "Course Learning Outcomes:",
    )
    start = 0
    if lines and course_code in lines[0]:
        start = 1
    parts: list[str] = []
    for i in range(start, len(lines)):
        line = lines[i]
        if any(line.startswith(p) for p in struct_prefixes):
            break
        parts.append(line)
    joined = " ".join(parts).strip()
    return _normalize_whitespace(joined) if joined else None

def create_supabase_client():
    """
    Create a Supabase client using environment variables for URL and key.
    Prefers SUPABASE_SERVICE_ROLE_KEY when set (bypasses RLS; use in CI/backend).
    """
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(SUPABASE_URL, key)
    return supabase


def fetch_page(page, url, wait_selector=".courseblock"):
    """
    Navigate to a URL with Playwright and return a BeautifulSoup object.
    Waits for the WAF challenge to resolve and the content to render.
    Falls back to networkidle if the expected selector never appears.
    """
    page.goto(url, timeout=60000)
    if wait_selector:
        try:
            page.wait_for_selector(wait_selector, timeout=30000)
        except Exception:
            page.wait_for_load_state("networkidle", timeout=15000)
    else:
        page.wait_for_load_state("networkidle", timeout=30000)
    return BeautifulSoup(page.content(), "html.parser")


def extract_courses_from_soup(soup):
    """
    Extract course data from a BeautifulSoup object containing .courseblock elements.
    Returns a list of dicts.
    """
    rows = []
    courses = soup.find_all("div", class_="courseblock")
    for course in courses:
        code_el = course.find("span", class_="detail-code")
        name_el = course.find("span", class_="detail-title")
        if not code_el or not name_el:
            continue

        course_code = code_el.get_text(strip=True)
        course_name = name_el.get_text(strip=True)
        lines = _courseblock_text_lines(course)

        units_el = course.find("span", class_="detail-hours_html")
        if units_el:
            course_units = _strip_leading_label(
                units_el.get_text(strip=True), "Units:", "Units :"
            )
        else:
            course_units = _fallback_units_from_lines(lines)

        desc_el = course.find("div", class_="courseblockextra")
        if desc_el:
            course_description = _normalize_whitespace(
                desc_el.get_text(separator=" ", strip=True)
            )
        else:
            course_description = _fallback_description_from_lines(lines, course_code)

        req_el = course.find("span", class_="detail-requirements")
        if req_el:
            course_requirements = _strip_leading_label(
                req_el.get_text(separator=" ", strip=True),
                "Requirements:",
                "Requirements :",
            )
        else:
            course_requirements = _fallback_line_after_prefix(lines, "Requirements:")

        lh_el = course.find("span", class_="detail-learning_hours")
        if lh_el:
            learning_hours = _strip_leading_label(
                lh_el.get_text(separator=" ", strip=True),
                "Learning Hours:",
                "Learning Hours :",
            )
        else:
            learning_hours = _fallback_line_after_prefix(lines, "Learning Hours:")

        eq_el = course.find("span", class_="detail-course_equivalencies")
        if eq_el:
            course_equivalencies = _strip_leading_label(
                eq_el.get_text(separator=" ", strip=True),
                "Course Equivalencies:",
                "Course Equivalencies :",
            )
        else:
            course_equivalencies = _fallback_line_after_prefix(
                lines, "Course Equivalencies:"
            )

        fac_el = course.find("span", class_="detail-offering_faculty")
        if fac_el:
            offering_faculty = _strip_leading_label(
                fac_el.get_text(separator=" ", strip=True),
                "Offering Faculty:",
                "Offering Faculty :",
            )
        else:
            offering_faculty = _fallback_line_after_prefix(lines, "Offering Faculty:")

        learning_outcomes = []
        outcomes_section = course.find("span", class_="detail-cim_los")
        if outcomes_section:
            for li in outcomes_section.find_all("li"):
                lo = _normalize_whitespace(li.get_text(separator=" ", strip=True))
                if lo:
                    learning_outcomes.append(lo)

        rows.append({
            "course_code": course_code,
            "course_name": course_name,
            "course_units": course_units,
            "course_description": course_description,
            "course_requirements": course_requirements,
            "learning_hours": learning_hours,
            "course_equivalencies": course_equivalencies,
            "offering_faculty": offering_faculty,
            "course_learning_outcomes": learning_outcomes,
        })
    return rows


ACADEMIC_CALENDAR_ROOT = "https://www.queensu.ca/academic-calendar/"

# Faculties whose courses are known to live on dedicated pages. The scraper
# always hits these (so we never regress), then runs an auto-discovery pass
# against the academic calendar root to surface any faculties not in this list.
KNOWN_FACULTY_PAGES: list[dict] = [
    {"faculty": "Arts & Science", "url": "https://www.queensu.ca/academic-calendar/arts-science/course-descriptions/", "mode": "arts-science"},
    {"faculty": "Education", "url": "https://www.queensu.ca/academic-calendar/education/course-descriptions/", "mode": "single"},
    {"faculty": "Health Sciences", "url": "https://www.queensu.ca/academic-calendar/health-sciences/bhsc/courses-instruction/", "mode": "single"},
    {"faculty": "Nursing", "url": "https://www.queensu.ca/academic-calendar/nursing/bachelor-nursing-science-course-descriptions/", "mode": "single"},
    {"faculty": "Engineering - APSC", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/apsc/", "mode": "single"},
    {"faculty": "Engineering - CHEE", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/chee/", "mode": "single"},
    {"faculty": "Engineering - CIVL", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/civl/", "mode": "single"},
    {"faculty": "Engineering - CMPE", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/cmpe/", "mode": "single"},
    {"faculty": "Engineering - ELEC", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/elec/", "mode": "single"},
    {"faculty": "Engineering - ENCH", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/ench/", "mode": "single"},
    {"faculty": "Engineering - ENPH", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/enph/", "mode": "single"},
    {"faculty": "Engineering - GEOE", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/geoe/", "mode": "single"},
    {"faculty": "Engineering - MTHE", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/mthe/", "mode": "single"},
    {"faculty": "Engineering - MECH", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/mech/", "mode": "single"},
    {"faculty": "Engineering - MREN", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/mren/", "mode": "single"},
    {"faculty": "Engineering - MINE", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/mine/", "mode": "single"},
    {"faculty": "Engineering - MNTC", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/mntc/", "mode": "single"},
    {"faculty": "Engineering - SOFT", "url": "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/soft/", "mode": "single"},
    {"faculty": "Commerce", "url": "https://www.queensu.ca/academic-calendar/business/bachelor-commerce/courses-of-instruction/by20number/#onezerozeroleveltext", "mode": "single"},
    {"faculty": "Law", "url": "https://www.queensu.ca/academic-calendar/law/courses-instruction/", "mode": "single"},
]

# Slugs that are not faculties / not expected to expose course pages — skip
# during auto-discovery to avoid wasted requests.
DISCOVERY_SKIP_SLUGS = {
    "", "search", "policies", "general-information", "academic-regulations",
    "glossary", "about", "contact", "privacy", "accessibility", "calendar",
    "student-services", "admissions", "fees-and-financial-assistance",
    "important-dates-and-deadlines",
}

COURSE_PATH_HINTS = ("course-descriptions", "courses-instruction", "courses-of-instruction")


def _abs_url(href: str) -> Optional[str]:
    if not href:
        return None
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://www.queensu.ca" + href
    return "https://www.queensu.ca/" + href


def _faculty_slug_from_url(url: str) -> Optional[str]:
    m = re.match(r"https?://www\.queensu\.ca/academic-calendar/([^/?#]+)/?", url or "")
    return m.group(1) if m else None


def discover_extra_faculty_pages(page, known_slugs: set[str]) -> list[dict]:
    """
    Crawl the academic-calendar root and surface any faculty slugs not present
    in `known_slugs`, returning their course-listing pages when found.
    Best-effort: failures here must not break the scrape; we just log and skip.
    """
    extras: list[dict] = []
    try:
        soup = fetch_page(page, ACADEMIC_CALENDAR_ROOT, wait_selector=None)
    except Exception as exc:
        print(f"  Auto-discovery: failed to load calendar root ({exc})")
        return extras

    candidate_slugs: set[str] = set()
    for a in soup.select('a[href*="/academic-calendar/"]'):
        href = a.get("href") or ""
        full = _abs_url(href) or ""
        slug = _faculty_slug_from_url(full)
        if not slug or slug in DISCOVERY_SKIP_SLUGS or slug in known_slugs:
            continue
        candidate_slugs.add(slug)

    for slug in sorted(candidate_slugs):
        index_url = f"{ACADEMIC_CALENDAR_ROOT}{slug}/"
        try:
            faculty_soup = fetch_page(page, index_url, wait_selector=None)
        except Exception as exc:
            print(f"  Auto-discovery: skip {slug} ({exc})")
            continue

        # Look for any link on the faculty index whose path contains one of the
        # known course-listing path hints.
        seen_urls: set[str] = set()
        for a in faculty_soup.select("a[href]"):
            href = a.get("href") or ""
            if not any(hint in href for hint in COURSE_PATH_HINTS):
                continue
            full = _abs_url(href)
            if not full or f"/academic-calendar/{slug}/" not in full:
                continue
            full = full.split("#", 1)[0].rstrip("/") + "/"
            if full in seen_urls:
                continue
            seen_urls.add(full)
            extras.append({
                "faculty": slug.replace("-", " ").title(),
                "url": full,
                "mode": "single",
            })

    if extras:
        print(f"  Auto-discovery: found {len(extras)} additional course page(s)")
        for entry in extras:
            print(f"    + {entry['faculty']}: {entry['url']}")
    else:
        print("  Auto-discovery: no new faculty course pages found")
    return extras


def _scrape_arts_science(page, base_url: str) -> list[dict]:
    """Arts & Science needs department-level link discovery before scraping."""
    rows: list[dict] = []
    soup = fetch_page(page, base_url, wait_selector='a[href*="arts-science/course-descriptions/"]')

    container = soup.find("ul", {"id": "/arts-science/course-descriptions/"})
    if container is not None:
        dept_links = container.find_all("a")
    else:
        def _is_dept_href(h):
            if not h or "arts-science/course-descriptions/" not in h or "crse-mode" in h:
                return False
            parts = h.replace("https://www.queensu.ca", "").strip("/").split("/")
            try:
                i = parts.index("course-descriptions")
                return len(parts) > i + 1 and (len(parts) == i + 2 or (len(parts) == i + 3 and parts[-1] == ""))
            except ValueError:
                return False
        dept_links = [a for a in soup.select('a[href*="arts-science/course-descriptions/"]') if _is_dept_href(a.get("href"))]

    for dept_link in dept_links:
        dept_url = _abs_url(dept_link.get("href"))
        dept_name = dept_link.get_text(strip=True)
        if not dept_url:
            continue
        dept_soup = fetch_page(page, dept_url)
        dept_rows = extract_courses_from_soup(dept_soup)
        rows.extend(dept_rows)
        print(f"  {dept_name}: {len(dept_rows)} courses")
    return rows


def scrape_all_course():
    """
    Scrape course data from Queen's University website and store it in Supabase.
    Iterates the known faculty pages, then runs auto-discovery to catch any
    faculties not in the hardcoded list.
    """
    all_rows: list[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        known_slugs = {
            slug for slug in (_faculty_slug_from_url(entry["url"]) for entry in KNOWN_FACULTY_PAGES)
            if slug
        }
        extras = discover_extra_faculty_pages(page, known_slugs)
        targets = list(KNOWN_FACULTY_PAGES) + extras

        for entry in targets:
            faculty = entry["faculty"]
            url = entry["url"]
            mode = entry.get("mode", "single")
            print(f"Scraping {faculty} courses... ({url})")
            try:
                if mode == "arts-science":
                    rows = _scrape_arts_science(page, url)
                else:
                    soup = fetch_page(page, url)
                    rows = extract_courses_from_soup(soup)
            except Exception as exc:
                print(f"  ⚠️  Failed to scrape {faculty}: {exc}")
                continue
            all_rows.extend(rows)
            print(f"✔ {faculty}: {len(rows)} courses")

        browser.close()

    course_data = pd.DataFrame(all_rows)

    # Drop duplicates
    course_data.drop_duplicates(subset=["course_code"], inplace=True)
    course_data.reset_index(drop=True, inplace=True)

    # Clean the dataframe
    course_data.replace({np.nan: None, float("inf"): None, float("-inf"): None}, inplace=True)

    print(f"Total number of courses scraped: {len(course_data)}")

    # Always-on summary so CI logs show whether key fields are being captured.
    summary_fields = [
        "course_units",
        "course_description",
        "course_requirements",
        "learning_hours",
        "course_equivalencies",
        "offering_faculty",
    ]
    print("Field coverage summary:")
    for field in summary_fields:
        non_null = int(course_data[field].notna().sum()) if field in course_data else 0
        null_count = len(course_data) - non_null
        print(f"  {field}: non_null={non_null} null={null_count}")
    if "course_learning_outcomes" in course_data:
        lo_counts = course_data["course_learning_outcomes"].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )
        print(
            "  course_learning_outcomes: "
            f"rows_with_any={int((lo_counts > 0).sum())} "
            f"max_per_course={int(lo_counts.max() if len(lo_counts) else 0)}"
        )

    log_rows = _env_flag("COURSE_SCRAPER_LOG_ROWS", False)
    log_codes = _parse_log_codes()
    full_text = _env_flag("COURSE_SCRAPER_LOG_FULL_TEXT", False)
    if log_rows or log_codes:
        print(
            "Detailed scrape logging enabled "
            f"(log_rows={log_rows}, codes={len(log_codes)}, full_text={full_text})"
        )
        for row_data in course_data.to_dict("records"):
            if _should_log_course(row_data.get("course_code", ""), log_rows, log_codes):
                _log_course_row("[SCRAPED]", row_data, full_text)

    return course_data

def upsert_course_data_to_supabase(supabase, course_data, batch_size=50):
    """
    Upsert course data into Supabase, updating if already exists.
    Preserve average_gpa and average_enrollment if course already exists.
    """

    existing_courses_response = supabase.table("courses").select("course_code, average_gpa, average_enrollment").execute()

    existing_courses = {
        course["course_code"]: {
            "average_gpa": course["average_gpa"],
            "average_enrollment": course["average_enrollment"]
        }
        for course in existing_courses_response.data
    }

    upsert_payload = []
    log_rows = _env_flag("COURSE_SCRAPER_LOG_ROWS", False)
    log_codes = _parse_log_codes()
    full_text = _env_flag("COURSE_SCRAPER_LOG_FULL_TEXT", False)
    log_upsert = _env_flag("COURSE_SCRAPER_LOG_UPSERT", False)
    total_rows = len(course_data)
    total_batches = (total_rows + batch_size - 1) // batch_size if total_rows else 0
    batch_number = 0

    for index, row in course_data.iterrows():
        course_code = row["course_code"]

        if course_code in existing_courses:
            avg_gpa = existing_courses[course_code]["average_gpa"]
            avg_enroll = existing_courses[course_code]["average_enrollment"]
        else:
            avg_gpa = None
            avg_enroll = None

        upsert_payload.append({
            "course_code": course_code,
            "course_name": row["course_name"],
            "course_description": row["course_description"],
            "offering_faculty": row["offering_faculty"],
            "learning_hours": row["learning_hours"],
            "course_learning_outcomes": row["course_learning_outcomes"],
            "course_requirements": row["course_requirements"],
            "course_equivalencies": row["course_equivalencies"],
            "course_units": row["course_units"],
            "average_gpa": avg_gpa,
            "average_enrollment": avg_enroll,
        })

        # If batch size reached or last row, send to Supabase
        if len(upsert_payload) == batch_size or index == len(course_data) - 1:
            batch_number += 1
            if log_upsert:
                codes = [item["course_code"] for item in upsert_payload]
                print(
                    f"Upsert batch {batch_number}/{total_batches}: "
                    f"{len(upsert_payload)} courses | codes={', '.join(codes)}"
                )
            if log_rows or log_codes:
                for item in upsert_payload:
                    if _should_log_course(item.get("course_code", ""), log_rows, log_codes):
                        _log_course_row("[UPSERT]", item, full_text)
            try:
                supabase.table("courses").upsert(upsert_payload, on_conflict=["course_code"]).execute()
            except Exception as exc:
                failed_codes = ", ".join(item["course_code"] for item in upsert_payload)
                print(
                    f"❌ Upsert failed for batch {batch_number}/{total_batches} "
                    f"(codes: {failed_codes})"
                )
                raise RuntimeError(
                    f"Supabase upsert failed for codes: {failed_codes}"
                ) from exc
            print(f"✅ Upserted {len(upsert_payload)} courses (batch {batch_number}/{total_batches})")
            upsert_payload.clear()

    print("✔ Successfully batch upserted all course data into Supabase!")


if __name__ == "__main__":
    # Create Supabase client
    supabase = create_supabase_client()

    # Scrape course data
    course_data = scrape_all_course()

    # Check for new courses and add them to Supabase
    upsert_course_data_to_supabase(supabase, course_data)

    # Print success message
    print("✔ Periodic course data check and update completed successfully!")
