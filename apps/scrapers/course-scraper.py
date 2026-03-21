import os
from supabase import create_client, Client
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

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
        units_el = course.find("span", class_="detail-hours_html")
        if not code_el or not name_el or not units_el:
            continue

        learning_outcomes = []
        outcomes_section = course.find("span", class_="detail-cim_los")
        if outcomes_section:
            for li in outcomes_section.find_all("li"):
                learning_outcomes.append(li.get_text(strip=True))

        rows.append({
            "course_code": code_el.get_text(strip=True),
            "course_name": name_el.get_text(strip=True),
            "course_units": units_el.get_text(strip=True).replace("Units: ", ""),
            "course_description": course.find("div", class_="courseblockextra").get_text(strip=True) if course.find("div", class_="courseblockextra") else None,
            "course_requirements": course.find("span", class_="detail-requirements").get_text(strip=True).replace("Requirements: ", "") if course.find("span", class_="detail-requirements") else None,
            "learning_hours": course.find("span", class_="detail-learning_hours").get_text(strip=True).replace("Learning Hours: ", "") if course.find("span", class_="detail-learning_hours") else None,
            "course_equivalencies": course.find("span", class_="detail-course_equivalencies").get_text(strip=True).replace("Course Equivalencies: ", "") if course.find("span", class_="detail-course_equivalencies") else None,
            "offering_faculty": course.find("span", class_="detail-offering_faculty").get_text(strip=True).replace("Offering Faculty: ", "") if course.find("span", class_="detail-offering_faculty") else None,
            "course_learning_outcomes": learning_outcomes,
        })
    return rows


def scrape_all_course():
    """
    Scrape course data from Queen's University website and store it in Supabase.
    Uses Playwright to render JS-heavy pages.
    """
    art_sci_url = "https://www.queensu.ca/academic-calendar/arts-science/course-descriptions/"
    education_url = "https://www.queensu.ca/academic-calendar/education/course-descriptions/"
    health_sci_url = "https://www.queensu.ca/academic-calendar/health-sciences/bhsc/courses-instruction/"
    nursing_url = "https://www.queensu.ca/academic-calendar/nursing/bachelor-nursing-science-course-descriptions/"
    engineering_urls = [
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/apsc/",
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/chee/",
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/civl/",
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/cmpe/",
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/elec/",
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/ench/",
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/enph/",
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/geoe/",
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/mthe/",
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/mech/",
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/mren/",
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/mine/",
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/mntc/",
        "https://www.queensu.ca/academic-calendar/engineering-applied-sciences/courses-instruction/soft/",
    ]
    commerce_url = "https://www.queensu.ca/academic-calendar/business/bachelor-commerce/courses-of-instruction/by20number/#onezerozeroleveltext"

    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        # Faculty 1: Arts & Science
        print("Scraping Arts & Science courses...")
        soup = fetch_page(page, art_sci_url, wait_selector='a[href*="arts-science/course-descriptions/"]')

        # Find department links
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
            dept_url = dept_link.get("href")
            dept_name = dept_link.get_text(strip=True)
            if not dept_url:
                continue
            if not dept_url.startswith("http"):
                dept_url = "https://www.queensu.ca" + (dept_url if dept_url.startswith("/") else "/" + dept_url)

            dept_soup = fetch_page(page, dept_url)
            rows = extract_courses_from_soup(dept_soup)
            all_rows.extend(rows)
            print(f"  {dept_name}: {len(rows)} courses")

        print(f"✔ Arts & Science: {len(all_rows)} courses total")

        # Faculty 2: Education
        print("Scraping Education courses...")
        soup = fetch_page(page, education_url)
        rows = extract_courses_from_soup(soup)
        all_rows.extend(rows)
        print(f"✔ Education: {len(rows)} courses")

        # Faculty 3: Health Sciences
        print("Scraping Health Sciences courses...")
        soup = fetch_page(page, health_sci_url)
        rows = extract_courses_from_soup(soup)
        all_rows.extend(rows)
        print(f"✔ Health Sciences: {len(rows)} courses")

        # Faculty 4: Nursing
        print("Scraping Nursing courses...")
        soup = fetch_page(page, nursing_url)
        rows = extract_courses_from_soup(soup)
        all_rows.extend(rows)
        print(f"✔ Nursing: {len(rows)} courses")

        # Faculty 5: Engineering
        print("Scraping Engineering courses...")
        eng_total = 0
        for engineering_url in engineering_urls:
            soup = fetch_page(page, engineering_url)
            rows = extract_courses_from_soup(soup)
            all_rows.extend(rows)
            dept = engineering_url.rstrip("/").split("/")[-1].upper()
            print(f"  {dept}: {len(rows)} courses")
            eng_total += len(rows)
        print(f"✔ Engineering: {eng_total} courses total")

        # Faculty 6: Commerce
        print("Scraping Commerce courses...")
        soup = fetch_page(page, commerce_url)
        rows = extract_courses_from_soup(soup)
        all_rows.extend(rows)
        print(f"✔ Commerce: {len(rows)} courses")

        browser.close()

    course_data = pd.DataFrame(all_rows)

    # Drop duplicates
    course_data.drop_duplicates(subset=["course_code"], inplace=True)

    # Clean the dataframe
    course_data.replace({np.nan: None, float("inf"): None, float("-inf"): None}, inplace=True)

    print(f"Total number of courses scraped: {len(course_data)}")

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
            supabase.table("courses").upsert(upsert_payload, on_conflict=["course_code"]).execute()
            print(f"✅ Upserted {len(upsert_payload)} courses")
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
