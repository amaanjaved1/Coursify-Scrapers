import os
from supabase import create_client, Client
import requests
from requests import get
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



def scrape_all_course():
    """
    Scrape course data from Queen's University website and store it in Supabase."""
    headers = { "Accept-Language": "en-US,en;q=0.9,en-GB;q=0.8,en-CA;q=0.7" }
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
    
    # Create a pandas DataFrame to store the scraped data
    columns = [
        "course_code",
        "course_name",
        "course_description",
        "offering_faculty",
        "learning_hours",
        "course_learning_outcomes",
        "course_requirements",
        "course_equivalencies"
    ]
    course_data = pd.DataFrame(columns=columns)
    
    # Faculty 1: Arts & Science
    print("Scraping Arts & Science courses...")
    
    # Step 1: Get the main URL content
    results = requests.get(art_sci_url, headers=headers)
    art_sci_main_url_content = BeautifulSoup(results.content, "html.parser")

    # Step 2: Find the embedded links for the course offerings page for each department within the faculty
    art_sci_main_url_content_container = art_sci_main_url_content.find("ul", {"id": "/arts-science/course-descriptions/"})  # get the container element

    if art_sci_main_url_content_container is not None:
        art_sci_dept_course_pages = art_sci_main_url_content_container.find_all("a")  # get all the links in the container
    else:
        # Fallback: department links are /academic-calendar/arts-science/course-descriptions/<dept>/ (one segment after course-descriptions/)
        def _is_dept_href(h):
            if not h or "arts-science/course-descriptions/" not in h or "crse-mode" in h:
                return False
            parts = h.replace("https://www.queensu.ca", "").strip("/").split("/")
            try:
                i = parts.index("course-descriptions")
                return len(parts) > i + 1 and (len(parts) == i + 2 or (len(parts) == i + 3 and parts[-1] == ""))
            except ValueError:
                return False

        art_sci_dept_course_pages = [a for a in art_sci_main_url_content.select('a[href*="arts-science/course-descriptions/"]') if _is_dept_href(a.get("href"))]

    # Step 3: For each department, go through the courses offered and scrape the data
    for dept_course_page in art_sci_dept_course_pages:

        # Get the URL and name of the department course page
        dept_course_page_url = dept_course_page.get("href")
        dept_course_page_name = dept_course_page.get_text(strip=True)
        _req_url = dept_course_page_url if (dept_course_page_url or "").startswith("http") else "https://www.queensu.ca" + ((dept_course_page_url or "") if (dept_course_page_url or "").startswith("/") else "/" + (dept_course_page_url or ""))

        print(f"Scraping {dept_course_page_name} courses...")

        # Make a request to the department course page
        dept_course_page_results = requests.get(_req_url, headers=headers)
        dept_course_page_content = BeautifulSoup(dept_course_page_results.content, "html.parser")
        
        # Get each course from the department course page
        courses = dept_course_page_content.find_all("div", class_="courseblock")
        for course in courses:
            course_code = course.find("span", class_="detail-code").get_text(strip=True)
            course_name = course.find("span", class_="detail-title").get_text(strip=True)
            course_units = course.find("span", class_="detail-hours_html").get_text(strip=True).replace("Units: ", "")
            course_description = course.find("div", class_="courseblockextra").get_text(strip=True) if course.find("div", class_="courseblockextra") else None
            course_requirements = course.find("span", class_="detail-requirements").get_text(strip=True).replace("Requirements: ", "") if course.find("span", class_="detail-requirements") else None
            course_learning_hours = course.find("span", class_="detail-learning_hours").get_text(strip=True).replace("Learning Hours: ", "") if course.find("span", class_="detail-learning_hours") else None
            course_equivalencies = course.find("span", class_="detail-course_equivalencies").get_text(strip=True).replace("Course Equivalencies: ", "") if course.find("span", class_="detail-course_equivalencies") else None
            offering_faculty = course.find("span", class_="detail-offering_faculty").get_text(strip=True).replace("Offering Faculty: ", "") if course.find("span", class_="detail-offering_faculty") else None

            learning_outcomes = []
            outcomes_section = course.find("span", class_="detail-cim_los")
            if outcomes_section:
                outcomes_list = outcomes_section.find_all("li")
                for outcome in outcomes_list:
                    learning_outcomes.append(outcome.get_text(strip=True))

            # Append the course data to the DataFrame
            course_data = pd.concat([
            course_data,
            pd.DataFrame([{
                "course_code": course_code,
                "course_name": course_name,
                "course_description": course_description,
                "offering_faculty": offering_faculty,
                "learning_hours": course_learning_hours,
                "course_learning_outcomes": learning_outcomes,
                "course_requirements": course_requirements,
                "course_equivalencies": course_equivalencies,
                "course_units": course_units
            }])
            ], ignore_index=True)

    # Print success message
    print("✔ Successfully scraped Arts & Science courses!")

    # Step 4: Repeat the process for other faculties
    
    # Faculty 2: Education
    print("Scraping Education courses...")

    # Step 1: Get the main URL content
    results = requests.get(education_url, headers=headers)
    education_main_url_content = BeautifulSoup(results.content, "html.parser")

    # Step 2: Find all course blocks
    course_blocks = education_main_url_content.find_all("div", class_="courseblock")

    # Step 3: Extract course details
    for course in course_blocks:
        course_code = course.find("span", class_="detail-code").get_text(strip=True)
        course_name = course.find("span", class_="detail-title").get_text(strip=True)
        course_units = course.find("span", class_="detail-hours_html").get_text(strip=True).replace("Units: ", "")
        course_description = course.find("div", class_="courseblockextra").get_text(strip=True) if course.find("div", class_="courseblockextra") else None
        course_requirements = course.find("span", class_="detail-requirements").get_text(strip=True).replace("Requirements: ", "") if course.find("span", class_="detail-requirements") else None
        course_learning_hours = course.find("span", class_="detail-learning_hours").get_text(strip=True).replace("Learning Hours: ", "") if course.find("span", class_="detail-learning_hours") else None
        course_equivalencies = course.find("span", class_="detail-course_equivalencies").get_text(strip=True).replace("Course Equivalencies: ", "") if course.find("span", class_="detail-course_equivalencies") else None
        offering_faculty = course.find("span", class_="detail-offering_faculty").get_text(strip=True).replace("Offering Faculty: ", "") if course.find("span", class_="detail-offering_faculty") else None

        learning_outcomes = []
        outcomes_section = course.find("span", class_="detail-cim_los")
        if outcomes_section:
            outcomes_list = outcomes_section.find_all("li")
            for outcome in outcomes_list:
                learning_outcomes.append(outcome.get_text(strip=True))

        # Append the course data to the DataFrame
        course_data = pd.concat([
            course_data,
            pd.DataFrame([{
                "course_code": course_code,
                "course_name": course_name,
                "course_description": course_description,
                "offering_faculty": offering_faculty,
                "learning_hours": course_learning_hours,
                "course_learning_outcomes": learning_outcomes,
                "course_requirements": course_requirements,
                "course_equivalencies": course_equivalencies,
                "course_units": course_units
            }])
        ], ignore_index=True)

    # Print success message
    print("✔ Successfully scraped Education courses!")

    # Faculty 3: Health Sciences
    print("Scraping Health Sciences courses...")
    results = requests.get(health_sci_url, headers=headers)
    health_sci_main_url_content = BeautifulSoup(results.content, "html.parser")

    # Step 1: Find all course blocks
    course_blocks = health_sci_main_url_content.find_all("div", class_="courseblock")

    # Step 2: Extract course details
    for course in course_blocks:
        course_code = course.find("span", class_="detail-code").get_text(strip=True)
        course_name = course.find("span", class_="detail-title").get_text(strip=True)
        course_units = course.find("span", class_="detail-hours_html").get_text(strip=True).replace("Units: ", "")
        course_description = course.find("div", class_="courseblockextra").get_text(strip=True) if course.find("div", class_="courseblockextra") else None
        course_requirements = course.find("span", class_="detail-requirements").get_text(strip=True).replace("Requirements: ", "") if course.find("span", class_="detail-requirements") else None
        course_learning_hours = course.find("span", class_="detail-learning_hours").get_text(strip=True).replace("Learning Hours: ", "") if course.find("span", class_="detail-learning_hours") else None
        course_equivalencies = course.find("span", class_="detail-course_equivalencies").get_text(strip=True).replace("Course Equivalencies: ", "") if course.find("span", class_="detail-course_equivalencies") else None
        offering_faculty = course.find("span", class_="detail-offering_faculty").get_text(strip=True).replace("Offering Faculty: ", "") if course.find("span", class_="detail-offering_faculty") else None

        learning_outcomes = []
        outcomes_section = course.find("span", class_="detail-cim_los")
        if outcomes_section:
            outcomes_list = outcomes_section.find_all("li")
            for outcome in outcomes_list:
                learning_outcomes.append(outcome.get_text(strip=True))

        # Append the course data to the DataFrame
        course_data = pd.concat([
            course_data,
            pd.DataFrame([{
                "course_code": course_code,
                "course_name": course_name,
                "course_description": course_description,
                "offering_faculty": offering_faculty,
                "learning_hours": course_learning_hours,
                "course_learning_outcomes": learning_outcomes,
                "course_requirements": course_requirements,
                "course_equivalencies": course_equivalencies,
                "course_units": course_units
            }])
        ], ignore_index=True)

    # Print success message
    print("✔ Successfully scraped Health Sciences courses!")

    # Faculty 4: Nursing
    print("Scraping Nursing courses...")
    results = requests.get(nursing_url, headers=headers)
    nursing_main_url_content = BeautifulSoup(results.content, "html.parser")

    # Step 1: Find all course blocks
    course_blocks = nursing_main_url_content.find_all("div", class_="courseblock")

    # Step 2: Extract course details
    for course in course_blocks:
        course_code = course.find("span", class_="detail-code").get_text(strip=True)
        course_name = course.find("span", class_="detail-title").get_text(strip=True)
        course_units = course.find("span", class_="detail-hours_html").get_text(strip=True).replace("Units: ", "")
        course_description = course.find("div", class_="courseblockextra").get_text(strip=True) if course.find("div", class_="courseblockextra") else None
        course_requirements = course.find("span", class_="detail-requirements").get_text(strip=True).replace("Requirements: ", "") if course.find("span", class_="detail-requirements") else None
        course_learning_hours = None  # Not available in the provided structure
        course_equivalencies = course.find("span", class_="detail-course_equivalencies").get_text(strip=True).replace("Course Equivalencies: ", "") if course.find("span", class_="detail-course_equivalencies") else None
        offering_faculty = course.find("span", class_="detail-offering_faculty").get_text(strip=True).replace("Offering Faculty: ", "") if course.find("span", class_="detail-offering_faculty") else None

        learning_outcomes = []
        outcomes_section = course.find("span", class_="detail-cim_los")
        if outcomes_section:
            outcomes_list = outcomes_section.find_all("li")
            for outcome in outcomes_list:
                learning_outcomes.append(outcome.get_text(strip=True))

        # Append the course data to the DataFrame
        course_data = pd.concat([
            course_data,
            pd.DataFrame([{
                "course_code": course_code,
                "course_name": course_name,
                "course_description": course_description,
                "offering_faculty": offering_faculty,
                "learning_hours": course_learning_hours,
                "course_learning_outcomes": learning_outcomes,
                "course_requirements": course_requirements,
                "course_equivalencies": course_equivalencies,
                "course_units": course_units
            }])
        ], ignore_index=True)

    # Print success message
    print("✔ Successfully scraped Nursing courses!")

    # Faculty 5: Engineering
    print("Scraping Engineering courses...")

    for engineering_url in engineering_urls:
        # Make a request to the engineering URL
        results = requests.get(engineering_url, headers=headers)
        engineering_main_url_content = BeautifulSoup(results.content, "html.parser")

        # Find all course blocks
        course_blocks = engineering_main_url_content.find_all("div", class_="courseblock")

        # Extract course details
        for course in course_blocks:
            course_code = course.find("span", class_="detail-code").get_text(strip=True)
            course_name = course.find("span", class_="detail-title").get_text(strip=True)
            course_units = course.find("span", class_="detail-hours_html").get_text(strip=True).replace("Units: ", "")
            course_description = course.find("div", class_="courseblockextra").get_text(strip=True) if course.find("div", class_="courseblockextra") else None
            course_requirements = course.find("span", class_="detail-requirements").get_text(strip=True).replace("Requirements: ", "") if course.find("span", class_="detail-requirements") else None
            course_learning_hours = None  # Not available in the provided structure
            course_equivalencies = course.find("span", class_="detail-course_equivalencies").get_text(strip=True).replace("Course Equivalencies: ", "") if course.find("span", class_="detail-course_equivalencies") else None
            offering_faculty = course.find("span", class_="detail-offering_faculty").get_text(strip=True).replace("Offering Faculty: ", "") if course.find("span", class_="detail-offering_faculty") else None

            learning_outcomes = []
            outcomes_section = course.find("span", class_="detail-cim_los")
            if outcomes_section:
                outcomes_list = outcomes_section.find_all("li")
                for outcome in outcomes_list:
                    learning_outcomes.append(outcome.get_text(strip=True))

            # Append the course data to the DataFrame
            course_data = pd.concat([
                course_data,
                pd.DataFrame([{
                    "course_code": course_code,
                    "course_name": course_name,
                    "course_description": course_description,
                    "offering_faculty": offering_faculty,
                    "learning_hours": course_learning_hours,
                    "course_learning_outcomes": learning_outcomes,
                    "course_requirements": course_requirements,
                    "course_equivalencies": course_equivalencies,
                    "course_units": course_units
                }])
            ], ignore_index=True)

        print(f"✔ Successfully scraped courses from {engineering_url}")

    # Faculty 6: Commerce
    print("Scraping Commerce courses...")

    # Make a request to the Commerce URL
    response = requests.get(commerce_url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all course blocks
    course_blocks = soup.find_all("div", class_="courseblock")

    # Extract course details
    for course in course_blocks:
        course_code = course.find("span", class_="detail-code").get_text(strip=True)
        course_name = course.find("span", class_="detail-title").get_text(strip=True)
        course_units = course.find("span", class_="detail-hours_html").get_text(strip=True).replace("Units: ", "")
        course_description = course.find("div", class_="courseblockextra").get_text(strip=True) if course.find("div", class_="courseblockextra") else None
        course_requirements = course.find("span", class_="detail-requirements").get_text(strip=True).replace("Requirements: ", "") if course.find("span", class_="detail-requirements") else None
        course_equivalencies = course.find("span", class_="detail-course_equivalencies").get_text(strip=True).replace("Course Equivalencies: ", "") if course.find("span", class_="detail-course_equivalencies") else None
        offering_faculty = course.find("span", class_="detail-offering_faculty").get_text(strip=True).replace("Offering Faculty: ", "") if course.find("span", class_="detail-offering_faculty") else None

        learning_outcomes = []
        outcomes_section = course.find("span", class_="detail-cim_los")
        if outcomes_section:
            outcomes_list = outcomes_section.find_all("li")
            for outcome in outcomes_list:
                learning_outcomes.append(outcome.get_text(strip=True))

        # Append the course data to the DataFrame
        course_data = pd.concat([
            course_data,
            pd.DataFrame([{
                "course_code": course_code,
                "course_name": course_name,
                "course_description": course_description,
                "offering_faculty": offering_faculty,
                "learning_hours": None,  # Not available in this structure
                "course_learning_outcomes": learning_outcomes,
                "course_requirements": course_requirements,
                "course_equivalencies": course_equivalencies,
                "course_units": course_units
            }])
        ], ignore_index=True)

    # Print success message
    print("✔ Successfully scraped Commerce courses!")

    # Drop duplicates
    course_data.drop_duplicates(subset=["course_code"], inplace=True)

    # Clean the dataframe
    course_data.replace({np.nan: None, float("inf"): None, float("-inf"): None}, inplace=True)
    
    # Print hte number of rows in the DataFrame
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