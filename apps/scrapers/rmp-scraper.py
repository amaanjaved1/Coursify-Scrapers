import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import WebDriverException
import time
from textblob import TextBlob
from supabase import create_client, Client
import re
from datetime import datetime
import os

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Referer": "https://www.ratemyprofessors.com/",
    "Accept": "application/json",
}
UNIVERSITY_ID = 1466
UNIVERSITY_NAME = "Queen's University at Kingston"

def create_supabase_client():
    """
    Create a Supabase client using environment variables for URL and key.
    Prefers SUPABASE_SERVICE_ROLE_KEY when set (bypasses RLS; use in CI/backend).
    """
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(SUPABASE_URL, key)
    return supabase

def is_valid_comment(comment):
    """
    Check if the comment is valid based on certain criteria.
    """
    # If the comment is empty or too short, it's invalid
    if not comment or len(comment) < 10:
        return False
    
    return True

def get_all_valid_courses(supabase):
    """
    Get all valid courses from the database.
    """
    # Query the database for all valid courses
    valid_courses = supabase.table("courses").select("course_code").execute().data
    # Extract course codes from the result if the course_code is not 'general_course'
    valid_course_codes = {course["course_code"] for course in valid_courses if course["course_code"] != "general_course"}
    
    return valid_course_codes

def clean_and_map_course_codes(course_codes, valid_courses):
    """
    Refined two-pass system to clean messy scraped course codes.
    """

    # --- Step 1: Build valid dept codes, number codes, and derived clean courses ---
    valid_dept_codes = set()
    valid_num_codes = set()
    derived_valid_courses = set()

    valid_courses_no_space = {course.replace(" ", "").upper(): course for course in valid_courses}

    for raw_code in course_codes:
        cleaned = raw_code.strip().replace(" ", "").upper()

        # Extract prefix and number parts
        prefix_match = re.match(r"^[A-Z]+", cleaned)
        number_parts = re.findall(r"\d+", cleaned)

        if prefix_match:
            prefix = prefix_match.group(0)

            # Check if prefix matches any valid course
            for valid in valid_courses:
                if valid.replace(" ", "").startswith(prefix):
                    valid_dept_codes.add(prefix)
                    break

        for num in number_parts:
            if len(num) >= 3:
                num = num[:3]
                # Try matching this number with known prefixes
                for dept in valid_dept_codes:
                    candidate = f"{dept} {num}"
                    if candidate in valid_courses:
                        valid_num_codes.add(num)
                        derived_valid_courses.add(candidate)

    # --- Step 2: Build mapping ---
    course_mapping = {}

    for raw_code in course_codes:
        matches = []
        cleaned = raw_code.strip().replace(" ", "").upper()

        # Exact match to known valid courses first
        if cleaned in valid_courses_no_space:
            matches.append(valid_courses_no_space[cleaned])

        else:
            prefix_match = re.match(r"^[A-Z]+", cleaned)
            number_parts = re.findall(r"\d+", cleaned)

            if prefix_match and number_parts:
                prefix = prefix_match.group(0)
                suffix = cleaned[len(prefix):]

                # Try to build full courses
                idx = 0
                while idx < len(suffix):
                    num = suffix[idx:idx+3]
                    idx += 3

                    for dept in valid_dept_codes:
                        candidate = f"{dept} {num}"
                        if candidate in derived_valid_courses:
                            matches.append(candidate)

            elif cleaned.isdigit() and len(cleaned) == 3:
                # Just numbers
                num = cleaned
                if num in valid_num_codes:
                    for dept in valid_dept_codes:
                        candidate = f"{dept} {num}"
                        if candidate in derived_valid_courses:
                            matches.append(candidate)
                else:
                    matches = None

            elif cleaned.isalpha():
                # Only letters (ANAT) => ambiguous
                matches = None

            else:
                matches = None

        if len(matches) == 1:
            course_mapping[raw_code] = matches
        else:
            course_mapping[raw_code] = None

    return course_mapping   

def detect_sentiment(text):
    """
    Determine the sentiment of a given text using TextBlob.
    returns a sentiment_score (float between -1 and 1) and a sentiment_label (very positive, positive, neutral, negative, very negative).
    """
    blob = TextBlob(text)
    sentiment_score = blob.sentiment.polarity
    if sentiment_score > 0.5:
        sentiment_label = "very positive"
    elif sentiment_score > 0.2:
        sentiment_label = "positive"
    elif sentiment_score < -0.5:
        sentiment_label = "very negative"
    elif sentiment_score < -0.2:
        sentiment_label = "negative"
    else:
        sentiment_label = "neutral"

    return sentiment_score, sentiment_label

def scrape_professors(supabase, testing=True):
    url = f"https://www.ratemyprofessors.com/search/professors/{UNIVERSITY_ID}?q=*"

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-setuid-sandbox")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--js-flags=--max-old-space-size=512")
    driver = webdriver.Chrome(options=options)

    professors = []
    seen_professor_ids = set()
    previous_count = 0
    
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "TeacherCard__StyledTeacherCard-syjs0d-0"))
        )

        # Dismiss OneTrust cookie banner so it does not intercept "Show More" clicks
        try:
            accept_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
            )
            accept_btn.click()
            time.sleep(1)
        except (TimeoutException, NoSuchElementException, Exception):
            pass

        while True:
            time.sleep(1)  # tiny wait to avoid rapid clicking
            tab_crashed = False

            try:
                # Get professor cards
                professor_cards = driver.find_elements(By.CLASS_NAME, "TeacherCard__StyledTeacherCard-syjs0d-0")
                for card in professor_cards[previous_count:]:
                    try:
                        name = card.find_element(By.CLASS_NAME, "CardName__StyledCardName-sc-1gyrgim-0").text
                        department = card.find_element(By.CLASS_NAME, "CardSchool__Department-sc-19lmz2k-0").text
                        school = card.find_element(By.CLASS_NAME, "CardSchool__School-sc-19lmz2k-1").text
                        rating = card.find_element(By.CLASS_NAME, "CardNumRating__CardNumRatingNumber-sc-17t4b9u-2").text
                        num_ratings = card.find_element(By.CLASS_NAME, "CardNumRating__CardNumRatingCount-sc-17t4b9u-3").text # formatted as "X ratings"
                        num_ratings = int(num_ratings.split()[0].replace(",", ""))  # Convert to integer

                        # Extract href directly from card; skip if missing
                        prof_url = card.get_attribute("href")
                        if not prof_url:
                            continue

                        # Make URL absolute if needed
                        if prof_url.startswith("/"):
                            prof_url = f"https://www.ratemyprofessors.com{prof_url}"

                        prof_id = prof_url.rstrip('/').split('/')[-1]

                        if prof_id not in seen_professor_ids:
                            seen_professor_ids.add(prof_id)

                            professors.append({
                                "id": prof_id,
                                "name": name,
                                "department": department,
                                "school": school,
                                "overall_rating": rating,
                                "num_ratings": num_ratings,
                                "url": prof_url,
                            })

                        print(name, "extracted")

                    except Exception as e:
                        if isinstance(e, WebDriverException) and "tab crashed" in str(e):
                            tab_crashed = True
                            break
                        print(f"Error extracting professor data: {e}")

                if tab_crashed:
                    print(f"Stopping pagination (tab crashed). Returning {len(professors)} professors.")
                    break

                previous_count = len(professor_cards)

                # Check for "Show More" button
                try:
                    show_more_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Show More')]")

                    # Safety: check if it's visible and enabled
                    if show_more_button.is_displayed() and show_more_button.is_enabled():
                        try:
                            show_more_button.click()
                        except ElementClickInterceptedException:
                            try:
                                driver.execute_script("arguments[0].click();", show_more_button)
                            except Exception:
                                break
                    else:
                        break

                except NoSuchElementException:
                    break
                except WebDriverException:
                    print(f"Stopping pagination (tab crashed or unreachable). Returning {len(professors)} professors.")
                    break

                # Testing mode: Limit pages
                if testing and len(professors) > 20:
                    break
            except WebDriverException:
                print(f"Stopping pagination (tab crashed or unreachable). Returning {len(professors)} professors.")
                break

    finally:
        driver.quit()

    # Professors are supposed to be unique according to the name
    professors = {prof["name"]: prof for prof in professors}.values()

    return professors

def normalize_comment(text):
    return re.sub(r"\s+", " ", text.strip().lower())    

def to_scrape_professor(supabase, professors):
    '''
    Returns a list of the professors that need to be scraped.    
    '''
    professors_to_scrape = []
    
    # Query the database for the professors that have already been scraped - from the professors table get the name, num_ratings, latest_comment_date - ignore the entry where the name is 'general_professor'
    previous_professors = supabase.table("professors").select("name, num_ratings, latest_comment_date").execute().data
    previous_professors_dict = {
        prof["name"]: (prof["num_ratings"], prof["latest_comment_date"])
        for prof in previous_professors
        if prof["name"] != "general_prof"
    }

    # Iterate through the professors scraped from the website
    for prof in professors:
        # Check if the professor is already in the database
        if prof["name"] in previous_professors_dict:
            # If the num_ratings is different, we need to scrape it again
            if prof["num_ratings"] != previous_professors_dict[prof["name"]][0]:
                # Also, attach the latest_comment_date to the professor object
                prof["latest_comment_date"] = previous_professors_dict[prof["name"]][1]
                professors_to_scrape.append(prof)
        else:
            # If the professor is not in the database, we need to scrape it
            # Since they have not been scraped before, we can assume the latest_comment_date is None
            prof["latest_comment_date"] = None
            professors_to_scrape.append(prof)
            
    return professors_to_scrape

def safe_float(text):
    try:
        return float(text)
    except (ValueError, TypeError):
        return None 

def scrape_professor_comments(supabase, prof, valid_courses):
    """
    Given a professor object scrape detailed rating information.
    """
    
    # --- Set up headless Chrome ---
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-setuid-sandbox")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--js-flags=--max-old-space-size=512")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(20)

    # Log Message
    print(f"Scraping comments for {prof['name']}...")
    # print url
    print(prof["url"])

    try:
        try:
            driver.get(prof["url"])
        except TimeoutException:
            print(f"Timeout while loading {prof['url']}. Skipping...")
            return

        # Dismiss OneTrust cookie banner if present so it does not block clicks
        try:
            accept_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
            )
            accept_btn.click()
            time.sleep(1)
        except (TimeoutException, NoSuchElementException, Exception):
            pass

        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CLASS_NAME, "RatingValue__Numerator-qw8sqy-2"))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")

        has_reviews = prof["num_ratings"] > 0

        # Extract items from the professors page

        # 1) Overall Rating
        rating_elem = soup.select_one("div.RatingValue__Numerator-qw8sqy-2")
        overall_rating = None
        if has_reviews:
            text = rating_elem.text.strip() if rating_elem else None
            overall_rating = safe_float(text)

        # 2) % would take again and overall difficulty rating
        feedback_numbers = soup.select("div.FeedbackItem__FeedbackNumber-uof32n-1")
        percent_take_again = None
        level_of_difficulty = None
        if has_reviews:
            if len(feedback_numbers) > 0:
                percent_take_again = safe_float(feedback_numbers[0].text.strip('%'))
            if len(feedback_numbers) > 1:
                level_of_difficulty = safe_float(feedback_numbers[1].text.strip())

        # 3) Top Tags
        top_tags = [
            tag.text.strip()
            for tag in soup.select("div.TeacherTags__TagsContainer-sc-16vmh1y-0 span.Tag-bs9vf4-0")
        ]

        # 4) All of the courses that the professor has been reviewed on
        # Find the dropdown div and click it to open
        dropdown_button = driver.find_element(By.CLASS_NAME, "Select__getDropdownIndicator-sc-9f4k3m-0")
        dropdown_button.click()

        # After clicking the dropdown
        time.sleep(1)  # Wait for animation to open (can also do WebDriverWait instead)

        # Scrape all course options
        course_menu = driver.find_element(By.CLASS_NAME, "css-1ogydhz-menu")
        menu_text = course_menu.text
        # split if by newlines
        raw_courses = menu_text.split("\n")

        all_courses = set()
        for course in raw_courses:
            cleaned = re.sub(r"\(\d+\)", "", course).strip()
            if cleaned and cleaned.lower() != "all courses":
                all_courses.add(cleaned)

        course_code_mappings = clean_and_map_course_codes(all_courses, valid_courses)

        # Get all of the previous comments from the database
        response = supabase.table("rag_chunks").select("text", "created_at").eq("professor_name", prof["name"]).execute()
        existing_reviews_set = set((r["text"].strip(), r["created_at"]) for r in response.data)
        seen_reviews_set = set()
        
        # Start loopin through all of the comments
        reviews = []
        stop_scraping = False

        while True:
            time.sleep(1)

            # If the prof has no reviews, skip to the next one
            if not has_reviews:
                break

            # --- Extract Student Reviews ---
            reviews_list = soup.select_one("ul#ratingsList")
            review_items = reviews_list.select("li") if reviews_list else []
            
            # Loop through all of the reviews
            for block in review_items:
                # Check if it is not an ad
                rating_div = block.select_one("div.Rating__StyledRating-sc-1rhvpxz-1")

                if rating_div:
                    try:
                        # Get the date
                        date = block.select_one("div.TimeStamp__StyledTimeStamp-sc-9q2r30-0").text.strip()
                        # Remove the "th", "st", "nd", "rd" from the date string
                        date = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", date)
                        # Convert the date to a datetime object
                        date = datetime.strptime(date, "%b %d, %Y").date().isoformat()

                        # Check to see if the date is more recent than the latest comment date

                        if prof["latest_comment_date"] is not None:
                            if date <= prof["latest_comment_date"]:
                                stop_scraping = True
                                break

                        # Get the mapped course code
                        scraped_course_code = block.select_one("div.RatingHeader__StyledClass-sc-1dlkqw1-3").text.strip()
                        course_codes = course_code_mappings[scraped_course_code]
                        
                        quality_elem = block.select_one("div.CardNumRating__CardNumRatingNumber-sc-17t4b9u-2.ERCLc")
                        difficulty_elem = block.select_one("div.CardNumRating__CardNumRatingNumber-sc-17t4b9u-2.eBKGNg")

                        if quality_elem:
                            quality = float(quality_elem.text.strip())
                        else:
                            quality = overall_rating  # fallback
                        
                        if difficulty_elem:
                            difficulty = float(difficulty_elem.text.strip())
                        else:
                            difficulty = level_of_difficulty  # fallback
                        
                        comment = block.select_one("div.Comments__StyledComments-dzzyvm-0").text.strip()
                        
                        # Check to see if the review is valid
                        if not is_valid_comment(comment):
                            continue

                        sentiment_score, sentiment_label = detect_sentiment(comment)
                    
                        tag_spans = block.select("span.Tag-bs9vf4-0")
                        review_tags = [tag.text.strip() for tag in tag_spans]

                        if not course_codes:
                            course_codes = ["general_course"]

                        # Check to see if the review is a duplicate
                        normalized_comment = normalize_comment(comment)
                        if (normalized_comment, date) in existing_reviews_set or (normalized_comment, date) in seen_reviews_set:
                            continue
                        seen_reviews_set.add((normalized_comment, date))

                        parsed_review = {
                            "date": date,
                            "quality": quality,
                            "difficulty": difficulty,
                            "comment": normalized_comment,
                            "tags": review_tags,
                            "sentiment_score": sentiment_score,
                            "sentiment_label": sentiment_label,
                            "course_code": course,
                        }

                        reviews.append(parsed_review)
                        
                    except Exception as e:
                        print(f"Skipping one review, error: {e}")
            if stop_scraping:
                break
            # Check for "Load More Ratings" button
            try:
                load_more_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Load More Ratings')]")
                
                # Safety: check if it's visible and enabled
                if load_more_button.is_displayed() and load_more_button.is_enabled():
                    load_more_button.click()
                    # print("Clicked 'Load More Ratings'")
                else:
                    # print("'Load More Ratings' button not clickable anymore.")
                    break
            except NoSuchElementException:
                # print("No 'Load More Ratings' button found at all.")
                break

        date = None
        if len(reviews) > 0:
            date = reviews[0]["date"]
        
        # Update the professor object with the scraped data
        updated_prof = {
            "id": prof["id"],
            "name": prof["name"],
            "overall_rating": overall_rating,
            "percent_retake": percent_take_again,
            "level_of_difficulty": level_of_difficulty,
            "professor_tags": top_tags,
            "latest_comment_date": date,
            "num_ratings": prof["num_ratings"],
            "url": prof["url"],
        }
        
        supabase.table("professors").upsert(updated_prof, on_conflict=["id"]).execute()

        # Insert the reviews into the database
        if reviews:
            comment_data_batch = []
            for review in reviews:
                comment_data = {
                    "text": review["comment"],
                    "source": "ratemyprofessors",
                    "course_code": review["course_code"],
                    "professor_name": prof["name"],
                    "source_url": prof["url"],
                    "tags": review["tags"],
                    "created_at": review["date"],
                    "quality_rating": review["quality"],
                    "sentiment_score": review["sentiment_score"],
                    "sentiment_label": review["sentiment_label"],
                    "difficulty_rating": review["difficulty"],
                }
                comment_data_batch.append(comment_data)

            supabase.table("rag_chunks").insert(comment_data_batch).execute()
            print(f"Inserted {len(comment_data_batch)} reviews for {prof['name']}")
        else:
            print(f"No reviews found for {prof['name']}")

    finally:
        driver.quit()

    


if __name__ == "__main__":
    # Create Supabase client
    supabase = create_supabase_client()

    # Get all of the professors from the website
    professors = scrape_professors(supabase, testing=False)

    # Get the professors that need to be scraped
    professors_to_scrape = to_scrape_professor(supabase, professors)
    count_professors_to_scrape = len(professors_to_scrape)
    print(f"Number of professors to scrape: {count_professors_to_scrape}")

    # Get all of the valid courses from the database
    valid_courses = get_all_valid_courses(supabase)

    # Iterate through the professors that need to be scraped
    scraped_count = 0
    for prof in professors_to_scrape:
        scrape_professor_comments(supabase, prof, valid_courses)
        # Print what the current count is, and the remaining profs to be scraped
        scraped_count += 1
        print(f"Scraped {scraped_count}/{count_professors_to_scrape} professors")
        # Sleep for a bit to avoid being blocked
        time.sleep(1)

    print("Scraping complete") 
    