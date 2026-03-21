from rmp_client import RMPClient
import hashlib
import time
from transformers import pipeline
from supabase import create_client, Client
from postgrest.exceptions import APIError
import re
from datetime import datetime, date
import os

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
    Paginates to avoid Supabase's default 1000-row limit.
    """
    all_courses = []
    page_size = 1000
    offset = 0
    while True:
        batch = supabase.table("courses").select("course_code").range(offset, offset + page_size - 1).execute().data
        all_courses.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    valid_course_codes = {course["course_code"] for course in all_courses if course["course_code"] != "general_course"}

    return valid_course_codes

def clean_and_map_course_codes(course_codes, valid_courses):
    """
    Strict course code mapping: exact match or prefix+number from the original
    code only. No cross-department guessing.
    """
    valid_courses_no_space = {c.replace(" ", "").upper(): c for c in valid_courses}
    course_mapping = {}

    for raw_code in course_codes:
        cleaned = raw_code.strip().replace(" ", "").upper()

        # 1. Exact match (e.g. "APSC112" -> "APSC 112")
        if cleaned in valid_courses_no_space:
            course_mapping[raw_code] = [valid_courses_no_space[cleaned]]
            continue

        # 2. Extract the original prefix + first 3-digit number only
        prefix_match = re.match(r"^([A-Z]+)[-\s]?(\d{3})", cleaned)
        if prefix_match:
            prefix, num = prefix_match.group(1), prefix_match.group(2)
            candidate = f"{prefix} {num}"
            if candidate in valid_courses:
                course_mapping[raw_code] = [candidate]
                continue

        # 3. No match -> general_course
        course_mapping[raw_code] = None

    return course_mapping

_sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="CoursifyQU/student-review-sentiment",
    device=-1,
)


def detect_sentiment(text):
    """
    Text-only sentiment using fine-tuned RoBERTa model trained on 18K+ Queen's student reviews.
    Returns a (-1..1) score and a human-readable label.
    Quality/difficulty ratings are stored as separate structured fields.
    """
    result = _sentiment_pipeline(text[:2000], truncation=True, max_length=512)[0]
    raw_label = result["label"]       # "negative", "neutral", or "positive"
    confidence = result["score"]

    if raw_label == "positive":
        score = confidence
    elif raw_label == "negative":
        score = -confidence
    else:
        score = 0.0  # neutral

    score = round(score, 4)

    if score > 0.85:
        label = "very positive"
    elif score > 0.3:
        label = "positive"
    elif score < -0.85:
        label = "very negative"
    elif score < -0.3:
        label = "negative"
    else:
        label = "neutral"

    return score, label

def scrape_professors(client, supabase, testing=True):
    """
    Fetch all professors for the university using the RMP GraphQL API.
    """
    professors = []
    seen_names = set()
    duplicates_skipped = 0

    print(f"\n{'='*60}")
    print(f"PHASE 1: Fetching professors for {UNIVERSITY_NAME} (ID: {UNIVERSITY_ID})")
    print(f"{'='*60}")
    start_time = time.time()

    for prof in client.iter_professors_for_school(UNIVERSITY_ID):
        if prof.name in seen_names:
            duplicates_skipped += 1
            continue
        seen_names.add(prof.name)

        prof_id = str(prof.id)
        prof_url = f"https://www.ratemyprofessors.com/professor/{prof_id}"

        professors.append({
            "id": prof_id,
            "name": prof.name,
            "department": prof.department,
            "school": prof.school.name if prof.school else UNIVERSITY_NAME,
            "overall_rating": prof.overall_rating,
            "num_ratings": prof.num_ratings or 0,
            "url": prof_url,
        })

        if testing and len(professors) > 20:
            break

    elapsed = time.time() - start_time
    print(f"  Fetched {len(professors)} professors in {elapsed:.1f}s")
    if duplicates_skipped:
        print(f"  Skipped {duplicates_skipped} duplicate names")
    with_ratings = sum(1 for p in professors if p["num_ratings"] > 0)
    without_ratings = len(professors) - with_ratings
    print(f"  {with_ratings} with ratings, {without_ratings} with no ratings")
    return professors

RMP_TAG_TO_CANONICAL = {
    "Tough grader": ["hard", "grading"],
    "Clear grading criteria": ["grading"],
    "Graded by few things": ["grading"],
    "Inspirational": ["professor_review"],
    "Amazing lectures": ["professor_review"],
    "Respected": ["professor_review"],
    "Caring": ["professor_review"],
    "Hilarious": ["professor_review"],
    "Accessible outside class": ["professor_review"],
    "Gives good feedback": ["professor_review"],
    "Lots of homework": ["workload"],
    "Get ready to read": ["workload"],
    "So many papers": ["workload"],
    "Group projects": ["group_work"],
    "Test heavy": ["exam_heavy"],
    "Beware of pop quizzes": ["exam_heavy"],
    "Tests? Not many": ["course_structure"],
    "Skip class? You won't pass.": ["course_structure"],
    "Lecture heavy": ["course_structure"],
    "Participation matters": ["course_structure"],
    "Online savvy": ["online"],
    "Extra credit": ["tips"],
    "Would take again": ["recommendation"],
    "Would not take again": ["recommendation"],
}

def normalize_rmp_tags(raw_tags):
    """Map raw RMP tags to canonical tag set, returning deduplicated canonical tags."""
    canonical = set()
    for tag in raw_tags:
        for mapped in RMP_TAG_TO_CANONICAL.get(tag, []):
            canonical.add(mapped)
    return sorted(canonical)

def normalize_comment(text):
    return re.sub(r"\s+", " ", text.strip().lower())

def to_scrape_professor(supabase, professors):
    '''
    Returns a list of the professors that need to be scraped.
    '''
    professors_to_scrape = []
    new_professors = 0
    updated_professors = 0
    up_to_date = 0

    # Query the database for the professors that have already been scraped - from the professors table get the name, num_ratings, latest_comment_date - ignore the entry where the name is 'general_professor'
    previous_professors = supabase.table("professors").select("name, num_ratings, latest_comment_date").execute().data
    previous_professors_dict = {
        prof["name"]: (prof["num_ratings"], prof["latest_comment_date"])
        for prof in previous_professors
        if prof["name"] != "general_prof"
    }

    print(f"\n{'='*60}")
    print(f"PHASE 2: Delta detection")
    print(f"{'='*60}")
    print(f"  Professors in DB: {len(previous_professors_dict)}")
    print(f"  Professors from API: {len(professors)}")

    # Iterate through the professors scraped from the website
    for prof in professors:
        # Check if the professor is already in the database
        if prof["name"] in previous_professors_dict:
            db_count = previous_professors_dict[prof["name"]][0]
            # If the num_ratings is different, we need to scrape it again
            if prof["num_ratings"] != db_count:
                # Also, attach the latest_comment_date to the professor object
                prof["latest_comment_date"] = previous_professors_dict[prof["name"]][1]
                professors_to_scrape.append(prof)
                updated_professors += 1
                delta = prof["num_ratings"] - (db_count or 0)
                print(f"  [UPDATED] {prof['name']}: {db_count} -> {prof['num_ratings']} ratings (+{delta})")
            else:
                up_to_date += 1
        else:
            # If the professor is not in the database, we need to scrape it
            # Since they have not been scraped before, we can assume the latest_comment_date is None
            prof["latest_comment_date"] = None
            professors_to_scrape.append(prof)
            new_professors += 1
            print(f"  [NEW] {prof['name']}: {prof['num_ratings']} ratings")

    print(f"\n  Summary: {new_professors} new, {updated_professors} updated, {up_to_date} up-to-date")
    print(f"  Total to scrape: {len(professors_to_scrape)}")
    return professors_to_scrape

def scrape_professor_comments(client, supabase, prof, valid_courses):
    """
    Given a professor object, fetch detailed rating information via the RMP API.
    """
    print(f"\n  --- {prof['name']} ({prof['url']}) ---")

    has_reviews = prof["num_ratings"] > 0

    # --- Fetch professor details ---
    prof_details = client.get_professor(prof["id"])
    overall_rating = prof_details.overall_rating
    percent_take_again = prof_details.percent_take_again
    level_of_difficulty = prof_details.level_of_difficulty
    print(f"  Rating: {overall_rating}/5 | Difficulty: {level_of_difficulty}/5 | Would retake: {percent_take_again}%")

    if not has_reviews:
        # Still upsert professor metadata even with no reviews
        updated_prof = {
            "id": prof["id"],
            "name": prof["name"],
            "overall_rating": overall_rating,
            "percent_retake": percent_take_again,
            "level_of_difficulty": level_of_difficulty,
            "professor_tags": [],
            "latest_comment_date": None,
            "num_ratings": prof["num_ratings"],
            "url": prof["url"],
        }
        try:
            supabase.table("professors").upsert(updated_prof, on_conflict=["id"]).execute()
        except APIError as e:
            code = getattr(e, "code", None) or (e.args[0].get("code") if e.args and isinstance(e.args[0], dict) else None)
            if code == "23505":
                print(f"  Skipped professor upsert (duplicate name)")
            else:
                raise
        print(f"  No reviews to process")
        return

    # --- Fetch ratings via iterator with since filter ---
    since_date = None
    if prof["latest_comment_date"]:
        since_date = date.fromisoformat(prof["latest_comment_date"])
        print(f"  Fetching ratings since {since_date}...")
    else:
        print(f"  Fetching all ratings (new professor)...")

    # Collect all ratings first to build course code mappings
    raw_ratings = list(client.iter_professor_ratings(prof["id"], since=since_date))
    print(f"  API returned {len(raw_ratings)} ratings")

    # Build course code mappings from all course labels in ratings
    all_courses = set()
    for rating in raw_ratings:
        if rating.course_raw:
            all_courses.add(rating.course_raw)
    course_code_mappings = clean_and_map_course_codes(all_courses, valid_courses) if all_courses else {}

    mapped_count = sum(1 for v in course_code_mappings.values() if v is not None)
    unmapped_count = sum(1 for v in course_code_mappings.values() if v is None)
    if all_courses:
        print(f"  Course codes found: {all_courses}")
        print(f"  Mapped: {mapped_count}, Unmapped: {unmapped_count}")
        for raw, mapped in course_code_mappings.items():
            if mapped:
                print(f"    {raw} -> {mapped[0]}")
            else:
                print(f"    {raw} -> general_course (no match)")

    # Get all of the previous comments from the database
    response = supabase.table("rag_chunks").select("text", "created_at").eq("professor_name", prof["name"]).execute()
    existing_reviews_set = set((r["text"].strip(), r["created_at"]) for r in response.data)
    seen_reviews_set = set()
    print(f"  Existing reviews in DB: {len(existing_reviews_set)}")

    # Collect all unique tags from ratings for professor-level tags
    all_tag_counts = {}

    reviews = []
    skipped_invalid = 0
    skipped_duplicate = 0

    for rating in raw_ratings:
        comment = rating.comment
        if not is_valid_comment(comment):
            skipped_invalid += 1
            continue

        rating_date = rating.date.isoformat()

        # Get the mapped course code
        course_codes = None
        if rating.course_raw:
            course_codes = course_code_mappings.get(rating.course_raw)
        if not course_codes:
            course_codes = ["general_course"]

        quality = rating.quality if rating.quality is not None else overall_rating
        difficulty = rating.difficulty if rating.difficulty is not None else level_of_difficulty

        # Normalize and deduplicate
        normalized_comment = normalize_comment(comment)
        if (normalized_comment, rating_date) in existing_reviews_set or (normalized_comment, rating_date) in seen_reviews_set:
            skipped_duplicate += 1
            continue
        seen_reviews_set.add((normalized_comment, rating_date))

        review_tags = rating.tags
        canonical_tags = normalize_rmp_tags(review_tags)
        sentiment_score, sentiment_label = detect_sentiment(comment)

        # Track tag counts for professor-level tags
        for tag in review_tags:
            all_tag_counts[tag] = all_tag_counts.get(tag, 0) + 1

        parsed_review = {
            "date": rating_date,
            "quality": quality,
            "difficulty": difficulty,
            "comment": normalized_comment,
            "tags": review_tags,
            "canonical_tags": canonical_tags,
            "sentiment_score": sentiment_score,
            "sentiment_label": sentiment_label,
            "course_codes": course_codes,
        }

        reviews.append(parsed_review)

    print(f"  Processing: {len(reviews)} new, {skipped_duplicate} duplicates, {skipped_invalid} invalid (too short)")

    # Build top tags from aggregated tag counts
    top_tags = sorted(all_tag_counts, key=all_tag_counts.get, reverse=True)[:5]

    latest_date = None
    if len(reviews) > 0:
        latest_date = reviews[0]["date"]

    # Update the professor object with the scraped data
    updated_prof = {
        "id": prof["id"],
        "name": prof["name"],
        "overall_rating": overall_rating,
        "percent_retake": percent_take_again,
        "level_of_difficulty": level_of_difficulty,
        "professor_tags": top_tags,
        "latest_comment_date": latest_date,
        "num_ratings": prof["num_ratings"],
        "url": prof["url"],
    }

    try:
        supabase.table("professors").upsert(updated_prof, on_conflict=["id"]).execute()
        print(f"  Professor record updated")
    except APIError as e:
        code = getattr(e, "code", None) or (e.args[0].get("code") if e.args and isinstance(e.args[0], dict) else None)
        if code == "23505":
            # Duplicate name (unique_professor_name): same person, different RMP id — skip upsert, still insert comments
            print(f"  Skipped professor upsert (duplicate name), inserting reviews only")
        else:
            raise

    # Insert the reviews into the database (one row per course_code for FK)
    if reviews:
        comment_data_batch = []
        for review in reviews:
            for code in review["course_codes"]:
                comment_data = {
                    "text": review["comment"],
                    "source": "ratemyprofessors",
                    "course_code": code,
                    "professor_name": prof["name"],
                    "source_url": prof["url"],
                    "tags": review["canonical_tags"] + review["tags"],
                    "created_at": review["date"],
                    "quality_rating": review["quality"],
                    "sentiment_score": review["sentiment_score"],
                    "sentiment_label": review["sentiment_label"],
                    "difficulty_rating": review["difficulty"],
                    "text_hash": hashlib.md5(review["comment"].encode()).hexdigest(),
                }
                comment_data_batch.append(comment_data)

        try:
            supabase.table("rag_chunks").upsert(
                comment_data_batch,
                on_conflict="source,source_url,text_hash",
                ignore_duplicates=True,
            ).execute()
            print(f"  Inserted {len(comment_data_batch)} review rows ({len(reviews)} reviews across {len(set(c['course_code'] for c in comment_data_batch))} courses)")
        except APIError as e:
            api_code = getattr(e, "code", None) or (e.args[0].get("code") if e.args and isinstance(e.args[0], dict) else None)
            print(f"  ERROR: Could not insert reviews (API error {api_code}): {e}")
    else:
        print(f"  No new reviews to insert")


if __name__ == "__main__":
    total_start = time.time()
    print(f"RMP Scraper started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Create Supabase client
    supabase = create_supabase_client()

    with RMPClient() as client:
        # Get all of the professors from the website
        professors = scrape_professors(client, supabase, testing=False)

        # Get the professors that need to be scraped
        professors_to_scrape = to_scrape_professor(supabase, professors)

        if not professors_to_scrape:
            print("\nNo professors need scraping. Everything is up to date.")
        else:
            # Get all of the valid courses from the database
            valid_courses = get_all_valid_courses(supabase)
            print(f"\nLoaded {len(valid_courses)} valid courses from DB")

            print(f"\n{'='*60}")
            print(f"PHASE 3: Scraping reviews for {len(professors_to_scrape)} professors")
            print(f"{'='*60}")

            # Iterate through the professors that need to be scraped
            total_new_reviews = 0
            total_errors = 0
            for i, prof in enumerate(professors_to_scrape, 1):
                try:
                    scrape_professor_comments(client, supabase, prof, valid_courses)
                except Exception as e:
                    print(f"  ERROR scraping {prof.get('name', 'unknown')}: {e}")
                    total_errors += 1
                print(f"  Progress: {i}/{len(professors_to_scrape)}")
                time.sleep(1)

            if total_errors:
                print(f"\n  Errors encountered: {total_errors}")

    total_elapsed = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"RMP Scraper complete in {total_elapsed:.1f}s ({total_elapsed/60:.1f}m)")
    print(f"{'='*60}")
