import praw
import os
import re
import uuid
from datetime import datetime
from supabase import create_client, Client
from postgrest.exceptions import APIError
from textblob import TextBlob

# Precompiled regex patterns
PROF_NAME_REGEX = re.compile(r'\b(?:Prof\.?|Dr\.?)\s+[A-Z][a-z]+\s+[A-Z][a-z]+\b')
COURSE_CODE_REGEX = re.compile(r'\b[A-Za-z]{4}\s?\d{3}\b')

def create_supabase_client():
    """
    Create a Supabase client using environment variables for URL and key.
    Prefers SUPABASE_SERVICE_ROLE_KEY when set (bypasses RLS; use in CI/backend).
    """
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(SUPABASE_URL, key)
    return supabase

def setup_reddit():
    """
    Setup Reddit API client using environment variables for client ID and secret.
    """
    REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
    REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent="CourseCentralBot by /u/CourseCentralQU"
    )
    return reddit

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

def detect_tags(text):
    """
    Creates tags based on the text content.
    Returns a list of tags.
    Possible tag values: easy, hard, professor_review, course_structure (i.e final exams, assignments, workload), tips
    """
    body = text.lower()
    tags = []

    # Preprocessing: detect negations manually
    is_not_easy = bool(re.search(r"not\s+(easy|light|bird course|straightforward)", body))
    is_not_hard = bool(re.search(r"not\s+(hard|tough|difficult|challenging|brutal|intense)", body))

    # Difficulty (easy)
    if not is_not_easy and any(word in body for word in ["easy", "light", "bird course", "manageable", "straightforward"]):
        tags.append("easy")

    # Difficulty (hard)
    if not is_not_hard and any(word in body for word in ["hard", "tough", "difficult", "challenging", "brutal", "intense"]):
        tags.append("hard")

    # Professor reviews
    if any(word in body for word in ["professor", "lecturer", "teaching", "instructor", "teaches", "taught"]):
        tags.append("professor_review")

    # Course structure (exams, assignments, workload)
    if any(word in body for word in ["exam", "midterm", "final", "assignment", "homework", "reading", "workload", "labs", "quizzes", "group project"]):
        tags.append("course_structure")

    # Tips and advice
    if any(word in body for word in ["recommend", "tip", "advice", "suggest", "strategy", "resource", "how to study"]):
        tags.append("tips")

    return tags

def extract_prof_name_from_post(post):
    """
    Extract the first detected professor name from a Reddit post's title or selftext.
    Matches formats like 'Dr. John Doe', 'Prof. Jane Smith', case-insensitive.
    
    Returns:
        prof_name (str) if found, otherwise None
    """
    # Combine title and selftext for full scanning
    full_text = f"{post.title} {post.selftext}"

    # Search for professor names
    match = PROF_NAME_REGEX.search(full_text)
    
    if match:
        return match.group(0)
    else:
        return None
    
def extract_prof_name_from_comment(comment):
    """
    Extract the first detected professor name from a Reddit comment's body.
    Matches formats like 'Dr. John Doe', 'Prof. Jane Smith', case-insensitive.
    
    Returns:
        prof_name (str) if found, otherwise None
    """
    # Search for professor names
    match = PROF_NAME_REGEX.search(comment.body)
    
    if match:
        return match.group(0)
    else:
        return None

def is_comment_of_interest(comment):
    body = comment.body.strip()

    # Must not be empty
    if not body:
        return False

    # Must not be deleted or removed
    if body.lower() in ["[deleted]", "[removed]"]:
        return False

    # Optional: Must have some engagement
    if comment.score < 1:
        return False

    # Optional: Must have decent length (avoid "lol" type comments)
    if len(body) < 15:
        return False

    # ✅ Passed all checks
    return True

def extract_course_code_from_post(post):
    full_text = f"{post.title} {post.selftext}"
    match = COURSE_CODE_REGEX.search(full_text)
    
    if match:
        course_code = match.group(0).replace(" ", "").upper()
        # Insert a space between letters and digits
        course_code = re.sub(r"([A-Z]{4})(\d{3})", r"\1 \2", course_code)
        return course_code
    else:
        return None

def extract_course_code_from_comment(comment):
    match = COURSE_CODE_REGEX.search(comment.body)
    
    if match:
        course_code = match.group(0).replace(" ", "").upper()
        course_code = re.sub(r"([A-Z]{4})(\d{3})", r"\1 \2", course_code)
        return course_code
    else:
        return None

def is_post_of_interest(post):
    # Filter: must be a self post (not a link post)
    if not post.is_self:
        return False

    # Filter: must NOT be NSFW
    if post.over_18:
        return False

    # Filter: must have non-empty body
    if not post.selftext.strip():
        return False

    # Must mention either a course code OR general course-related keywords
    course_code_regex = r'\b[A-Za-z]{4}\s?\d{3}\b'
    general_course_keywords = ["courses", "course", "classes", "electives", "program requirements", "bird courses", "easy A"]

    full_text = post.title.lower() + " " + post.selftext.lower()

    if not (re.search(course_code_regex, full_text) or any(keyword in full_text for keyword in general_course_keywords)):
        return False

    # (Optional) Filter: avoid locked posts
    if post.locked:
        return False

    # (Optional) Filter: require some engagement
    if post.score < 2 or post.num_comments == 0:
        return False

    # ✅ Otherwise, looks interesting!
    return True
    
def clean_text(text):
    # Replace multiple newlines with a single newline
    text = re.sub(r'\n\s*\n', '\n\n', text)
    # Remove leading/trailing spaces
    text = text.strip()
    # Optionally, replace bullets like 'a)' or 'b)' with dashes
    text = re.sub(r'^[a-z]\)', '-', text, flags=re.MULTILINE)
    return text

def scrape_and_store(courses, professors):
    subreddit = reddit.subreddit("queensuniversity")
    results = []
    print("Scraping r/queensuniversity (limit 1000 posts)...")

    for post in subreddit.new(limit=1000):

        # Determine if this is a post of interest, if not, skip it
        if not is_post_of_interest(post):
            continue
        
        # If the post title/description contains a course code, extract it, otherwise set to None
        course_code = extract_course_code_from_post(post)
        if not course_code:
            course_code = None

        # If the post title/description contains a prof name, extract it, otherwise set to None
        prof_name = extract_prof_name_from_post(post)
        if not prof_name:
            prof_name = None

        # Iterate through the comments of the post
        post.comments.replace_more(limit=None)
        for comment in post.comments:
            # Check to see if the comment is a valid comment
            if not is_comment_of_interest(comment):
                continue
            
            # If course_code is not None, use it, otherwise try to find the course code in the comment.
            # If that fails, skip the comment.
            # This is to ensure that we have a course code for every comment.
            temp_course_code = course_code or extract_course_code_from_comment(comment)
            if not temp_course_code:
                continue

            # If the professor name is not None, use it, otherwise try to find the professor name in the comment.
            if not prof_name:
                prof_name = extract_prof_name_from_comment(comment)

            # Extract tags from the comment
            tags = detect_tags(comment.body)

            # Extract sentiment from the comment
            sentiment_score, sentiment_label = detect_sentiment(comment.body)

            comment_data = {
                "text": comment.body,
                "source": "reddit",
                "course_code": temp_course_code,
                "source_url": post.url,
                "tags": tags,
                "professor_name": prof_name,
                "sentiment_score": sentiment_score,
                "sentiment_label": sentiment_label,
                "upvotes": comment.score,
                "created_at": datetime.utcfromtimestamp(comment.created_utc).date().isoformat(),
            }

            # If the course code is in the list of valid courses, insert the comment into the database
            if temp_course_code in courses:
                if prof_name in professors:
                    comment_data["professor_name"] = prof_name
                else:
                    comment_data["professor_name"] = 'general_prof'

                try:
                    supabase.table("rag_chunks").insert(comment_data).execute()
                    results.append(comment_data)
                    print(f"Stored comment: course={temp_course_code}, post={(post.title or '')[:60]}...")
                except APIError as e:
                    code = getattr(e, "code", None) or (e.args[0].get("code") if e.args and isinstance(e.args[0], dict) else None)
                    if code == "23505":
                        print(f"Skipped duplicate comment (already in DB).")
                    else:
                        raise

    print(f"Reddit scrape done: {len(results)} comments stored.")
    return results

if __name__ == "__main__":
    # Initialize Supabase client and Reddit client
    supabase = create_supabase_client()
    reddit = setup_reddit()

   # Get all valid courses from Supabase
    courses_response = supabase.table("courses").select("course_code").execute()
    courses = courses_response.data  # <-- .data gives you the list
    courses = [c for c in courses if c["course_code"] != "general_course"]
    courses = {c["course_code"] for c in courses}

    # Get all valid professors from Supabase
    professors_response = supabase.table("professors").select("name").execute()
    professors = professors_response.data
    professors = [p for p in professors if p["name"] != "general_prof"]
    professors = {p["name"] for p in professors}
    print(f"Loaded {len(courses)} courses, {len(professors)} professors.")

    # Scrape and store comments
    scraped_data = scrape_and_store(courses, professors)
    print(f"Stored {len(scraped_data)} comments from Reddit.")
    print("Reddit scrape complete.")
