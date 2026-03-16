import praw
import os
import re
import math
import uuid
import hashlib
from datetime import datetime
from supabase import create_client, Client
from postgrest.exceptions import APIError
from textblob import TextBlob

# Precompiled regex patterns
PROF_NAME_REGEX = re.compile(r'\b(?:Prof\.?|Dr\.?)\s+[A-Z][a-z]+\s+[A-Z][a-z]+\b')
COURSE_CODE_REGEX = re.compile(r'\b[A-Za-z]{2,4}\s?\d{3}[A-Z]?\b')

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

def detect_sentiment(text, upvotes=1):
    """
    Composite sentiment: TextBlob polarity weighted with a Reddit upvote confidence signal.
    upvotes > ~10 amplifies the polarity direction (community agrees), low/negative upvotes dampen it.
    Returns (sentiment_score, sentiment_label).
    """
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity

    upvote_signal = math.tanh((upvotes - 1) / 10.0)  # maps ~(-1, 1); 1 upvote -> 0
    # If polarity and upvotes agree in sign, reinforce; otherwise dampen
    if polarity >= 0:
        score = 0.7 * polarity + 0.3 * max(upvote_signal, 0)
    else:
        score = 0.7 * polarity + 0.3 * min(upvote_signal, 0)

    score = max(-1.0, min(1.0, score))

    if score > 0.4:
        label = "very positive"
    elif score > 0.15:
        label = "positive"
    elif score < -0.4:
        label = "very negative"
    elif score < -0.15:
        label = "negative"
    else:
        label = "neutral"

    return round(score, 4), label

_NEG_PREFIX = r"(?:not|n't|never|isn't|wasn't|aren't|weren't|doesn't|don't|didn't|hardly|barely)\s+"

def detect_tags(text):
    """
    Assigns canonical tags to a comment for RAG retrieval.
    Returns a list of tags.
    """
    body = text.lower()
    tags = []

    is_not_easy = bool(re.search(_NEG_PREFIX + r"(easy|light|bird course|straightforward|manageable|simple|chill)", body))
    is_not_hard = bool(re.search(_NEG_PREFIX + r"(hard|tough|difficult|challenging|brutal|intense|heavy|killer)", body))

    easy_words = [
        "easy", "light", "bird course", "manageable", "straightforward",
        "simple", "chill", "gpa booster", "gut course", "easy a",
    ]
    hard_words = [
        "hard", "tough", "difficult", "challenging", "brutal", "intense",
        "heavy", "killer", "weed-out", "weed out", "insane workload",
    ]

    if not is_not_easy and any(w in body for w in easy_words):
        tags.append("easy")
    if not is_not_hard and any(w in body for w in hard_words):
        tags.append("hard")

    if any(w in body for w in [
        "professor", "prof ", "prof.", "lecturer", "teaching", "instructor",
        "teaches", "taught", "dr.", "office hours",
    ]):
        tags.append("professor_review")

    if any(w in body for w in [
        "exam", "midterm", "final", "assignment", "homework", "reading",
        "workload", "labs", "quizzes", "group project", "project", "lab report",
        "tutorial", "lecture", "seminar", "presentation",
    ]):
        tags.append("course_structure")

    if any(w in body for w in [
        "recommend", "tip", "advice", "suggest", "strategy", "resource",
        "how to study", "study guide", "practice problems",
    ]):
        tags.append("tips")

    if any(w in body for w in [
        "bell curve", "curved", "harsh grading", "harsh marker", "lenient",
        "fair grading", "easy marker", "tough grader", "grade inflation",
    ]):
        tags.append("grading")

    if any(w in body for w in [
        "heavy workload", "time-consuming", "time consuming", "reading-heavy",
        "reading heavy", "hours per week", "weekly", "constant work",
    ]):
        tags.append("workload")

    if any(w in body for w in [
        "online", "remote", "hybrid", "recorded", "asynchronous", "async",
        "zoom", "virtual",
    ]):
        tags.append("online")

    if any(w in body for w in [
        "group project", "group assignment", "teamwork", "partner", "team-based",
        "group work", "group presentation",
    ]):
        tags.append("group_work")

    if any(w in body for w in [
        "would recommend", "take this", "must take", "highly recommend",
        "avoid", "don't take", "do not take", "skip this", "worst course",
        "best course", "loved this", "hated this",
    ]):
        tags.append("recommendation")

    if any(w in body for w in [
        "exam-heavy", "exam heavy", "multiple choice", "written exam",
        "open book", "closed book", "cheat sheet", "proctored",
    ]):
        tags.append("exam_heavy")

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

def _normalize_course_code(raw):
    """'cisc121' / 'CISC 121A' -> 'CISC 121'  (strip optional suffix letter, insert space)."""
    code = raw.replace(" ", "").upper()
    return re.sub(r"([A-Z]{2,4})(\d{3})[A-Z]?", r"\1 \2", code)

def extract_course_code_from_post(post):
    full_text = f"{post.title} {post.selftext}"
    match = COURSE_CODE_REGEX.search(full_text)
    return _normalize_course_code(match.group(0)) if match else None

def extract_course_code_from_comment(comment):
    match = COURSE_CODE_REGEX.search(comment.body)
    return _normalize_course_code(match.group(0)) if match else None

GENERAL_COURSE_KEYWORDS = [
    # Original
    "courses", "course", "classes", "electives", "program requirements", "bird courses", "easy a",
    # Registration / planning
    "enrol", "enrollment", "registration", "timetable", "schedule", "prerequisite",
    "corequisite", "waitlist", "add/drop", "degree requirements", "academic plan",
    "course load", "overload", "breadth requirement", "solus",
    # Grading / assessment
    "gpa", "grade", "grading", "bell curve", "curved", "pass/fail", "dean's list",
    "transcript", "credit", "unit",
    # Workload
    "workload", "readings", "assignments", "midterm", "final exam", "essay",
    "lab", "tutorial", "lecture",
    # General academic
    "major", "minor", "specialization", "concentration", "faculty", "department",
    "ta ", "office hours", "syllabus",
    # Slang / colloquial
    "bird course", "gpa booster", "gut course", "brutal", "weed-out", "weed out",
    # Professor-related
    "professor", "prof ", "prof.", "instructor", "lecturer", "dr.",
]

def is_post_of_interest(post):
    if not post.is_self:
        return False

    if post.over_18:
        return False

    full_text = post.title.lower() + " " + (post.selftext or "").lower()
    has_course_code = bool(re.search(COURSE_CODE_REGEX.pattern, full_text))
    has_keyword = any(kw in full_text for kw in GENERAL_COURSE_KEYWORDS)

    # Allow empty body only when the *title* alone contains a course code or keyword
    if not post.selftext.strip():
        if not (has_course_code or has_keyword):
            return False

    if not (has_course_code or has_keyword):
        return False

    if post.locked:
        return False

    if post.score < 2 or post.num_comments == 0:
        return False

    return True
    
def clean_text(text):
    # Replace multiple newlines with a single newline
    text = re.sub(r'\n\s*\n', '\n\n', text)
    # Remove leading/trailing spaces
    text = text.strip()
    # Optionally, replace bullets like 'a)' or 'b)' with dashes
    text = re.sub(r'^[a-z]\)', '-', text, flags=re.MULTILINE)
    return text

def _iter_unique_posts(subreddit, limit=1000):
    """Yield posts from .new(), .top(all), and .hot(), deduplicating by post ID."""
    seen_ids = set()
    sources = [
        ("new", subreddit.new(limit=limit)),
        ("top(all)", subreddit.top(time_filter="all", limit=limit)),
        ("hot", subreddit.hot(limit=limit)),
    ]
    for label, listing in sources:
        count = 0
        for post in listing:
            if post.id not in seen_ids:
                seen_ids.add(post.id)
                yield post
                count += 1
        print(f"  [{label}] yielded {count} unique posts (total unique so far: {len(seen_ids)})")

def scrape_and_store(courses, professors):
    subreddit = reddit.subreddit("queensuniversity")
    results = []
    print("Scraping r/queensuniversity via .new(), .top(all), .hot()...")

    # Pre-fetch already-stored post URLs to skip posts whose comments are fully stored
    processed_posts = supabase.table("rag_chunks").select("source_url").eq("source", "reddit").execute()
    processed_posts_urls = {row["source_url"] for row in processed_posts.data}
    print(f"  {len(processed_posts_urls)} Reddit post URLs already in DB.")

    for post in _iter_unique_posts(subreddit):
        if post.url in processed_posts_urls:
            continue


        if not is_post_of_interest(post):
            continue

        post_date = datetime.utcfromtimestamp(post.created_utc).strftime("%Y-%m-%d")
        course_code = extract_course_code_from_post(post) or None
        post_prof_name = extract_prof_name_from_post(post) or None

        post.comments.replace_more(limit=None)
        for comment in post.comments:
            if not is_comment_of_interest(comment):
                continue

            temp_course_code = course_code or extract_course_code_from_comment(comment)

            # Per-comment prof detection (avoid leaking across comments)
            comment_prof = post_prof_name or extract_prof_name_from_comment(comment)

            # If no course code found, allow storage only when a known professor is referenced
            if not temp_course_code:
                if comment_prof and comment_prof in professors:
                    temp_course_code = "general_course"
                else:
                    continue

            tags = detect_tags(comment.body)
            sentiment_score, sentiment_label = detect_sentiment(comment.body, upvotes=comment.score)

            # Resolve professor: use known name or fall back to general_prof
            resolved_prof = comment_prof if comment_prof in professors else "general_prof"

            comment_data = {
                "text": comment.body,
                "source": "reddit",
                "course_code": temp_course_code,
                "source_url": post.url,
                "tags": tags,
                "professor_name": resolved_prof,
                "sentiment_score": sentiment_score,
                "sentiment_label": sentiment_label,
                "upvotes": comment.score,
                "created_at": datetime.utcfromtimestamp(comment.created_utc).date().isoformat(),
                "text_hash": hashlib.md5(comment.body.encode()).hexdigest(),
            }

            if temp_course_code in courses or temp_course_code == "general_course":
                resp = supabase.table("rag_chunks").upsert(
                    comment_data,
                    on_conflict="source,source_url,text_hash",
                    ignore_duplicates=True,
                ).execute()
                if resp.data:
                    results.append(comment_data)
                    print(f"[{post_date}] Stored comment: course={temp_course_code}, prof={resolved_prof}, post={(post.title or '')[:60]}...")
                else:
                    print(f"[{post_date}] Skipped duplicate comment (already in DB).")

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
