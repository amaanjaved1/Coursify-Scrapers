"""
One-shot backfill script: recomputes sentiment_score and sentiment_label for all
existing RMP rag_chunks using the improved composite sentiment function.

Safe to run multiple times (idempotent).

Usage:
    python apps/scrapers/rmp-sentiment-backfill.py
"""
import os
import time

import httpx
from supabase import create_client, Client
from textblob import TextBlob

# ---------------------------------------------------------------------------
# Sentiment lookup (mirrors rmp-scraper.py — keep in sync)
# ---------------------------------------------------------------------------
RMP_TAG_SENTIMENT = {
    "Amazing lectures": 0.8, "Inspirational": 0.9, "Respected": 0.7,
    "Caring": 0.7, "Hilarious": 0.6, "Accessible outside class": 0.6,
    "Clear grading criteria": 0.5, "Gives good feedback": 0.6,
    "Would take again": 0.8, "Participation matters": 0.1,
    "Lecture heavy": 0.0, "Tests? Not many": 0.2, "Extra credit": 0.3,
    "Graded by few things": -0.1, "Group projects": 0.0,
    "Online savvy": 0.2, "Beware of pop quizzes": -0.2,
    "Tough grader": -0.4, "Skip class? You won't pass.": -0.3,
    "Get ready to read": -0.3, "Lots of homework": -0.4,
    "Test heavy": -0.3, "So many papers": -0.4,
    "Would not take again": -0.9,
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CHECKPOINT_FILE = os.path.join(SCRIPT_DIR, "backfill_checkpoint.txt")
MAX_RETRIES = int(os.getenv("RMP_BACKFILL_MAX_RETRIES", "5"))
PROGRESS_EVERY = int(os.getenv("RMP_BACKFILL_PROGRESS_EVERY", "500"))


def detect_sentiment(text, quality_rating=None, difficulty_rating=None, tags=None):
    blob = TextBlob(text)
    text_polarity = blob.sentiment.polarity

    quality_signal = (quality_rating - 3.0) / 2.0 if quality_rating is not None else 0.0

    difficulty_signal = 0.0
    if difficulty_rating is not None and quality_rating is not None:
        if difficulty_rating >= 4.0 and quality_rating <= 2.0:
            difficulty_signal = -0.6
        elif difficulty_rating >= 4.0 and quality_rating <= 3.0:
            difficulty_signal = -0.3
        elif difficulty_rating <= 2.0 and quality_rating >= 4.0:
            difficulty_signal = 0.4
        else:
            difficulty_signal = (3.0 - difficulty_rating) / 5.0

    tag_signal = 0.0
    if tags:
        tag_scores = [RMP_TAG_SENTIMENT.get(t, 0.0) for t in tags]
        if tag_scores:
            tag_signal = sum(tag_scores) / len(tag_scores)

    score = 0.35 * text_polarity + 0.40 * quality_signal + 0.15 * difficulty_signal + 0.10 * tag_signal
    score = max(-1.0, min(1.0, round(score, 4)))

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

    return score, label


def create_supabase_client():
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
    return create_client(SUPABASE_URL, key)


def _execute_with_retry(query_builder, action_label):
    for attempt in range(MAX_RETRIES):
        try:
            return query_builder.execute()
        except (httpx.RemoteProtocolError, httpx.ConnectError, Exception) as exc:
            if attempt == MAX_RETRIES - 1:
                print(f"[error] {action_label} failed after {MAX_RETRIES} attempts: {exc}")
                raise
            wait_seconds = 2 ** attempt
            print(
                f"[warn] {action_label} failed on attempt {attempt + 1}/{MAX_RETRIES}: {exc}. "
                f"Retrying in {wait_seconds}s..."
            )
            time.sleep(wait_seconds)


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return set()

    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as checkpoint_file:
        return {line.strip() for line in checkpoint_file if line.strip()}


def save_checkpoint(row_id, checkpoint_fh):
    checkpoint_fh.write(f"{row_id}\n")
    checkpoint_fh.flush()


def fetch_all_rmp_chunks(supabase):
    """Paginate through all RMP rag_chunks (Supabase caps at 1000 per request)."""
    all_rows = []
    page_size = 1000
    offset = 0
    while True:
        resp = _execute_with_retry(
            supabase.table("rag_chunks")
            .select("id, text, quality_rating, difficulty_rating, tags, sentiment_score, sentiment_label")
            .eq("source", "ratemyprofessors")
            .range(offset, offset + page_size - 1),
            action_label=f"fetch rows {offset}-{offset + page_size - 1}",
        )
        rows = resp.data
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return all_rows


def main():
    supabase = create_supabase_client()
    processed_ids = load_checkpoint()
    if processed_ids:
        print(f"Loaded checkpoint with {len(processed_ids)} processed rows.")

    rows = fetch_all_rmp_chunks(supabase)
    print(f"Fetched {len(rows)} RMP rag_chunks to backfill.")

    updated = 0
    processed_this_run = 0
    skipped_from_checkpoint = 0
    label_changes = {"same": 0}

    with open(CHECKPOINT_FILE, "a", encoding="utf-8") as checkpoint_fh:
        for index, row in enumerate(rows, start=1):
            row_id = str(row["id"])
            if row_id in processed_ids:
                skipped_from_checkpoint += 1
                continue

            new_score, new_label = detect_sentiment(
                row["text"],
                quality_rating=row.get("quality_rating"),
                difficulty_rating=row.get("difficulty_rating"),
                tags=row.get("tags") or [],
            )

            old_label = row.get("sentiment_label")
            if new_score != row.get("sentiment_score") or new_label != old_label:
                _execute_with_retry(
                    supabase.table("rag_chunks").update({
                        "sentiment_score": new_score,
                        "sentiment_label": new_label,
                    }).eq("id", row["id"]),
                    action_label=f"update row {row_id}",
                )
                updated += 1

                transition = f"{old_label} -> {new_label}"
                if old_label != new_label:
                    label_changes[transition] = label_changes.get(transition, 0) + 1
            else:
                label_changes["same"] += 1

            save_checkpoint(row_id, checkpoint_fh)
            processed_ids.add(row_id)
            processed_this_run += 1

            if processed_this_run % PROGRESS_EVERY == 0:
                print(
                    f"Progress: {index}/{len(rows)} scanned this pass, "
                    f"{processed_this_run} processed this run, "
                    f"{updated} updated, {skipped_from_checkpoint} skipped from checkpoint."
                )

    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)

    print(f"\nBackfill complete: {updated}/{len(rows)} rows updated.")
    print(
        f"Rows processed this run: {processed_this_run}. "
        f"Rows skipped from checkpoint: {skipped_from_checkpoint}."
    )
    print("Label transitions:")
    for k, v in sorted(label_changes.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
