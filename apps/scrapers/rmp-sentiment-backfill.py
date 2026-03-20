"""
One-shot backfill script: recomputes sentiment_score and sentiment_label for all
existing RMP rag_chunks using distilbert text-only sentiment.

Safe to run multiple times (idempotent).

Usage:
    python apps/scrapers/rmp-sentiment-backfill.py
"""
import os
import time

import httpx
from transformers import pipeline
from supabase import create_client, Client

_sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="CoursifyQU/student-review-sentiment",
    device=-1,
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CHECKPOINT_FILE = os.path.join(SCRIPT_DIR, "backfill_checkpoint.txt")
MAX_RETRIES = int(os.getenv("RMP_BACKFILL_MAX_RETRIES", "5"))
PROGRESS_EVERY = int(os.getenv("RMP_BACKFILL_PROGRESS_EVERY", "500"))


def detect_sentiment(text):
    """
    Text-only sentiment using fine-tuned RoBERTa model trained on 18K+ Queen's student reviews.
    Returns a (-1..1) score and a human-readable label.
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
            .select("id, text, sentiment_score, sentiment_label")
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

            new_score, new_label = detect_sentiment(row["text"])

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
