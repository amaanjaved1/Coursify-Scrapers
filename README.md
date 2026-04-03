# 🕷️ Coursify — Scrapers

## 💡 What is Coursify?

**Coursify** is a course-insights platform for Queen's University students. It features course grade distributions, relevant Reddit and RateMyProfessors comments, and also an AI Chatbot.

**This repository** is the **data collection layer**: scheduled and manual jobs that load the Queen's academic calendar, Reddit threads, and RateMyProfessors reviews into **Supabase** (`courses`, `professors`, `rag_chunks`) for the web app and RAG stack.

---

## 🔗 Related repositories

| Repository                                                           | Purpose                                                                                 |
| -------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| [Coursify-WebApp](https://github.com/CoursifyQU/Coursify-WebApp)     | Full stack application                                                                  |
| [Coursify-Scrapers](https://github.com/CoursifyQU/Coursify-Scrapers) | Scheduled data scrapers for the Queen's academic calendar, Reddit, and RateMyProfessors |
| [Coursify-RAG](https://github.com/amaanjaved1/Coursify-RAG)          | Queen's Answers - Our chatbot                                                           |

🌐 [**Live site**](https://www.coursify.ca/)

---

## 🛠️ Tech stack

- **Supabase** (PostgREST client) — `courses`, `professors`, and `rag_chunks`
- **Playwright** + **BeautifulSoup** — Queen's academic calendar
- **PRAW** — Reddit
- **ratemyprofessors-client** — RateMyProfessors
- **transformers** / **torch** — on-device sentiment inference ([`CoursifyQU/student-review-sentiment`](https://huggingface.co/CoursifyQU/student-review-sentiment) for Reddit pipeline)
- **GitHub Actions** — scheduled and manual scraper runs (`.github/workflows/scraper.yaml`)

---

## 📁 Layout

```
apps/scrapers/
  course-scraper.py   # Academic calendar → Supabase `courses`
  reddit-scraper.py   # PRAW → Supabase `rag_chunks`
  rmp-scraper.py      # RateMyProfessors → `professors` + `rag_chunks`
  requirements.txt
.env.example
database.txt          # Notes on `rag_chunks` / placeholder rows
test_sentiment_comparison.py  # Optional local comparison of sentiment models
.github/workflows/scraper.yaml  # CI schedule and jobs
```

---

## ⚙️ How it works

### GitHub Actions (`.github/workflows/scraper.yaml`)

- **Schedule:** `cron: 0 0 * * 0` — every Sunday at 00:00 UTC.
- **Course scraper** runs first, then the **Reddit scraper** (the Reddit job `needs: course-scraper`).
- **RateMyProfessors scraper** is part of the same workflow and is intended to run when you trigger the workflow manually (**Actions → Run scrapers → Run workflow**) or under the extra `if:` conditions in the workflow file (see that file for the exact gate).

Secrets used in CI:

- `SUPABASE_URL`, `SUPABASE_KEY`, `SUPABASE_SERVICE_ROLE_KEY`
- `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET` (Reddit job only)

The Supabase client code prefers `SUPABASE_SERVICE_ROLE_KEY` when set so server-side jobs can write past RLS.

### Scraper overview

**`course-scraper.py`**

- Loads the [Queen's Academic Calendar](https://www.queensu.ca/academic-calendar/) with **Playwright** (Chromium) and parses pages with **BeautifulSoup**.
- Upserts rows into `courses`, preserving manually maintained fields such as `average_gpa` and `average_enrollment` when updating existing codes.
- Optional debugging env vars: `COURSE_SCRAPER_LOG_CODES` (comma-separated codes), `COURSE_SCRAPER_LOG_ROWS`, `COURSE_SCRAPER_LOG_FULL_TEXT`, `COURSE_SCRAPER_LOG_UPSERT` (truthy: `1`, `true`, `yes`, `on`).

**`reddit-scraper.py`**

- Uses **PRAW** against configured Queen's-related subreddits.
- Writes deduplicated chunks to `rag_chunks` with `source: reddit`, using `source_url` to avoid reprocessing.
- Runs **Hugging Face** sentiment analysis on [`CoursifyQU/student-review-sentiment`](https://huggingface.co/CoursifyQU/student-review-sentiment), blended with a simple upvote-based signal for Reddit comments.
- Derives **tags** (difficulty, workload, etc.) from comment text for downstream RAG filters.

**`rmp-scraper.py`**

- Uses the **`ratemyprofessors-client`** library (Queen's institution id is configured in the script).
- Upserts professor metadata and review text into `professors` and `rag_chunks`, with deduplication informed by stored review timestamps.
- Maps free-text course mentions to calendar codes using a strict normalization pass against `courses` from Supabase (unmatched mentions can roll up to `general_course` where appropriate).

Conventions for `general_course`, `general_professor`, and how Reddit vs RMP populate `rag_chunks` are summarized in `database.txt`.

---

## 🚀 Setup & development

1. **Python:** 3.11 (matches CI).

2. **Dependencies:**

   ```bash
   python -m pip install --upgrade pip
   pip install -r apps/scrapers/requirements.txt
   playwright install chromium
   ```

3. **Environment:** Copy `.env.example` to `.env` and fill in values. For local runs that must write like CI, set `SUPABASE_SERVICE_ROLE_KEY`; otherwise `SUPABASE_KEY` is used as a fallback.

4. **Run** (from the repository root):

   ```bash
   python apps/scrapers/course-scraper.py
   python apps/scrapers/reddit-scraper.py
   python apps/scrapers/rmp-scraper.py
   ```

Reddit requires `REDDIT_CLIENT_ID` and `REDDIT_CLIENT_SECRET`. Scrapers that use the sentiment pipeline will download model weights on first run (torch/transformers).

**Optional:** `test_sentiment_comparison.py` at the repo root compares the Coursify sentiment model with a generic SST-2 baseline for ad-hoc evaluation; it is not part of the scheduled pipeline.

---

## 🤝 Contributing

Contributions are welcome.

- 🐛 **Issues** — Open an issue for scraper bugs, schema questions, or workflow changes before large refactors.
- 🔀 **Pull requests** — Keep changes focused; match existing patterns in `apps/scrapers/`.
- 🔐 **Security** — Do not commit Supabase keys or Reddit credentials; use `.env.example` as a template only.
