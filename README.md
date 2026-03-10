# TaglishBench Dataset Collection

This repository contains the data collection pipeline for **TaglishBench**, a multi-task benchmark dataset designed for evaluating Large Language Models (LLMs) on Tagalog-English code-switched language (Taglish).

The pipeline is built to scrape real, authentic Taglish conversational data from public online sources (YouTube and Reddit) and store it cleanly into an SQLite database for downstream processing, quality filtering, and LLM benchmarking.

## Architecture

The data collection relies on three main components:
1. **Scrapers**: Python scripts targeting specific platforms to extract deeply nested conversation threads.
2. **Database Utilities**: A unified SQLite database (`taglishbench.db`) that handles schema alignment, deduplication, and relationship mapping (parent/child thread depth).
3. **Analysis & Filtering**: Scripts to analyze the raw text, apply "Gold Standard" heuristic filters, and visualize dataset composition.

## Prerequisites

Before running any scripts, ensure your environment has the required dependencies installed:

```bash
pip install yt-dlp youtube-comment-downloader requests pandas matplotlib seaborn
```

## Usage

### 1. YouTube Scraper (`youtube_scraper.py`)
Scrapes videos and comments from a predefined list of popular Filipino YouTube channels. It uses `yt-dlp` to fetch the latest video IDs and `youtube-comment-downloader` to pull the comment trees.

**Run default scrape (5 videos per channel, up to 500 comments each):**
```bash
python youtube_scraper.py
```

**Run a test scrape (1 channel, 1 video, 10 comments):**
```bash
python youtube_scraper.py --test-run
```

**Scrape a specific video by URL:**
```bash
python youtube_scraper.py --video-url "https://www.youtube.com/watch?v=XYZ"
```

**Override limits:**
```bash
python youtube_scraper.py --video-limit 10 --comment-limit 1000
```

### 2. Reddit Scraper (`reddit_scraper.py`)
Scrapes posts and deep comment threads from popular Filipino subreddits using Reddit's public `.json` endpoints (bypassing the need for an official API key). It respects rate limits and maps the nested thread structure natively.

**Run default scrape (10 hot posts from default subreddits):**
```bash
python reddit_scraper.py
```

**Run a test scrape (2 posts from r/Philippines):**
```bash
python reddit_scraper.py --test-run
```

**Target specific subreddits with custom limits:**
```bash
python reddit_scraper.py --subreddits OffMyChestPH CasualPH --post-limit 20
```

### 3. Database Storage (`db_utils.py`)
Both scrapers automatically route their processed output through `db_utils.py` to be saved in `taglishbench.db`.

* **Deduplication**: The database uses an `UPSERT` methodology. If you scrape the same video or subreddit multiple times, it will simply update the engagement scores (likes/upvotes) without creating duplicated text entries.
* **Schema Mapping**: All entries are mapped to the core TaglishBench schema, tracking `source`, `origin`, `thread_id`, `parent_id`, `depth`, `author_hash`, and engagement metrics.

### 4. Dataset Quality Analysis (`analyze_dataset.py`)
Because massive scrapes include noise (short replies like "True", bots, emoji spam), this script queries the SQLite database, applies rigorous "Gold Standard" filtering heuristics, and generates visualizations.

**Run the analysis:**
```bash
python analyze_dataset.py
```

**Current Filtering Heuristics:**
*   `word_count >= 10`: Drops short, non-conversational clutter.
*   `engagement_score >= 1`: Drops unengaged or heavily downvoted/troll comments.
*   `alpha_ratio >= 0.70`: Ensures the text is mostly alphanumeric (drops emoji/symbol spam).
*   `has_url == 0`: Drops self-promoters and sales bots.

The script will output three `.png` files visualizing comment length by platform, total volume versus viable "Gold Standard" retention, and engagement distribution.

## License & Ethics
The data gathered by these scripts is intended for **Non-Commercial Research** to benchmark LLMs. Anonymization via `author_hash` mapping is already integrated during the scrape phase. Any subsequent public datasets generated directly from this pipeline should be accompanied by further PII scrubbing and released under a restrictive open-research license (e.g., CC BY-NC 4.0).
