# decisions_log.md

This document serves as a reference log for the design and implementation decisions made during the construction of the TaglishBench dataset filtering pipeline. The goal of this pipeline is to extract high-quality, authentic Taglish (code-switched Tagalog and English) from raw scraped internet data.

## Data Sources & Scraping Strategy
*   **Decision:** Target YouTube comments and Reddit threads using a mixed-selection scraping strategy.
*   **Rationale:** 
    *   **Authenticity:** Platforms like Reddit and YouTube host deep, nested comment trees where users naturally code-switch and use raw, unfiltered "internet-speak".
    *   **Mixed Selection:** Instead of just scraping the most "popular" or "hot" threads, the scrapers pull a mix of content (e.g., latest, popular, random for YouTube; hot, top, new for Reddit). This ensures the dataset captures diverse linguistic phenomena rather than just echo-chamber viral posts.

## Database Schema & Storage
*   **Decision:** Utilize a unified SQLite database (`taglishbench.db`) with a strict conceptual schema and `UPSERT` deduplication.
*   **Rationale:** 
    *   **Schema Design:** All platform data is normalized into a standard JSON-like schema tracking `entry_id`, `source`, `origin`, `text`, `thread_info` (depth, parent), and `metadata`. This allows seamless downstream analysis across completely completely different platforms.
    *   **Deduplication:** By tracking unique entry IDs and using SQLite `UPSERT`, repeated scrape runs on the same videos/subreddits will update engagement scores (likes/upvotes) without duplicating the text payloads in the dataset.

## Initial Heuristic Quality Filtering
*   **Decision:** Apply rigid, rule-based heuristics (`word_count >= 10`, `engagement_score >= 1`, `alpha_ratio >= 0.70`, `has_url == 0`) before any advanced NLP filtering.
*   **Rationale:** 
    *   **Noise Reduction:** Scraped datasets contain massive amounts of non-conversational clutter (e.g., "True", emoji spam, bot links).
    *   **Efficiency:** Before running computationally expensive LLMs or FastText models, these cheap mathematical rules immediately drop unengaged, short, or spammy text, saving time and compute resources.

## Taglish Filtering Pipeline Architecture

We decided on a two-pass filtering approach rather than a single complex step. 

### Pass 1: Coarse Filtering with FastText
*   **Decision:** We are using Meta's `fasttext` language classification model (`lid.176.bin`).
*   **Rationale:** 
    *   **Speed & Efficiency:** FastText is extremely fast and can process hundreds of thousands of rows locally on CPU in seconds.
    *   **Coarse Sorting:** The raw scraped data contains spam and content in completely unrelated languages (e.g., Spanish, Indonesian). Before applying more expensive or computationally heavy classification methods, we need a fast way to trim the fat.
    *   **Methodology:** We extract the top predicted languages and their confidence levels for each sentence. We keep texts where either Tagalog (`tl`) or English (`en`) are strongly represented, and discard texts that are overwhelmingly classified as unrelated languages. While FastText often struggles with the direct concept of "Taglish" (often assigning it a mix of `tl` and `en` or misclassifying slang), identifying the presence of at least *some* `tl` or `en` is mathematically fast and sufficient for a first-pass filter.

### Pass 2: Fine-Grained Zero-Shot Classification
*   **Decision:** We implemented an LLM-based zero-shot classifier using local inference via Ollama and `gemma2:9b`.
*   **Rationale:**
    *   **Complexity of Code-Switching:** Hardcoded dictionary methods fail to capture the nuance, slang, morphological changes, and true alternating syntax of online Taglish. Large Language Models natively understand context and code-switching far better than traditional NLP heuristics.
    *   **Cost & Privacy:** Rather than using paid APIs (Gemini/OpenAI) where data leaves the machine, or hitting deployed Cloud endpoints, we opted for local inference. This guarantees privacy and zero marginal cost for bulk dataset classification, taking advantage of local Apple Silicon hardware.
    *   **Prompt Design:** We use a strict zero-shot prompt asking the model to classify text strictly as "English", "Tagalog", or "Taglish", parsing the exact response category.
