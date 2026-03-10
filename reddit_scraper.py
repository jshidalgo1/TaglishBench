import os
import time
import json
import logging
import argparse
import hashlib
from datetime import datetime, timezone
from dotenv import load_dotenv
import praw
from prawcore.exceptions import ResponseException

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

SUBREDDITS = [
    "Philippines", "CasualPH", "pinoy",
    "adultingph", "phcareers", "buhaydigital", "BPOinPH",
    "phinvest", "PHCreditCards", "taxPH",
    "OffMyChestPH", "ChikaPH", "AlasFeels", "relasyon",
    "filipuns", "2philippines4u"
]

def init_reddit():
    """Initializes and returns a PRAW Reddit instance."""
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    user_agent = os.getenv("REDDIT_USER_AGENT")

    if not client_id or not client_secret or not user_agent:
        logger.error("Reddit API credentials missing in .env file.")
        raise ValueError("Missing Reddit API credentials in .env")

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )
    return reddit

def save_data(data, filename="reddit_data.jsonl"):
    """Appends a dictionary to a JSONL file."""
    try:
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False) + '\n')
    except Exception as e:
        logger.error(f"Error saving data: {e}")

def extract_comments(submission, max_comments=None):
    """
    Extracts comments from a submission.
    Flattens the comment tree and returns a list of dictionaries.
    """
    comments_data = []
    try:
        # replace_more(limit=0) removes the MoreComments objects. 
        # For deeper scraping, you might increase the limit, but it slows down the process significantly.
        submission.comments.replace_more(limit=0)
        
        count = 0
        for comment in submission.comments.list():
            if max_comments and count >= max_comments:
                break
            
            # Skip deleted/removed comments
            if not comment.author or comment.body in ["[deleted]", "[removed]"]:
                continue

            author_name = comment.author.name if comment.author else None
            # parent_id is like 't3_xxx' (post) or 't1_xxx' (comment)
            raw_parent_id = comment.parent_id if comment.parent_id else ""
            clean_parent_id = raw_parent_id.split('_')[1] if '_' in raw_parent_id else raw_parent_id
            
            comment_dict = {
                "entry_id": f"red_{comment.id}",
                "source": "reddit",
                "origin": submission.subreddit.display_name,
                "text": comment.body,
                "thread_info": {
                    "thread_id": submission.id,
                    "parent_id": clean_parent_id,
                    "depth": getattr(comment, 'depth', 1)
                },
                "metadata": {
                    "platform_id": comment.id,
                    "author_hash": hashlib.sha256(author_name.encode('utf-8')).hexdigest() if author_name else "anonymous",
                    "timestamp": datetime.fromtimestamp(comment.created_utc, tz=timezone.utc).isoformat(),
                    "engagement": {
                        "score": comment.score,
                        "replies": 0
                    }
                },
                "complexity_metrics": {
                    "cmi": None,
                    "m_index": None,
                    "i_index": None
                }
            }
            comments_data.append(comment_dict)
            count += 1
            
    except Exception as e:
        logger.error(f"Error extracting comments for post {submission.id}: {e}")
        
    return comments_data

def scrape_subreddit(reddit, subreddit_name, limit=10, output_file="reddit_data.jsonl", max_comments_per_post=None):
    """Scrapes hot and top posts from a specific subreddit."""
    logger.info(f"Starting scrape for r/{subreddit_name}...")
    try:
        subreddit = reddit.subreddit(subreddit_name)
        
        # We can iterate through different categories. Let's do 'hot' and 'top' (of the year)
        categories = [
            ("hot", subreddit.hot(limit=limit)),
            ("top_year", subreddit.top(time_filter="year", limit=limit))
        ]
        
        for cat_name, posts in categories:
            logger.info(f"  Scraping {cat_name} posts...")
            for post in posts:
                # Skip stickied posts if desired, or skip [deleted]/[removed]
                if post.selftext in ["[deleted]", "[removed]"]:
                    continue
                    
                author_name = post.author.name if post.author else None
                post_data = {
                    "entry_id": f"red_{post.id}",
                    "source": "reddit",
                    "origin": subreddit_name,
                    "text": f"{post.title}\n\n{post.selftext}".strip(),
                    "thread_info": {
                        "thread_id": post.id,
                        "parent_id": None,
                        "depth": 0
                    },
                    "metadata": {
                        "platform_id": post.id,
                        "author_hash": hashlib.sha256(author_name.encode('utf-8')).hexdigest() if author_name else "anonymous",
                        "timestamp": datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
                        "engagement": {
                            "score": post.score,
                            "replies": post.num_comments
                        }
                    },
                    "complexity_metrics": {
                        "cmi": None,
                        "m_index": None,
                        "i_index": None
                    }
                }
                
                # Save post data
                save_data(post_data, output_file)
                
                # Fetch and save comments
                comments = extract_comments(post, max_comments=max_comments_per_post)
                for comment in comments:
                    save_data(comment, output_file)
                    
                logger.debug(f"    Scraped post {post.id} with {len(comments)} comments.")
                time.sleep(1) # Polite pause between API heavy requests (though PRAW handles rate limits)
                
    except ResponseException as e:
        if e.response.status_code == 429:
            logger.warning(f"Rate limited! Sleeping for 60 seconds... Details: {e}")
            time.sleep(60)
        else:
            logger.error(f"API Error scraping r/{subreddit_name}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error scraping r/{subreddit_name}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Scrape Taglish data from Reddit.")
    parser.add_argument("--test-run", action="store_true", help="Run a quick test scrape on a single subreddit.")
    parser.add_argument("--post-limit", type=int, default=100, help="Number of posts to fetch per category per subreddit.")
    parser.add_argument("--output", type=str, default="reddit_data.jsonl", help="Output JSONL file name.")
    args = parser.parse_args()

    try:
        reddit = init_reddit()
        logger.info(f"Authenticated as {reddit.user.me()} (if user script) or read-only mode.")
    except Exception as e:
        logger.error(f"Failed to initialize Reddit client. Please check .env file. Error: {e}")
        return

    # Clear output file if it exists, or let it append? Better to let it append, 
    # but for a test run we might want a clean slate.
    if args.test_run:
        args.output = "reddit_test_data.jsonl"
        subreddits_to_scrape = ["CasualPH"]
        limit = 2
        max_comments = 10
        logger.info(f"Running in TEST MODE. Outputting to {args.output}")
    else:
        subreddits_to_scrape = SUBREDDITS
        limit = args.post_limit
        max_comments = None

    for sub in subreddits_to_scrape:
        scrape_subreddit(reddit, sub, limit=limit, output_file=args.output, max_comments_per_post=max_comments)
        
    logger.info("Scraping complete.")

if __name__ == "__main__":
    main()
