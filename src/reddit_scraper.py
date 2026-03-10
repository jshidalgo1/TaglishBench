import json
import logging
import argparse
import time
import hashlib
import requests
from datetime import datetime, timezone
import db_utils

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

SUBREDDITS = [
    "Philippines",
    "CasualPH",
    "phcareers",
    "AskPH",
    "OffMyChestPH",
    "adultingph",
    "relationship_advicePH",
    "studentsph",
    "PinoyProgrammer",
    "phinvest",
    "ChikaPH",
    "peyups",
    "dlsu",
    "Pinoy",
    "PHGamers"
]

def save_data(data, db_path="taglishbench.db"):
    """Upserts a dictionary into the SQLite database."""
    db_utils.save_data(data, db_path)

def get_posts_by_sort(subreddit, sort_type="hot", limit=5):
    """Fetches posts from a subreddit by sort type."""
    if sort_type == "hot":
        url = f"https://old.reddit.com/r/{subreddit}.json"
    else:
        url = f"https://old.reddit.com/r/{subreddit}/{sort_type}.json"
        
    params = {'limit': limit}
    if sort_type == "top":
        params['t'] = 'all'
        
    headers = {'User-Agent': USER_AGENT}

    logger.info(f"Fetching {limit} {sort_type} posts from r/{subreddit}...")
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        posts = data.get('data', {}).get('children', [])
        return [p['data'] for p in posts]
    except Exception as e:
        logger.error(f"Error fetching {sort_type} posts for r/{subreddit}: {e}")
        return []

def get_mixed_posts(subreddit):
    """Fetches exactly 4 hot, 3 top, and 3 new posts for a subreddit."""
    logger.info(f"Fetching mixed post selection (4 hot, 3 top, 3 new) for r/{subreddit}...")
    hot = get_posts_by_sort(subreddit, "hot", 4)
    top = get_posts_by_sort(subreddit, "top", 3)
    new = get_posts_by_sort(subreddit, "new", 3)
    
    combined = hot + top + new
    
    # Deduplicate in case a post happens to be in multiple categories
    unique_posts = []
    seen = set()
    for p in combined:
        if p.get('id') and p['id'] not in seen:
            seen.add(p['id'])
            unique_posts.append(p)
            
    return unique_posts

def process_comment_tree(comments_list, thread_id, origin, output_file, parent_id=None, depth=0):
    """Recursively parses Reddit's comment tree into the target schema."""
    count = 0
    for item in comments_list:
        if item.get('kind') == 'more':
            # Skip "load more comments" stubs for now to keep it simple and avoid excess API calls
            continue
            
        comment_data = item.get('data', {})
        
        # Base cases where valid data might not exist
        if not comment_data or 'body' not in comment_data:
            continue
        
        author = comment_data.get('author', 'anonymous')
        if author == '[deleted]':
            author = 'anonymous'
        
        timestamp_val = comment_data.get('created_utc')
        iso_timestamp = None
        if timestamp_val is not None:
            try:
                iso_timestamp = datetime.fromtimestamp(timestamp_val, tz=timezone.utc).isoformat()
            except Exception:
                pass

        cid = comment_data.get('id', '')
        score = comment_data.get('score', 0)
        
        structured_data = {
            "entry_id": f"reddit_{cid}",
            "source": "reddit",
            "origin": origin,
            "text": comment_data.get('body', ''),
            "thread_info": {
                "thread_id": thread_id,
                "parent_id": parent_id,
                "depth": depth
            },
            "metadata": {
                "platform_id": cid,
                "author_hash": hashlib.sha256(author.encode('utf-8')).hexdigest() if author else "anonymous",
                "timestamp": iso_timestamp or datetime.now(timezone.utc).isoformat(),
                "engagement": {
                    "score": score,
                    "replies": 0 # Not easily obtainable statically for replies, we derive from depth tree later
                }
            },
            "complexity_metrics": {
                "cmi": None,
                "m_index": None,
                "i_index": None
            }
        }
        
        if cid: # Ensure data integrity
            save_data(structured_data, output_file)
            count += 1
            
        # Process replies recursively
        replies = comment_data.get('replies')
        if replies and isinstance(replies, dict):
            reply_children = replies.get('data', {}).get('children', [])
            if reply_children:
                count += process_comment_tree(reply_children, thread_id, origin, output_file, parent_id=cid, depth=depth + 1)
                
    return count

def scrape_thread(post_id, subreddit, output_file):
    """Fetches and parses a specific post and its comments."""
    url = f"https://old.reddit.com/r/{subreddit}/comments/{post_id}.json"
    headers = {'User-Agent': USER_AGENT}
    
    logger.info(f"  Scraping thread: {post_id}")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if not data or len(data) < 2:
            return
            
        post_data = data[0]['data']['children'][0]['data']
        comments_list = data[1]['data']['children']
        
        origin = f"r/{subreddit}"
        
        # Save the main post body as a top-level item (depth = 0)
        thread_id = post_data.get('id')
        author = post_data.get('author', 'anonymous')
        if author == '[deleted]': author = 'anonymous'
        
        timestamp_val = post_data.get('created_utc')
        iso_timestamp = None
        if timestamp_val is not None:
            try:
                iso_timestamp = datetime.fromtimestamp(timestamp_val, tz=timezone.utc).isoformat()
            except Exception:
                pass
                
        # Only save if there's actual text
        post_text = post_data.get('selftext', '')
        if post_text:
            post_record = {
                "entry_id": f"reddit_{thread_id}",
                "source": "reddit",
                "origin": origin,
                "text": post_text,
                "thread_info": {
                    "thread_id": thread_id,
                    "parent_id": None,
                    "depth": 0
                },
                "metadata": {
                    "platform_id": thread_id,
                    "author_hash": hashlib.sha256(author.encode('utf-8')).hexdigest() if author else "anonymous",
                    "timestamp": iso_timestamp or datetime.now(timezone.utc).isoformat(),
                    "engagement": {
                        "score": post_data.get('score', 0),
                        "replies": post_data.get('num_comments', 0)
                    }
                },
                "complexity_metrics": {
                    "cmi": None,
                    "m_index": None,
                    "i_index": None
                }
            }
            save_data(post_record, output_file)
            
        # Parse the comments (depth >= 1)
        count = process_comment_tree(comments_list, thread_id, origin, output_file, parent_id=thread_id, depth=1)
        logger.debug(f"    Scraped {count} items from thread {thread_id}.")
        
        # Rate limit safety
        time.sleep(1.5)
        
    except Exception as e:
        logger.error(f"Error scraping thread {post_id}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Scrape Taglish discussions from Reddit JSON endpoints.")
    parser.add_argument("--test-run", action="store_true", help="Run a quick test scrape on a single subreddit.")
    parser.add_argument("--subreddits", type=str, nargs='+', help="Specific subreddits to scrape.", default=[])
    parser.add_argument("--post-limit", type=int, default=10, help="Number of front-page posts to fetch per subreddit.")
    parser.add_argument("--db", type=str, default="taglishbench.db", help="Output SQLite database file.")
    args = parser.parse_args()
    
    if args.test_run:
        args.db = "taglishbench_test.db"
        
    db_utils.init_db(args.db)

    if args.test_run:
        targets = ["Philippines"]
        post_limit = 2
        args.db = "taglishbench_test.db"
        logger.info(f"Running in TEST MODE. Outputting to {args.db}")
    else:
        targets = args.subreddits if args.subreddits else SUBREDDITS
        post_limit = args.post_limit

    for sub in targets:
        if args.test_run:
            posts = get_mixed_posts(sub)[:post_limit]
        else:
            posts = get_mixed_posts(sub)
            
        for post in posts:
            post_id = post.get('id')
            if post_id:
                scrape_thread(post_id, sub, args.db)
            time.sleep(2) # Polite sleep between different posts
            
    logger.info("Scraping complete.")

if __name__ == "__main__":
    main()
