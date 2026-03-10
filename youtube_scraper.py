import json
import logging
import argparse
import time
import hashlib
from datetime import datetime, timezone
from itertools import islice

import yt_dlp
from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_POPULAR
import db_utils

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CHANNELS = [
    "https://www.youtube.com/@RaffyTulfoInAction",
    "https://www.youtube.com/@BITAGOFFICIAL",
    "https://www.youtube.com/@CongTV",
    "https://www.youtube.com/@ViyCortez",
    "https://www.youtube.com/@IvanaAlawi",
    "https://www.youtube.com/@AlexGonzagaOfficial",
    "https://www.youtube.com/@ZeinabHarake",
    "https://www.youtube.com/@ABSCBNNews",
    "https://www.youtube.com/@gmanews",
    "https://www.youtube.com/@gmapublicaffairs",
    "https://www.youtube.com/@fliptopbattles",
    "https://www.youtube.com/@Wish1075official",
    "https://www.youtube.com/@PinoyBigBrother",
    "https://www.youtube.com/@DocWillieOng",
    "https://www.youtube.com/@TeamLyqa",
    "https://www.youtube.com/@NicoleAlbaYT"
]

def save_data(data, filename="taglishbench.db"):
    """Upserts a dictionary into the SQLite database."""
    db_utils.save_data(data, filename)

def get_recent_videos(channel_url, limit=5):
    """Uses yt-dlp to get the latest video IDs from a YouTube channel."""
    
    # Ensure URL ends with /videos so yt-dlp doesn't just extract the channel overview
    fetch_url = channel_url if channel_url.endswith("/videos") else f"{channel_url}/videos"
    
    ydl_opts = {
        'extract_flat': 'in_playlist',
        'playlistend': limit,
        'quiet': True,
        'no_warnings': True,
    }
    
    videos = []
    logger.info(f"Fetching recent {limit} videos for {fetch_url}...")
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            result = ydl.extract_info(fetch_url, download=False)
            if 'entries' in result:
                for entry in result['entries']:
                    if entry.get('id'):
                        videos.append({
                            'id': entry['id'],
                            'title': entry.get('title', 'Unknown Title')
                        })
    except Exception as e:
        logger.error(f"Error fetching videos for {channel_url}: {e}")
        
    return videos

def scrape_comments(video_tuple, channel_url, output_file, max_comments=1000):
    """Scrapes comments and replies from a specific video."""
    video_id, title = video_tuple['id'], video_tuple['title']
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    logger.info(f"  Scraping comments for video: {title} ({video_id})")
    
    downloader = YoutubeCommentDownloader()
    count = 0
    try:
        comments_generator = downloader.get_comments_from_url(video_url, sort_by=SORT_BY_POPULAR)
        
        # islice allows us to efficiently limit the generator
        last_root_id = None
        for comment in islice(comments_generator, max_comments):
            cid = comment.get('cid')
            if not cid:
                continue
                
            author_name = comment.get('author', 'anonymous')
            timestamp_val = comment.get('time_parsed')
            iso_timestamp = None
            if timestamp_val is not None:
                try:
                    iso_timestamp = datetime.fromtimestamp(timestamp_val, tz=timezone.utc).isoformat()
                except Exception:
                    pass
            
            # YouTube votes might be string or int. Try parsing.
            votes_raw = comment.get('votes', 0)
            score = 0
            if isinstance(votes_raw, (int, float)):
                score = int(votes_raw)
            elif isinstance(votes_raw, str):
                votes_str = votes_raw.lower().replace(',', '').strip()
                if 'k' in votes_str:
                    try:
                        score = int(float(votes_str.replace('k', '')) * 1000)
                    except ValueError:
                        score = 0
                elif 'm' in votes_str:
                    try:
                        score = int(float(votes_str.replace('m', '')) * 1000000)
                    except ValueError:
                        score = 0
                else:
                    try:
                        score = int(votes_str)
                    except ValueError:
                        score = 0

            # Determine origin channel name from URL (e.g., "https://www.youtube.com/@CongTV" -> "@CongTV")
            origin = channel_url.rstrip('/').split('/')[-1] if channel_url else "Unknown"

            is_reply = bool(comment.get('reply', False))
            
            if not is_reply:
                last_root_id = cid
                parent_id = None
                depth = 0
            else:
                parent_id = last_root_id
                depth = 1

            comment_data = {
                "entry_id": f"yt_{cid}",
                "source": "youtube",
                "origin": origin,
                "text": comment.get('text', ''),
                "thread_info": {
                    "thread_id": video_id,
                    "parent_id": parent_id,
                    "depth": depth
                },
                "metadata": {
                    "platform_id": cid,
                    "author_hash": hashlib.sha256(author_name.encode('utf-8')).hexdigest() if author_name else "anonymous",
                    "timestamp": iso_timestamp or datetime.now(timezone.utc).isoformat(),
                    "engagement": {
                        "score": score,
                        "replies": int(comment.get('reply_count', 0)) if str(comment.get('reply_count', 0)).isdigit() else 0
                    }
                },
                "complexity_metrics": {
                    "cmi": None,
                    "m_index": None,
                    "i_index": None
                }
            }
            save_data(comment_data, output_file)
            count += 1
            
            # Add small sleep to be polite, although downloader does some waiting
            if count % 100 == 0:
                time.sleep(1)
                
        logger.debug(f"    Scraped {count} comments from video {video_id}.")
    except Exception as e:
        logger.error(f"Error scraping comments for {video_id}: {e}")
        
def main():
    parser = argparse.ArgumentParser(description="Scrape Taglish comments from YouTube channels.")
    parser.add_argument("--test-run", action="store_true", help="Run a quick test scrape on a single channel.")
    parser.add_argument("--video-url", type=str, help="Scrape comments from a specific video URL.")
    parser.add_argument("--video-limit", type=int, default=5, help="Number of recent videos to fetch per channel.")
    parser.add_argument("--comment-limit", type=int, default=500, help="Max comments to fetch per video.")
    parser.add_argument("--db", type=str, default="taglishbench.db", help="Output SQLite database file.")
    args = parser.parse_args()
    
    if args.test_run:
        args.db = "taglishbench_test.db"
        
    db_utils.init_db(args.db)

    if args.video_url:
        logger.info(f"Fetching info for specific video: {args.video_url}")
        ydl_opts = {'quiet': True, 'no_warnings': True}
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(args.video_url, download=False)
                video_tuple = {'id': info.get('id'), 'title': info.get('title', 'Unknown Title')}
                channel_url = info.get('channel_url', 'Unknown')
                scrape_comments(video_tuple, channel_url, args.db, max_comments=args.comment_limit)
        except Exception as e:
            logger.error(f"Error fetching specific video: {e}")
        return

    if args.test_run:
        channels_to_scrape = ["https://www.youtube.com/@RaffyTulfoInAction"]
        video_limit = 1
        comment_limit = 10
        args.db = "taglishbench_test.db"
        logger.info(f"Running in TEST MODE. Outputting to {args.db}")
    else:
        channels_to_scrape = CHANNELS
        video_limit = args.video_limit
        comment_limit = args.comment_limit

    for channel in channels_to_scrape:
        videos = get_recent_videos(channel, limit=video_limit)
        for video in videos:
            scrape_comments(video, channel, args.db, max_comments=comment_limit)
            time.sleep(2) # Polite sleep between videos
            
    logger.info("Scraping complete.")

if __name__ == "__main__":
    main()
