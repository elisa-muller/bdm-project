import csv
import json
import time
import random
import os
import subprocess
from dotenv import load_dotenv
from googleapiclient.discovery import build

# --- CONFIG ---
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")
CSV_PATH = "ingestion/spotify_batch/data/lastfm_tracks_unique.csv"
CACHE_PATH = "ingestion/spotify_batch/data/youtube_video_ids.json"
MAX_COMMENTS_PER_TRACK = 50
TOP_N_TRACKS = 100

youtube = build("youtube", "v3", developerKey=API_KEY)

# --- PHASE 1: Load tracks ---
def load_tracks(csv_path):
    tracks = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row["lastfm_playcount"] or not row["lastfm_track_name"]:
                continue
            tracks.append({
                "track": row["lastfm_track_name"],
                "artist": row["lastfm_artist_name"],
                "playcount": int(row["lastfm_playcount"])
            })
    return tracks

# --- PHASE 2: Resolve video IDs via yt-dlp (no quota!) ---
def resolve_video_id(artist, track):
    query = f"{artist} - {track} official"
    try:
        result = subprocess.run(
            ["yt-dlp", f"ytsearch1:{query}", "--get-id", "--no-playlist"],
            capture_output=True, text=True, timeout=15
        )
        video_id = result.stdout.strip()
        return video_id if video_id else None
    except Exception as e:
        print(f"  yt-dlp error for {artist} - {track}: {e}")
        return None

def get_video_ids(tracks, cache_path):
    # Load cache if exists
    if os.path.exists(cache_path):
        print("Loading video IDs from cache...")
        with open(cache_path) as f:
            cache = json.load(f)
        # Check if any tracks are missing from cache
        missing = [t for t in tracks 
                  if f"{t['artist']}||{t['track']}" not in cache]
        if not missing:
            print(f"All {len(tracks)} tracks found in cache!")
            return cache
        print(f"Found {len(missing)} tracks missing from cache, resolving...")
        tracks_to_resolve = missing
    else:
        cache = {}
        tracks_to_resolve = tracks

    print(f"Resolving {len(tracks_to_resolve)} video IDs via yt-dlp...")
    for i, t in enumerate(tracks_to_resolve):
        key = f"{t['artist']}||{t['track']}"
        video_id = resolve_video_id(t["artist"], t["track"])
        if video_id:
            cache[key] = video_id
            print(f"  [{i+1}/{len(tracks_to_resolve)}] {t['artist']} - {t['track']} → {video_id}")
        else:
            print(f"  [{i+1}/{len(tracks_to_resolve)}] Not found: {t['artist']} - {t['track']}")
        time.sleep(0.5)  # be polite

    # Save updated cache
    with open(cache_path, "w") as f:
        json.dump(cache, f, indent=2)
    print(f"Saved {len(cache)} video IDs to cache")
    return cache

# --- PHASE 3: Fetch comments (uses quota: 1 unit per call) ---
def fetch_comments(video_id, max_results=50):
    comments = []
    try:
        res = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=min(max_results, 100),
            order="time"
        ).execute()
        for item in res.get("items", []):
            s = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "comment_id": item["id"],
                "video_id": video_id,
                "author": s["authorDisplayName"],
                "text": s["textDisplay"],
                "likes": s["likeCount"],
                "published_at": s["publishedAt"],
                "updated_at": s["updatedAt"]
            })
    except Exception as e:
        print(f"  Could not fetch comments for {video_id}: {e}")
    return comments

# --- PHASE 4: Simulate stream ---
def simulate_stream(tracks, video_cache):
    total_playcount = sum(t["playcount"] for t in tracks)
    
    all_events = []
    for t in tracks:
        key = f"{t['artist']}||{t['track']}"
        video_id = video_cache.get(key)
        if not video_id:
            continue
        
        # Scale comments by playcount
        weight = t["playcount"] / total_playcount
        n_comments = max(1, int(weight * MAX_COMMENTS_PER_TRACK * len(tracks)))
        n_comments = min(n_comments, MAX_COMMENTS_PER_TRACK)
        
        print(f"  Fetching {n_comments} comments for {t['artist']} - {t['track']}...")
        comments = fetch_comments(video_id, max_results=n_comments)
        for c in comments:
            c["track"] = t["track"]
            c["artist"] = t["artist"]
            c["lastfm_playcount"] = t["playcount"]
            all_events.append(c)
    
    # Shuffle to mix artists
    random.shuffle(all_events)
    
    print(f"\n--- Starting stream simulation ({len(all_events)} events) ---\n")
    for event in all_events:
        print(json.dumps(event, indent=2))
        print("---")
        time.sleep(random.uniform(0.5, 2.0))

# --- MAIN ---
if __name__ == "__main__":
    tracks = load_tracks(CSV_PATH)
    print(f"Loaded {len(tracks)} tracks")
    
    # Sort by playcount and keep top N
    tracks = sorted(tracks, key=lambda x: x["playcount"], reverse=True)
    tracks = tracks[:TOP_N_TRACKS]
    print(f"Using top {len(tracks)} tracks by playcount")
    print(f"Top 5: {[(t['artist'], t['track']) for t in tracks[:5]]}")
    
    # Phase 2: resolve video IDs (no quota used)
    video_cache = get_video_ids(tracks, CACHE_PATH)
    
    # Phase 3+4: fetch comments and stream (uses quota)
    # Comment this out today, uncomment tomorrow!
    # simulate_stream(tracks, video_cache)