"""
test_youtube_comments.py
Test rápido — comenta + metadata de YouTube para canciones del CSV.
Sin MinIO, sin Kafka — solo muestra el raw data en consola.

Uso: python test_youtube_comments.py
"""

import os
import json
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

youtube = build("youtube", "v3", developerKey=os.getenv("YOUTUBE_API_KEY"))

# Test con unas pocas canciones de vuestro CSV
TEST_TRACKS = [
    ("PinkPantheress", "Stateside"),
    ("Harry Styles",   "American Girls"),
    ("Bad Bunny",      "DtMF"),
    ("Addison Rae",    "Fame Is a Gun"),
]


def search_video(artist: str, track: str) -> dict | None:
    try:
        r = youtube.search().list(
            q=f"{artist} {track} official",
            part="snippet", type="video",
            maxResults=1, videoCategoryId="10"
        ).execute()
        items = r.get("items", [])
        if not items:
            return None
        item = items[0]
        return {"video_id":    item["id"]["videoId"],
                "video_title": item["snippet"]["title"],
                "channel":     item["snippet"]["channelTitle"]}
    except Exception as e:
        print(f"  Search error: {e}")
        return None


def get_video_metadata(video_id: str) -> dict:
    try:
        r = youtube.videos().list(
            part="snippet,statistics", id=video_id
        ).execute()
        items = r.get("items", [])
        if not items:
            return {}
        s = items[0]["snippet"]
        st = items[0].get("statistics", {})
        return {
            "tags":          s.get("tags", [])[:10],
            "description":   s.get("description", "")[:200],
            "view_count":    st.get("viewCount"),
            "like_count":    st.get("likeCount"),
            "comment_count": st.get("commentCount"),
        }
    except Exception as e:
        print(f"  Metadata error: {e}")
        return {}


def get_comments(video_id: str, max_comments: int = 10) -> list[dict]:
    try:
        r = youtube.commentThreads().list(
            part="snippet", videoId=video_id,
            maxResults=max_comments, order="relevance"
        ).execute()
        return [
            {
                "text":      i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                "likes":     i["snippet"]["topLevelComment"]["snippet"]["likeCount"],
                "published": i["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                "lang_hint": i["snippet"]["topLevelComment"]["snippet"].get("authorDisplayName", ""),
            }
            for i in r.get("items", [])
        ]
    except Exception as e:
        print(f"  Comments error (disabled?): {e}")
        return []


print("=== YouTube Comments + Metadata TEST ===\n")

for artist, track in TEST_TRACKS:
    print(f"── {artist} — {track}")

    video = search_video(artist, track)
    if not video:
        print("  ✗ Not found\n")
        continue
    print(f"  Video   : {video['video_title'][:60]}")
    print(f"  Channel : {video['channel']}")

    meta = get_video_metadata(video["video_id"])
    if meta:
        print(f"  Views   : {int(meta.get('view_count') or 0):,}")
        print(f"  Likes   : {int(meta.get('like_count') or 0):,}")
        print(f"  Comments: {int(meta.get('comment_count') or 0):,}")
        print(f"  Tags    : {meta.get('tags', [])[:5]}")

    comments = get_comments(video["video_id"], max_comments=10)
    print(f"\n  Top {len(comments)} comments:")
    for i, c in enumerate(comments, 1):
        print(f"  [{i}] ({c['likes']} likes) {c['text'][:120]}")

    # Raw payload
    payload = {
        "track":    f"{artist} - {track}",
        "video_id": video["video_id"],
        **meta,
        "comments": comments,
    }
    print(f"\n  Raw JSON size: {len(json.dumps(payload))} bytes")
    print()

print("=== Done ===")