"""
test_lastfm_tags.py
Test Last.fm tag.getTopTracks — given a mood/context tag, returns top tracks.
No Kafka, no MinIO — just prints raw JSON to console.

Uso: python test_lastfm_tags.py
"""

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
BASE_URL       = "https://ws.audioscrobbler.com/2.0/"

# Mood/context tags — these map directly to visual contexts
# party/club → urban night scene
# chill      → nature, beach, sunset
# sad        → rainy, melancholic scenes
# workout    → action, sport, energy
# romantic   → couple, candles, soft light
# focus      → office, study, minimalist
MOOD_TAGS = [
    "chill", "party", "sad", "workout",
    "romantic", "focus", "happy", "melancholic",
    "summer", "night", "rainy day", "road trip"
]


def get_top_tracks_for_tag(tag: str, limit: int = 10) -> list[dict]:
    try:
        r = requests.get(BASE_URL, params={
            "method":  "tag.getTopTracks",
            "tag":     tag,
            "limit":   limit,
            "api_key": LASTFM_API_KEY,
            "format":  "json",
        }, timeout=10)
        r.raise_for_status()
        data   = r.json()
        tracks = data.get("tracks", {}).get("track", [])
        return [
            {
                "tag":         tag,
                "rank":        t.get("@attr", {}).get("rank"),
                "track_name":  t.get("name"),
                "artist_name": t.get("artist", {}).get("name"),
                "lastfm_url":  t.get("url"),
                "duration":    t.get("duration"),
            }
            for t in tracks
        ]
    except Exception as e:
        print(f"  Error for tag '{tag}': {e}")
        return []


print("=== Last.fm tag.getTopTracks TEST ===\n")

all_results = {}

for tag in MOOD_TAGS:
    tracks = get_top_tracks_for_tag(tag, limit=5)
    all_results[tag] = tracks

    print(f"── #{tag} ({len(tracks)} tracks)")
    for t in tracks:
        print(f"   [{t['rank']}] {t['artist_name']} — {t['track_name']}")
    print()

print("=== Sample raw payload (what goes to MinIO) ===")
sample = all_results.get("chill", [{}])[0]
print(json.dumps(sample, indent=2, ensure_ascii=False))

print(f"\n=== Summary ===")
print(f"  Tags tested  : {len(MOOD_TAGS)}")
print(f"  Total tracks : {sum(len(v) for v in all_results.values())}")
useful = sum(1 for v in all_results.values() if len(v) > 0)
print(f"  Tags with results: {useful}/{len(MOOD_TAGS)}")
print(f"  → {'✓ Useful' if useful >= len(MOOD_TAGS) * 0.8 else '✗ Too many empty tags'}")