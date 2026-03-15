"""
test_bluesky_raw.py
Test Bluesky stream — raw data only, no sentiment analysis.
Filters by: artist names from CSV + strong music hashtags.

Instalar: pip install websockets pandas
Uso: python test_bluesky_raw.py --csv path/to/lastfm_tracks.csv
     python test_bluesky_raw.py  (uses default CSV path)
"""

import re
import json
import asyncio
import argparse
import websockets
import pandas as pd
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
MAX_POSTS     = 40

# Strong music hashtags — low noise
MUSIC_HASHTAGS = {
    "nowplaying", "albumreview", "newmusic", "musicrecommendation",
    "np", "nowlistening", "albumoftheday", "songoftheday"
}


def load_artists(csv_path: str, top_n: int = 50) -> set[str]:
    """Load top N artist names from CSV."""
    try:
        df = pd.read_csv(csv_path)
        # Support both spotify and lastfm CSV column names
        col = next((c for c in ["artist_name", "lastfm_artist_name", "artist"]
                    if c in df.columns), None)
        if not col:
            print(f"  Warning: no artist column found in {csv_path}")
            return set()
        top = df[col].value_counts().head(top_n).index.tolist()
        artists = {a.lower() for a in top if isinstance(a, str)}
        print(f"  Loaded {len(artists)} artists from CSV")
        return artists
    except Exception as e:
        print(f"  Warning: could not load CSV: {e}")
        return set()


def is_relevant(text: str, artists: set[str], hashtags: list[str]) -> tuple[bool, str]:
    """Returns (is_relevant, match_reason)."""
    text_lower = text.lower()

    # Check hashtags in post
    post_tags = set(re.findall(r"#(\w+)", text_lower))
    tag_match = post_tags & MUSIC_HASHTAGS
    if tag_match:
        return True, f"hashtag: #{list(tag_match)[0]}"

    # Check artist names
    for artist in artists:
        if len(artist) > 3 and artist in text_lower:
            return True, f"artist: {artist}"

    return False, ""


async def stream(csv_path: str):
    artists = load_artists(csv_path)

    print(f"\n=== Bluesky RAW Stream TEST ===")
    print(f"  Artists loaded : {len(artists)}")
    print(f"  Hashtags       : {MUSIC_HASHTAGS}")
    print(f"  Target posts   : {MAX_POSTS}")
    print(f"  Saving raw JSON payload\n")

    count     = 0
    checked   = 0
    samples   = []

    async with websockets.connect(JETSTREAM_URL) as ws:
        async for raw_msg in ws:
            checked += 1
            try:
                msg = json.loads(raw_msg)
            except:
                continue

            if msg.get("kind") != "commit":
                continue
            commit = msg.get("commit", {})
            if commit.get("operation") != "create":
                continue
            if commit.get("collection") != "app.bsky.feed.post":
                continue

            record = commit.get("record", {})
            text   = record.get("text", "").strip()
            if not text:
                continue

            relevant, reason = is_relevant(text, artists, MUSIC_HASHTAGS)
            if not relevant:
                continue

            count += 1

            # Raw payload — exactly what we'd send to Kafka
            payload = {
                "post_id":    msg.get("did", "") + "/" + commit.get("rkey", ""),
                "created_at": record.get("createdAt", ""),
                "text":       text,
                "lang":       record.get("langs", ["?"])[0] if record.get("langs") else "?",
                "tags":       re.findall(r"#(\w+)", text),
                "match_reason": reason,
                "author_did": msg.get("did", ""),
                "reply":      record.get("reply") is not None,
            }
            samples.append(payload)

            print(f"── [{count}/{MAX_POSTS}] matched via {reason} (lang: {payload['lang']})")
            print(f"   {text[:160]}")
            print(f"   tags: {payload['tags']}")
            print()

            if count >= MAX_POSTS:
                break

    # Stats
    print(f"{'='*55}")
    print(f"  Checked {checked:,} total posts → {count} music matches")
    print(f"  Match rate: {count/checked*100:.2f}%")
    print()

    by_reason = {}
    for p in samples:
        r = p["match_reason"].split(":")[0]
        by_reason[r] = by_reason.get(r, 0) + 1
    print("  Match breakdown:")
    for r, c in by_reason.items():
        print(f"    {r:12}: {c} posts")

    by_lang = {}
    for p in samples:
        by_lang[p["lang"]] = by_lang.get(p["lang"], 0) + 1
    print("\n  Languages:")
    for lang, c in sorted(by_lang.items(), key=lambda x: -x[1])[:8]:
        print(f"    {lang:6}: {c}")

    print(f"\n  Sample raw payload (last post):")
    print(json.dumps(samples[-1], indent=2, ensure_ascii=False))


parser = argparse.ArgumentParser()
parser.add_argument("--csv", default="data/lastfm_tracks.csv")
args = parser.parse_args()

asyncio.run(stream(args.csv))