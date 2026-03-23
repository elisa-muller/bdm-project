# pip install requests pandas python-dotenv

import os
import time
import json
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
if not LASTFM_API_KEY:
    raise ValueError("Missing LASTFM_API_KEY in .env")

API_URL = "http://ws.audioscrobbler.com/2.0/"

# -----------------------------
# Configuration
# -----------------------------
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

# Broad discovery dimensions
tags = [
    "pop", "rock", "jazz", "electronic", "classical", "hip-hop", "reggae",
    "indie", "metal", "blues", "folk", "soul", "dance", "ambient",
    "techno", "house", "punk", "latin", "rnb", "country"
]

countries = [
    "spain", "united states", "united kingdom", "germany", "france",
    "italy", "japan", "south korea", "brazil", "mexico"
]

# Last.fm docs show paginated methods with page + limit and default limit 50
per_page_limit = 50
chart_pages = 20
tag_pages = 20
geo_pages = 20

sleep_seconds = 0.25

# -----------------------------
# Helpers
# -----------------------------
def call_lastfm(method, extra_params):
    params = {
        "method": method,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        **extra_params
    }

    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def parse_track_item(track, source_type, source_value, page):
    """
    Normalize different Last.fm track payloads into one schema.
    """
    artist_name = None
    artist_mbid = None

    artist_field = track.get("artist")
    if isinstance(artist_field, dict):
        artist_name = artist_field.get("name")
        artist_mbid = artist_field.get("mbid")
    else:
        artist_name = artist_field

    image_url = None
    images = track.get("image", [])
    if isinstance(images, list) and images:
        for img in reversed(images):
            if isinstance(img, dict) and img.get("#text"):
                image_url = img.get("#text")
                break

    streamable = track.get("streamable")
    if isinstance(streamable, dict):
        streamable = streamable.get("fulltrack") or streamable.get("#text")

    return {
        "lastfm_track_name": track.get("name"),
        "lastfm_track_mbid": track.get("mbid"),
        "lastfm_artist_name": artist_name,
        "lastfm_artist_mbid": artist_mbid,
        "lastfm_url": track.get("url"),
        "lastfm_duration": track.get("duration"),
        "lastfm_listeners": track.get("listeners"),
        "lastfm_playcount": track.get("playcount"),
        "lastfm_streamable": streamable,
        "lastfm_rank": (
            track.get("@attr", {}).get("rank")
            if isinstance(track.get("@attr"), dict) else None
        ),
        "lastfm_image_url": image_url,
        "source_type": source_type,     # chart / tag / geo
        "source_value": source_value,   # e.g. pop / spain / global
        "source_page": page
    }

def fetch_chart_tracks(max_pages=20, limit=50):
    rows = []
    for page in range(1, max_pages + 1):
        print(f"[chart] page {page}")
        data = call_lastfm("chart.getTopTracks", {"page": page, "limit": limit})

        tracks = data.get("tracks", {}).get("track", [])
        if not tracks:
            break

        for track in tracks:
            rows.append(parse_track_item(track, "chart", "global", page))

        time.sleep(sleep_seconds)

    return rows

def fetch_tag_tracks(tag, max_pages=20, limit=50):
    rows = []
    for page in range(1, max_pages + 1):
        print(f"[tag={tag}] page {page}")
        data = call_lastfm(
            "tag.getTopTracks",
            {"tag": tag, "page": page, "limit": limit}
        )

        tracks = data.get("tracks", {}).get("track", [])
        if not tracks:
            break

        for track in tracks:
            rows.append(parse_track_item(track, "tag", tag, page))

        time.sleep(sleep_seconds)

    return rows

def fetch_geo_tracks(country, max_pages=20, limit=50):
    rows = []
    for page in range(1, max_pages + 1):
        print(f"[country={country}] page {page}")
        data = call_lastfm(
            "geo.getTopTracks",
            {"country": country, "page": page, "limit": limit}
        )

        tracks = data.get("tracks", {}).get("track", [])
        if not tracks:
            break

        for track in tracks:
            rows.append(parse_track_item(track, "geo", country, page))

        time.sleep(sleep_seconds)

    return rows

# -----------------------------
# Main ingestion
# -----------------------------
all_rows = []

# 1) Global chart
all_rows.extend(fetch_chart_tracks(max_pages=chart_pages, limit=per_page_limit))

# 2) Tags
for tag in tags:
    try:
        all_rows.extend(fetch_tag_tracks(tag, max_pages=tag_pages, limit=per_page_limit))
    except Exception as e:
        print(f"Skipping tag={tag} due to error: {e}")
        time.sleep(2)

# 3) Countries
for country in countries:
    try:
        all_rows.extend(fetch_geo_tracks(country, max_pages=geo_pages, limit=per_page_limit))
    except Exception as e:
        print(f"Skipping country={country} due to error: {e}")
        time.sleep(2)

# -----------------------------
# Save raw + deduplicated
# -----------------------------
df_raw = pd.DataFrame(all_rows)

raw_path = os.path.join(output_dir, "lastfm_tracks_raw.csv")
df_raw.to_csv(raw_path, index=False)

# Deduplicate conservatively by artist + track + source_type + source_value
# so we keep provenance but remove exact duplicates from repeated pages.
df_dedup = df_raw.drop_duplicates(
    subset=["lastfm_artist_name", "lastfm_track_name", "source_type", "source_value"]
).reset_index(drop=True)

dedup_path = os.path.join(output_dir, "lastfm_tracks_dedup.csv")
df_dedup.to_csv(dedup_path, index=False)

# Also create a canonical unique track table for later Spotify matching
df_unique_tracks = df_raw.drop_duplicates(
    subset=["lastfm_artist_name", "lastfm_track_name"]
).reset_index(drop=True)

unique_path = os.path.join(output_dir, "lastfm_tracks_unique.csv")
df_unique_tracks.to_csv(unique_path, index=False)

print(f"\nRaw rows collected: {len(df_raw)}")
print(f"Deduplicated rows (with provenance kept): {len(df_dedup)}")
print(f"Unique artist-track pairs: {len(df_unique_tracks)}")

print(f"Saved raw dataset to: {raw_path}")
print(f"Saved deduplicated dataset to: {dedup_path}")
print(f"Saved unique-track dataset to: {unique_path}")