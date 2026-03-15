"""
test_reccobeats.py
Prueba ReccoBeats con Spotify IDs directamente en /v1/audio-features.
Batches de 5 IDs por request.

Uso: python test_reccobeats.py
"""

import os
import time
import requests
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.reccobeats.com/v1"

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=os.getenv("SPOTIFY_CLIENT_ID"),
    client_secret=os.getenv("SPOTIFY_CLIENT_SECRET")
))

TEST_TRACKS = [
    ("PinkPantheress", "Stateside"),
    ("Harry Styles",   "American Girls"),
    ("Bad Bunny",      "DtMF"),
    ("Addison Rae",    "Fame Is a Gun"),
    ("Harry Styles",   "As It Was"),
]


def get_spotify_ids(tracks: list[tuple]) -> dict:
    """Get Spotify IDs for a list of (artist, track) tuples."""
    id_map = {}
    for artist, track in tracks:
        try:
            r = sp.search(q=f"{artist} {track}", type="track", limit=1)
            items = r["tracks"]["items"]
            if items:
                id_map[items[0]["id"]] = f"{artist} — {track}"
        except Exception as e:
            print(f"  Spotify error for {artist} {track}: {e}")
        time.sleep(0.2)
    return id_map


def get_audio_features_batch(spotify_ids: list[str]) -> dict:
    """Get audio features for up to 5 Spotify IDs at once."""
    try:
        ids_str = ",".join(spotify_ids)
        r = requests.get(
            f"{BASE_URL}/audio-features",
            params={"ids": ids_str},
            timeout=10
        )
        print(f"  Status: {r.status_code}")
        if r.status_code == 200:
            return r.json()
        else:
            print(f"  Response: {r.text[:200]}")
        return {}
    except Exception as e:
        print(f"  Error: {e}")
        return {}


def assign_mood(f: dict) -> str:
    e = f.get("energy", 0.5)
    v = f.get("valence", 0.5)
    t = f.get("tempo", 120)
    if e >= 0.7 and v >= 0.6:   return "party / euphoric"
    elif e >= 0.7 and v < 0.4:  return "intense / aggressive"
    elif e >= 0.5 and t >= 120: return "energetic / workout"
    elif e < 0.4 and v >= 0.6:  return "happy / chill"
    elif e < 0.4 and v < 0.4:   return "sad / melancholic"
    else:                        return "calm / neutral"


print("=== ReccoBeats Audio Features TEST (via Spotify IDs) ===\n")

# Step 1: Get Spotify IDs
print("[1/2] Getting Spotify IDs...")
id_map = get_spotify_ids(TEST_TRACKS)
print(f"  Found {len(id_map)} IDs: {list(id_map.keys())}\n")

# Step 2: Get audio features in batches of 5
print("[2/2] Getting audio features from ReccoBeats...")
ids = list(id_map.keys())
results = get_audio_features_batch(ids[:5])

print(f"\nRaw response: {results}\n")

# Parse results
if isinstance(results, dict) and "content" in results:
    items = results["content"]
elif isinstance(results, list):
    items = results
else:
    items = []

for item in items:
    spotify_url = item.get("href", "")
    sid   = spotify_url.split("/")[-1] if spotify_url else ""
    label = id_map.get(sid, sid or "unknown")
    print(f"── {label}")
    for k in ["danceability","energy","valence","tempo","acousticness","instrumentalness"]:
        if k in item:
            print(f"  {k:20}: {item[k]}")
    print(f"  → Mood: {assign_mood(item)}")
    print()