# pip install pandas spotipy python-dotenv

import os
import re
import time
import pandas as pd
import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials

# =========================
# 1. Load env + auth
# =========================
load_dotenv()

client_id = os.getenv("SPOTIFY_CLIENT_ID")
client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

if not client_id or not client_secret:
    raise ValueError("Missing SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET in .env")

sp = spotipy.Spotify(
    auth_manager=SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    )
)

# =========================
# 2. Paths
# =========================
input_csv = "data/lastfm_tracks_unique.csv"
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

output_csv = os.path.join(output_dir, "lastfm_spotify_enriched.csv")
unmatched_csv = os.path.join(output_dir, "lastfm_unmatched.csv")
checkpoint_csv = os.path.join(output_dir, "lastfm_spotify_checkpoint.csv")

# =========================
# 3. Load data
# =========================
df = pd.read_csv(input_csv)

# Optional: take only a subset while testing
# df = df.head(200)

# =========================
# 4. Helpers
# =========================
def normalize_text(text):
    if pd.isna(text):
        return ""
    text = str(text).strip().lower()

    # remove bracketed additions like (Remastered), [Live], etc.
    text = re.sub(r"\(.*?\)", "", text)
    text = re.sub(r"\[.*?\]", "", text)

    # remove common suffixes
    text = re.sub(r"\b(remaster(ed)?|live|mono|stereo|version|edit|radio edit|deluxe)\b", "", text)

    # keep alphanumerics and spaces
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text

def token_overlap(a, b):
    set_a = set(normalize_text(a).split())
    set_b = set(normalize_text(b).split())
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / max(len(set_a), len(set_b))

def score_candidate(lastfm_artist, lastfm_track, spotify_artist, spotify_track):
    artist_score = token_overlap(lastfm_artist, spotify_artist)
    track_score = token_overlap(lastfm_track, spotify_track)
    return 0.6 * artist_score + 0.4 * track_score

def search_spotify_best_match(artist_name, track_name):
    artist_name = "" if pd.isna(artist_name) else str(artist_name)
    track_name = "" if pd.isna(track_name) else str(track_name)

    # Most precise query first
    queries = [
        f'track:"{track_name}" artist:"{artist_name}"',
        f'{track_name} {artist_name}',
        f'track:"{normalize_text(track_name)}" artist:"{normalize_text(artist_name)}"',
        f'{normalize_text(track_name)} {normalize_text(artist_name)}'
    ]

    best_item = None
    best_score = -1.0
    best_query = None

    for q in queries:
        try:
            results = sp.search(
                q=q,
                type="track",
                limit=10
            )
        except Exception as e:
            print(f"Spotify search failed for query={q}: {e}")
            time.sleep(1.5)
            continue

        items = results.get("tracks", {}).get("items", [])
        if not items:
            continue

        for item in items:
            spotify_track = item.get("name", "")
            spotify_artist = item.get("artists", [{}])[0].get("name", "") if item.get("artists") else ""

            s = score_candidate(artist_name, track_name, spotify_artist, spotify_track)

            # small bonus if exact normalized equality happens
            if normalize_text(track_name) == normalize_text(spotify_track):
                s += 0.15
            if normalize_text(artist_name) == normalize_text(spotify_artist):
                s += 0.15

            if s > best_score:
                best_score = s
                best_item = item
                best_query = q

        # if very good match found, stop early
        if best_score >= 0.95:
            break

    return best_item, best_score, best_query

# =========================
# 5. Match + enrich
# =========================
results = []

for i, row in df.iterrows():
    lastfm_artist = row.get("lastfm_artist_name")
    lastfm_track = row.get("lastfm_track_name")

    print(f"[{i+1}/{len(df)}] Matching: {lastfm_artist} - {lastfm_track}")

    best_item, best_score, best_query = search_spotify_best_match(lastfm_artist, lastfm_track)

    enriched = row.to_dict()

    if best_item is None or best_score < 0.55:
        enriched.update({
            "spotify_match_found": 0,
            "spotify_match_score": best_score if best_score >= 0 else None,
            "spotify_match_query": best_query,
            "spotify_track_id": None,
            "spotify_track_name": None,
            "spotify_artist_id": None,
            "spotify_artist_name": None,
            "spotify_album_id": None,
            "spotify_album_name": None,
            "spotify_release_date": None,
            "spotify_popularity": None,
            "spotify_duration_ms": None,
            "spotify_explicit": None,
            "spotify_external_url": None
        })
    else:
        album = best_item.get("album", {})
        artists = best_item.get("artists", [])

        enriched.update({
            "spotify_match_found": 1,
            "spotify_match_score": round(best_score, 4),
            "spotify_match_query": best_query,
            "spotify_track_id": best_item.get("id"),
            "spotify_track_name": best_item.get("name"),
            "spotify_artist_id": artists[0].get("id") if artists else None,
            "spotify_artist_name": artists[0].get("name") if artists else None,
            "spotify_album_id": album.get("id"),
            "spotify_album_name": album.get("name"),
            "spotify_release_date": album.get("release_date"),
            "spotify_popularity": best_item.get("popularity"),
            "spotify_duration_ms": best_item.get("duration_ms"),
            "spotify_explicit": best_item.get("explicit"),
            "spotify_external_url": best_item.get("external_urls", {}).get("spotify")
        })

    results.append(enriched)

    # checkpoint every 500 rows
    if (i + 1) % 500 == 0:
        pd.DataFrame(results).to_csv(checkpoint_csv, index=False)
        print(f"Checkpoint saved: {checkpoint_csv}")

    time.sleep(0.15)

# =========================
# 6. Save outputs
# =========================
df_out = pd.DataFrame(results)
df_out.to_csv(output_csv, index=False)

df_unmatched = df_out[df_out["spotify_match_found"] == 0].copy()
df_unmatched.to_csv(unmatched_csv, index=False)

print(f"\nTotal input rows: {len(df_out)}")
print(f"Matched rows: {(df_out['spotify_match_found'] == 1).sum()}")
print(f"Unmatched rows: {(df_out['spotify_match_found'] == 0).sum()}")

print(f"Saved enriched CSV to: {output_csv}")
print(f"Saved unmatched CSV to: {unmatched_csv}")