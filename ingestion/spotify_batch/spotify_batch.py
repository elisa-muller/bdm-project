import os
import time
import pandas as pd
import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials

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

genres = ["pop", "rock", "jazz", "electronic", "classical", "hip-hop", "reggae"]

per_query_limit = 10
pages_per_genre = 10

output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

tracks = []

for genre in genres:
    print(f"Collecting tracks for genre: {genre}")

    for page in range(pages_per_genre):
        offset = page * per_query_limit

        results = sp.search(
            q=f'genre:"{genre}"',
            type="track",
            limit=per_query_limit,
            offset=offset
        )

        items = results.get("tracks", {}).get("items", [])
        if not items:
            break

        for track in items:
            album = track.get("album", {})
            artists = track.get("artists", [])

            tracks.append({
                "track_id": track.get("id"),
                "track_name": track.get("name"),
                "artist_id": artists[0].get("id") if artists else None,
                "artist_name": artists[0].get("name") if artists else None,
                "album_id": album.get("id"),
                "album_name": album.get("name"),
                "release_date": album.get("release_date"),
                "popularity": track.get("popularity"),
                "duration_ms": track.get("duration_ms"),
                "explicit": track.get("explicit"),
                "external_url": track.get("external_urls", {}).get("spotify"),
                "query_genre": genre
            })

        time.sleep(0.2)

df_tracks = pd.DataFrame(tracks)
df_tracks = df_tracks.drop_duplicates(subset=["track_id"]).reset_index(drop=True)

tracks_path = os.path.join(output_dir, "spotify_tracks.csv")
df_tracks.to_csv(tracks_path, index=False)

print(f"Collected {len(df_tracks)} unique tracks")
print(f"Saved tracks to: {tracks_path}")