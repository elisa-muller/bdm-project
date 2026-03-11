import os
import re
import pandas as pd

# =====================================
# 1. Paths
# =====================================
LASTFM_PATH = "data/lastfm_tracks_unique.csv"
SPOTIFY_PATH = "data/spotify_kaggle.csv"

OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_MATCHED = os.path.join(OUTPUT_DIR, "lastfm_spotify_kaggle_enriched.csv")
OUTPUT_UNMATCHED = os.path.join(OUTPUT_DIR, "lastfm_unmatched_kaggle.csv")

# =====================================
# 2. Load data
# =====================================
df_lastfm = pd.read_csv(LASTFM_PATH)
df_spotify = pd.read_csv(SPOTIFY_PATH)

print("Last.fm rows:", len(df_lastfm))
print("Spotify Kaggle rows:", len(df_spotify))

# =====================================
# 3. Correct column names
# =====================================
LASTFM_TRACK_COL = "lastfm_track_name"
LASTFM_ARTIST_COL = "lastfm_artist_name"

SPOTIFY_TRACK_COL = "track_name"
SPOTIFY_ARTIST_COL = "artists"

# =====================================
# 4. Normalization helpers
# =====================================
def clean_text(text):
    if pd.isna(text):
        return ""

    text = str(text).lower().strip()

    # remove content inside parentheses/brackets
    text = re.sub(r"\(.*?\)", "", text)
    text = re.sub(r"\[.*?\]", "", text)

    # remove common noisy words
    patterns_to_remove = [
        r"\bremaster(ed)?\b",
        r"\blive\b",
        r"\bmono\b",
        r"\bstereo\b",
        r"\bversion\b",
        r"\bradio edit\b",
        r"\bedit\b",
        r"\bdeluxe\b",
        r"\bbonus track\b",
        r"\bfeat\b\.?",
        r"\bfeaturing\b",
        r"\bft\b\.?"
    ]
    for p in patterns_to_remove:
        text = re.sub(p, "", text)

    text = re.sub(r"[^a-z0-9\s,&;x]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text

def keep_main_artist(text):
    text = clean_text(text)

    separators = [",", "&", ";", " x ", " and "]
    for sep in separators:
        if sep in text:
            return text.split(sep)[0].strip()

    return text

def parse_artist_field(text):
    if pd.isna(text):
        return ""

    text = str(text)

    # in case artists comes like ['Taylor Swift']
    text = text.replace("[", "").replace("]", "")
    text = text.replace("'", "").replace('"', "")

    return keep_main_artist(text)

# =====================================
# 5. Create normalized keys
# =====================================
df_lastfm["track_norm"] = df_lastfm[LASTFM_TRACK_COL].apply(clean_text)
df_lastfm["artist_norm"] = df_lastfm[LASTFM_ARTIST_COL].apply(parse_artist_field)
df_lastfm["match_key"] = df_lastfm["artist_norm"] + " || " + df_lastfm["track_norm"]

df_spotify["track_norm"] = df_spotify[SPOTIFY_TRACK_COL].apply(clean_text)
df_spotify["artist_norm"] = df_spotify[SPOTIFY_ARTIST_COL].apply(parse_artist_field)
df_spotify["match_key"] = df_spotify["artist_norm"] + " || " + df_spotify["track_norm"]

# =====================================
# 6. Remove duplicate Spotify keys
#    Keep most popular match
# =====================================
df_spotify = df_spotify.sort_values(by="popularity", ascending=False)
df_spotify_unique = df_spotify.drop_duplicates(subset=["match_key"]).copy()

print("Unique Spotify match keys:", len(df_spotify_unique))

# =====================================
# 7. Exact normalized match
# =====================================
df_merged = df_lastfm.merge(
    df_spotify_unique,
    on="match_key",
    how="left",
    suffixes=("_lastfm", "_spotify")
)

# =====================================
# 8. Diagnostics
# =====================================
df_merged["spotify_match_found"] = df_merged[SPOTIFY_TRACK_COL].notna().astype(int)

matched = df_merged["spotify_match_found"].sum()
unmatched = len(df_merged) - matched
match_rate = 100 * matched / len(df_merged) if len(df_merged) > 0 else 0

print(f"Matched rows: {matched}")
print(f"Unmatched rows: {unmatched}")
print(f"Match rate: {match_rate:.2f}%")

# =====================================
# 9. Save outputs
# =====================================
df_merged.to_csv(OUTPUT_MATCHED, index=False)

df_unmatched = df_merged[df_merged["spotify_match_found"] == 0].copy()
df_unmatched.to_csv(OUTPUT_UNMATCHED, index=False)

print(f"Saved enriched dataset to: {OUTPUT_MATCHED}")
print(f"Saved unmatched dataset to: {OUTPUT_UNMATCHED}")