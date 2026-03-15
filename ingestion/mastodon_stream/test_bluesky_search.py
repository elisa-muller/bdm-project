"""
test_bluesky_search.py
Prueba Bluesky searchPosts — busca posts recientes sobre artistas.
Simula warm path: polling cada 30s con deduplicación.

Instalar: pip install atproto
Uso: python test_bluesky_search.py
"""

import time
import json
from atproto import Client

# ── Config ────────────────────────────────────────────────────────────────────
# No necesita cuenta — búsqueda pública
client = Client()
client.login("anonymous", "")  # fallback: puede que no necesite login para search

# Top artists from your CSV to search
ARTISTS = [
    "Harry Styles",
    "Bad Bunny",
    "Taylor Swift",
    "Billie Eilish",
    "The Weeknd",
    "Olivia Rodrigo",
    "Drake",
    "Beyoncé",
]

POLL_ROUNDS = 2      # how many polling rounds to simulate
POLL_SLEEP  = 10     # seconds between rounds (in production: 30-60s)


def search_artist_posts(artist: str, limit: int = 10) -> list[dict]:
    """Search recent posts mentioning an artist."""
    try:
        results = client.app.bsky.feed.search_posts(
            params={"q": artist, "limit": limit, "sort": "latest"}
        )
        posts = []
        for post in results.posts:
            posts.append({
                "post_id":    post.uri,
                "created_at": str(post.record.created_at),
                "text":       post.record.text,
                "lang":       post.record.langs[0] if post.record.langs else "?",
                "author":     post.author.handle,
                "likes":      post.like_count or 0,
                "reposts":    post.repost_count or 0,
                "artist_query": artist,
            })
        return posts
    except Exception as e:
        print(f"  Error searching '{artist}': {e}")
        return []


print("=== Bluesky searchPosts TEST (warm path simulation) ===\n")

seen_ids = set()
total_new = 0

for round_num in range(1, POLL_ROUNDS + 1):
    print(f"── Round {round_num}/{POLL_ROUNDS} ──────────────────────────────")
    round_new = 0

    for artist in ARTISTS:
        posts = search_artist_posts(artist, limit=5)
        new_posts = [p for p in posts if p["post_id"] not in seen_ids]

        if new_posts:
            print(f"\n  {artist}: {len(new_posts)} new posts")
            for p in new_posts:
                seen_ids.add(p["post_id"])
                round_new += 1
                print(f"  [{p['lang']}] {p['text'][:120]}")
                print(f"  ❤ {p['likes']}  🔁 {p['reposts']}")
                print()

        time.sleep(0.5)  # be polite

    total_new += round_new
    print(f"\n  Round {round_num}: {round_new} new posts found")

    if round_num < POLL_ROUNDS:
        print(f"  Waiting {POLL_SLEEP}s before next round...\n")
        time.sleep(POLL_SLEEP)

print(f"\n{'='*55}")
print(f"  Total new posts across {POLL_ROUNDS} rounds: {total_new}")
print(f"  Unique posts seen: {len(seen_ids)}")
print(f"\n  → {'✓ Good volume for warm path' if total_new > 20 else '~ Low volume but usable'}")
print(f"\n  Sample raw payload:")
print(json.dumps({
    "post_id":     "at://did:plc:xxx/app.bsky.feed.post/xxx",
    "created_at":  "2026-03-15T10:23:45.000Z",
    "text":        "Harry Styles new album is giving me life rn 🔥",
    "lang":        "en",
    "author":      "user.bsky.social",
    "likes":       12,
    "reposts":     3,
    "artist_query": "Harry Styles",
}, indent=2))