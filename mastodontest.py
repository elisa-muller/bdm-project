from mastodon import Mastodon
from dotenv import load_dotenv
import os

load_dotenv()

mastodon = Mastodon(
    access_token=os.getenv('MASTODON_ACCESS_TOKEN'),
    api_base_url=os.getenv('MASTODON_API_BASE_URL')
)

# Search for music-related posts
posts = mastodon.timeline_hashtag('nowplaying', limit=5)

for post in posts:
    print(post['created_at'], '|', post['content'][:100])