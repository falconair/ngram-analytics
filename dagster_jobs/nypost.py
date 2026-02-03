import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "business": "https://nypost.com/business/feed/",
    "entertainment": "https://nypost.com/entertainment/feed/",
    "fashion": "https://nypost.com/fashion-and-beauty/feed/",
    "metro": "https://nypost.com/metro/feed/",
    "news": "https://nypost.com/us-news/feed/",
    "opinion": "https://nypost.com/opinion/feed/",
    "sports": "https://nypost.com/sports/feed/",
    "tech": "https://nypost.com/tech/feed/",
}


@asset
def nypost_raw() -> list[dict]:
    """Fetch all NY Post RSS feeds"""
    all_entries = []
    for feed_name, url in FEEDS.items():
        feed = feedparser.parse(url)
        for entry in feed.entries:
            all_entries.append({
                "feed": feed_name,
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
                "summary": entry.get("summary", ""),
                "author": entry.get("author", ""),
                "tags": ", ".join(tag.get("term", "") for tag in entry.get("tags", [])),
            })
    return all_entries


@asset
def nypost_csv(nypost_raw: list[dict]) -> str:
    """Save NY Post feed data to CSV"""
    df = pd.DataFrame(nypost_raw)
    output_dir = Path("dagster_data/nypost")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)

# Define a job specifically for NY Post assets
nypost_job = define_asset_job(
    "nypost_job",
    selection=["nypost_raw", "nypost_csv"],
    description="Job to fetch and process NY Post RSS feeds"
)

# Schedule to run every 3 hours
nypost_schedule = ScheduleDefinition(
    job=nypost_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="nypost_schedule",
    description="Run NY Post RSS feed collection every morning"
)
