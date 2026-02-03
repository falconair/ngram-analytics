import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.reviewjournal.com/feed/",
    "news": "https://www.reviewjournal.com/news/feed/",
    "sports": "https://www.reviewjournal.com/sports/feed/",
}


@asset
def reviewjournal_raw() -> list[dict]:
    """Fetch all Las Vegas Review-Journal RSS feeds"""
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
def reviewjournal_csv(reviewjournal_raw: list[dict]) -> str:
    """Save Las Vegas Review-Journal feed data to CSV"""
    df = pd.DataFrame(reviewjournal_raw)
    output_dir = Path("dagster_data/reviewjournal")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)

# Define a job specifically for Review Journal assets
reviewjournal_job = define_asset_job(
    "reviewjournal_job",
    selection=["reviewjournal_raw", "reviewjournal_csv"],
    description="Job to fetch and process Review Journal RSS feeds"
)

# Schedule to run every 8 hours
reviewjournal_schedule = ScheduleDefinition(
    job=reviewjournal_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="reviewjournal_schedule",
    description="Run Review Journal RSS feed collection every morning"
)
