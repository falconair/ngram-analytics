import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.mercurynews.com/feed/",
}


@asset
def mercurynews_raw() -> list[dict]:
    """Fetch all San Jose Mercury News RSS feeds"""
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
def mercurynews_csv(mercurynews_raw: list[dict]) -> str:
    """Save San Jose Mercury News feed data to CSV"""
    df = pd.DataFrame(mercurynews_raw)
    output_dir = Path("dagster_data/mercurynews")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)

# Define a job specifically for Mercury News assets
mercurynews_job = define_asset_job(
    "mercurynews_job",
    selection=["mercurynews_raw", "mercurynews_csv"],
    description="Job to fetch and process Mercury News RSS feeds"
)

# Schedule to run every 6 hours
mercurynews_schedule = ScheduleDefinition(
    job=mercurynews_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="mercurynews_schedule",
    description="Run Mercury News RSS feed collection every morning"
)
