import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://kdvr.com/feed/",
}


@asset
def kdvr_raw() -> list[dict]:
    """Fetch all FOX31 Denver RSS feeds"""
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
def kdvr_csv(kdvr_raw: list[dict]) -> str:
    """Save FOX31 Denver feed data to CSV"""
    df = pd.DataFrame(kdvr_raw)
    output_dir = Path("dagster_data/kdvr")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)

# Define a job specifically for KDVR assets
kdvr_job = define_asset_job(
    "kdvr_job",
    selection=["kdvr_raw", "kdvr_csv"],
    description="Job to fetch and process KDVR RSS feeds"
)

# Schedule to run every 8 hours
kdvr_schedule = ScheduleDefinition(
    job=kdvr_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="kdvr_schedule",
    description="Run KDVR RSS feed collection every morning"
)
