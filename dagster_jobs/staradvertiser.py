import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.staradvertiser.com/feed/",
}


@asset
def staradvertiser_raw() -> list[dict]:
    """Fetch all Honolulu Star-Advertiser RSS feeds"""
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
def staradvertiser_csv(staradvertiser_raw: list[dict]) -> str:
    """Save Honolulu Star-Advertiser feed data to CSV"""
    df = pd.DataFrame(staradvertiser_raw)
    output_dir = Path("dagster_data/staradvertiser")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Star Advertiser assets
staradvertiser_job = define_asset_job(
    "staradvertiser_job",
    selection=["staradvertiser_raw", "staradvertiser_csv"],
    description="Job to fetch and process Star Advertiser RSS feeds"
)

# Schedule to run every 12 hours
staradvertiser_schedule = ScheduleDefinition(
    job=staradvertiser_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="staradvertiser_schedule",
    description="Run Star Advertiser RSS feed collection every morning"
)
