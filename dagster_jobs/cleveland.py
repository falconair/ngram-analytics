import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.cleveland.com/arc/outboundfeeds/rss/",
}


@asset
def cleveland_raw() -> list[dict]:
    """Fetch all Cleveland Plain Dealer RSS feeds"""
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
def cleveland_csv(cleveland_raw: list[dict]) -> str:
    """Save Cleveland Plain Dealer feed data to CSV"""
    df = pd.DataFrame(cleveland_raw)
    output_dir = Path("dagster_data/cleveland")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Cleveland assets
cleveland_job = define_asset_job(
    "cleveland_job",
    selection=["cleveland_raw", "cleveland_csv"],
    description="Job to fetch and process Cleveland RSS feeds"
)

# Schedule to run every 8 hours
cleveland_schedule = ScheduleDefinition(
    job=cleveland_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="cleveland_schedule",
    description="Run Cleveland RSS feed collection every morning"
)
