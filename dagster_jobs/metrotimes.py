import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.metrotimes.com/feed/?partner-feed=all",
}


@asset
def metrotimes_raw() -> list[dict]:
    """Fetch all Detroit Metro Times RSS feeds"""
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
def metrotimes_csv(metrotimes_raw: list[dict]) -> str:
    """Save Detroit Metro Times feed data to CSV"""
    df = pd.DataFrame(metrotimes_raw)
    output_dir = Path("dagster_data/metrotimes")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Metro Times assets
metrotimes_job = define_asset_job(
    "metrotimes_job",
    selection=["metrotimes_raw", "metrotimes_csv"],
    description="Job to fetch and process Metro Times RSS feeds"
)

# Schedule to run every 12 hours
metrotimes_schedule = ScheduleDefinition(
    job=metrotimes_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="metrotimes_schedule",
    description="Run Metro Times RSS feed collection every morning"
)
