import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://omaha.com/search/?f=rss",
}


@asset
def omaha_raw() -> list[dict]:
    """Fetch all Omaha World-Herald RSS feeds"""
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
def omaha_csv(omaha_raw: list[dict]) -> str:
    """Save Omaha World-Herald feed data to CSV"""
    df = pd.DataFrame(omaha_raw)
    output_dir = Path("dagster_data/omaha")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Omaha assets
omaha_job = define_asset_job(
    "omaha_job",
    selection=["omaha_raw", "omaha_csv"],
    description="Job to fetch and process Omaha RSS feeds"
)

# Schedule to run every 8 hours
omaha_schedule = ScheduleDefinition(
    job=omaha_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="omaha_schedule",
    description="Run Omaha RSS feed collection every morning"
)
