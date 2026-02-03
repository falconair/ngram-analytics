import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://tulsaworld.com/search/?f=rss",
}


@asset
def tulsaworld_raw() -> list[dict]:
    """Fetch all Tulsa World RSS feeds"""
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
def tulsaworld_csv(tulsaworld_raw: list[dict]) -> str:
    """Save Tulsa World feed data to CSV"""
    df = pd.DataFrame(tulsaworld_raw)
    output_dir = Path("dagster_data/tulsaworld")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Tulsa World assets
tulsaworld_job = define_asset_job(
    "tulsaworld_job",
    selection=["tulsaworld_raw", "tulsaworld_csv"],
    description="Job to fetch and process Tulsa World RSS feeds"
)

# Schedule to run every 8 hours
tulsaworld_schedule = ScheduleDefinition(
    job=tulsaworld_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="tulsaworld_schedule",
    description="Run Tulsa World RSS feed collection every morning"
)
