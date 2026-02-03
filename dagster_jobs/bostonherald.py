import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.bostonherald.com/feed/",
}


@asset
def bostonherald_raw() -> list[dict]:
    """Fetch all Boston Herald RSS feeds"""
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
def bostonherald_csv(bostonherald_raw: list[dict]) -> str:
    """Save Boston Herald feed data to CSV"""
    df = pd.DataFrame(bostonherald_raw)
    output_dir = Path("dagster_data/bostonherald")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)

# Define a job specifically for Boston Herald assets
bostonherald_job = define_asset_job(
    "bostonherald_job",
    selection=["bostonherald_raw", "bostonherald_csv"],
    description="Job to fetch and process Boston Herald RSS feeds"
)

# Schedule to run every 6 hours
bostonherald_schedule = ScheduleDefinition(
    job=bostonherald_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="bostonherald_schedule",
    description="Run Boston Herald RSS feed collection every morning"
)
