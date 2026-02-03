import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.dailynews.com/feed/",
}


@asset
def dailynews_raw() -> list[dict]:
    """Fetch all Daily News (Los Angeles) RSS feeds"""
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
def dailynews_csv(dailynews_raw: list[dict]) -> str:
    """Save Daily News (Los Angeles) feed data to CSV"""
    df = pd.DataFrame(dailynews_raw)
    output_dir = Path("dagster_data/dailynews")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Daily News assets
dailynews_job = define_asset_job(
    "dailynews_job",
    selection=["dailynews_raw", "dailynews_csv"],
    description="Job to fetch and process Daily News RSS feeds"
)

# Schedule to run every 4 hours
dailynews_schedule = ScheduleDefinition(
    job=dailynews_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="dailynews_schedule",
    description="Run Daily News RSS feed collection every morning"
)
