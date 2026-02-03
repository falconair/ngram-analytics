import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://fox5sandiego.com/feed/",
}


@asset
def fox5sandiego_raw() -> list[dict]:
    """Fetch all FOX 5 San Diego RSS feeds"""
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
def fox5sandiego_csv(fox5sandiego_raw: list[dict]) -> str:
    """Save FOX 5 San Diego feed data to CSV"""
    df = pd.DataFrame(fox5sandiego_raw)
    output_dir = Path("dagster_data/fox5sandiego")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Fox5 San Diego assets
fox5sandiego_job = define_asset_job(
    "fox5sandiego_job",
    selection=["fox5sandiego_raw", "fox5sandiego_csv"],
    description="Job to fetch and process Fox5 San Diego RSS feeds"
)

# Schedule to run every 8 hours
fox5sandiego_schedule = ScheduleDefinition(
    job=fox5sandiego_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="fox5sandiego_schedule",
    description="Run Fox5 San Diego RSS feed collection every morning"
)
