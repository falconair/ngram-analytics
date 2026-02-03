import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://fox2now.com/feed/",
}


@asset
def fox2now_raw() -> list[dict]:
    """Fetch all FOX 2 St. Louis RSS feeds"""
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
def fox2now_csv(fox2now_raw: list[dict]) -> str:
    """Save FOX 2 St. Louis feed data to CSV"""
    df = pd.DataFrame(fox2now_raw)
    output_dir = Path("dagster_data/fox2now")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Fox2Now assets
fox2now_job = define_asset_job(
    "fox2now_job",
    selection=["fox2now_raw", "fox2now_csv"],
    description="Job to fetch and process Fox2Now RSS feeds"
)

# Schedule to run every 8 hours
fox2now_schedule = ScheduleDefinition(
    job=fox2now_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="fox2now_schedule",
    description="Run Fox2Now RSS feed collection every morning"
)
