import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.wivb.com/feed/",
}


@asset
def wivb_raw() -> list[dict]:
    """Fetch all News 4 Buffalo RSS feeds"""
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
def wivb_csv(wivb_raw: list[dict]) -> str:
    """Save News 4 Buffalo feed data to CSV"""
    df = pd.DataFrame(wivb_raw)
    output_dir = Path("dagster_data/wivb")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for WIVB assets
wivb_job = define_asset_job(
    "wivb_job",
    selection=["wivb_raw", "wivb_csv"],
    description="Job to fetch and process WIVB RSS feeds"
)

# Schedule to run every 8 hours
wivb_schedule = ScheduleDefinition(
    job=wivb_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="wivb_schedule",
    description="Run WIVB RSS feed collection every morning"
)
