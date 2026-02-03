import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.boston.com/feed/",
}


@asset
def boston_com_raw() -> list[dict]:
    """Fetch all Boston.com RSS feeds"""
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
def boston_com_csv(boston_com_raw: list[dict]) -> str:
    """Save Boston.com feed data to CSV"""
    df = pd.DataFrame(boston_com_raw)
    output_dir = Path("dagster_data/boston_com")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Boston.com assets
boston_com_job = define_asset_job(
    "boston_com_job",
    selection=["boston_com_raw", "boston_com_csv"],
    description="Job to fetch and process Boston.com RSS feeds"
)

# Schedule to run every 8 hours
boston_com_schedule = ScheduleDefinition(
    job=boston_com_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="boston_com_schedule",
    description="Run Boston.com RSS feed collection every morning"
)
