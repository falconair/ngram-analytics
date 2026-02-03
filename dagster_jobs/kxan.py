import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.kxan.com/feed/",
}


@asset
def kxan_raw() -> list[dict]:
    """Fetch all KXAN Austin RSS feeds"""
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
def kxan_csv(kxan_raw: list[dict]) -> str:
    """Save KXAN Austin feed data to CSV"""
    df = pd.DataFrame(kxan_raw)
    output_dir = Path("dagster_data/kxan")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for KXAN assets
kxan_job = define_asset_job(
    "kxan_job",
    selection=["kxan_raw", "kxan_csv"],
    description="Job to fetch and process KXAN RSS feeds"
)

# Schedule to run every 8 hours
kxan_schedule = ScheduleDefinition(
    job=kxan_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="kxan_schedule",
    description="Run KXAN RSS feed collection every morning"
)
