import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.kron4.com/feed/",
}


@asset
def kron4_raw() -> list[dict]:
    """Fetch all KRON4 San Francisco RSS feeds"""
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
def kron4_csv(kron4_raw: list[dict]) -> str:
    """Save KRON4 San Francisco feed data to CSV"""
    df = pd.DataFrame(kron4_raw)
    output_dir = Path("dagster_data/kron4")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for KRON4 assets
kron4_job = define_asset_job(
    "kron4_job",
    selection=["kron4_raw", "kron4_csv"],
    description="Job to fetch and process KRON4 RSS feeds"
)

# Schedule to run every 8 hours
kron4_schedule = ScheduleDefinition(
    job=kron4_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="kron4_schedule",
    description="Run KRON4 RSS feed collection every morning"
)
