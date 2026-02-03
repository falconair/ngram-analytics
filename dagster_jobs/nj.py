import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.nj.com/arc/outboundfeeds/rss/",
}


@asset
def nj_raw() -> list[dict]:
    """Fetch all Star-Ledger (Newark/NJ) RSS feeds"""
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
def nj_csv(nj_raw: list[dict]) -> str:
    """Save Star-Ledger (Newark/NJ) feed data to CSV"""
    df = pd.DataFrame(nj_raw)
    output_dir = Path("dagster_data/nj")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for NJ.com assets
nj_job = define_asset_job(
    "nj_job",
    selection=["nj_raw", "nj_csv"],
    description="Job to fetch and process NJ.com RSS feeds"
)

# Schedule to run every 6 hours
nj_schedule = ScheduleDefinition(
    job=nj_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="nj_schedule",
    description="Run NJ.com RSS feed collection every morning"
)
