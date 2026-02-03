import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.wfla.com/feed/",
}


@asset
def wfla_raw() -> list[dict]:
    """Fetch all WFLA Tampa Bay RSS feeds"""
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
def wfla_csv(wfla_raw: list[dict]) -> str:
    """Save WFLA Tampa Bay feed data to CSV"""
    df = pd.DataFrame(wfla_raw)
    output_dir = Path("dagster_data/wfla")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for WFLA assets
wfla_job = define_asset_job(
    "wfla_job",
    selection=["wfla_raw", "wfla_csv"],
    description="Job to fetch and process WFLA RSS feeds"
)

# Schedule to run every 8 hours
wfla_schedule = ScheduleDefinition(
    job=wfla_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="wfla_schedule",
    description="Run WFLA RSS feed collection every morning"
)
