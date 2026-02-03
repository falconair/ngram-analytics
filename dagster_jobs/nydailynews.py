import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.nydailynews.com/feed/",
}


@asset
def nydailynews_raw() -> list[dict]:
    """Fetch all New York Daily News RSS feeds"""
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
def nydailynews_csv(nydailynews_raw: list[dict]) -> str:
    """Save New York Daily News feed data to CSV"""
    df = pd.DataFrame(nydailynews_raw)
    output_dir = Path("dagster_data/nydailynews")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for NY Daily News assets
nydailynews_job = define_asset_job(
    "nydailynews_job",
    selection=["nydailynews_raw", "nydailynews_csv"],
    description="Job to fetch and process NY Daily News RSS feeds"
)

# Schedule to run every 3 hours
nydailynews_schedule = ScheduleDefinition(
    job=nydailynews_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="nydailynews_schedule",
    description="Run NY Daily News RSS feed collection every morning"
)
