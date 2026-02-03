import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://laist.com/index.atom",
}


@asset
def laist_raw() -> list[dict]:
    """Fetch all LAist RSS feeds"""
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
def laist_csv(laist_raw: list[dict]) -> str:
    """Save LAist feed data to CSV"""
    df = pd.DataFrame(laist_raw)
    output_dir = Path("dagster_data/laist")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for LAist assets
laist_job = define_asset_job(
    "laist_job",
    selection=["laist_raw", "laist_csv"],
    description="Job to fetch and process LAist RSS feeds"
)

# Schedule to run every 8 hours
laist_schedule = ScheduleDefinition(
    job=laist_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="laist_schedule",
    description="Run LAist RSS feed collection every morning"
)
