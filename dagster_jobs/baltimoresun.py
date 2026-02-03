import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.baltimoresun.com/arcio/feed/",
    "sports": "https://www.baltimoresun.com/sports/feed/",
}


@asset
def baltimoresun_raw() -> list[dict]:
    """Fetch all Baltimore Sun RSS feeds"""
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
def baltimoresun_csv(baltimoresun_raw: list[dict]) -> str:
    """Save Baltimore Sun feed data to CSV"""
    df = pd.DataFrame(baltimoresun_raw)
    output_dir = Path("dagster_data/baltimoresun")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Baltimore Sun assets
baltimoresun_job = define_asset_job(
    "baltimoresun_job",
    selection=["baltimoresun_raw", "baltimoresun_csv"],
    description="Job to fetch and process Baltimore Sun RSS feeds"
)

# Schedule to run every 6 hours
baltimoresun_schedule = ScheduleDefinition(
    job=baltimoresun_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="baltimoresun_schedule",
    description="Run Baltimore Sun RSS feed collection every morning"
)
