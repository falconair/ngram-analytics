import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.ocregister.com/feed/",
    "news": "https://www.ocregister.com/news/feed/",
    "sports": "https://www.ocregister.com/sports/feed/",
}


@asset
def ocregister_raw() -> list[dict]:
    """Fetch all Orange County Register RSS feeds"""
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
def ocregister_csv(ocregister_raw: list[dict]) -> str:
    """Save Orange County Register feed data to CSV"""
    df = pd.DataFrame(ocregister_raw)
    output_dir = Path("dagster_data/ocregister")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for OC Register assets
ocregister_job = define_asset_job(
    "ocregister_job",
    selection=["ocregister_raw", "ocregister_csv"],
    description="Job to fetch and process OC Register RSS feeds"
)

# Schedule to run every 6 hours
ocregister_schedule = ScheduleDefinition(
    job=ocregister_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="ocregister_schedule",
    description="Run OC Register RSS feed collection every morning"
)
