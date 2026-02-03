import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.courant.com/arcio/feed/",
    "news": "https://www.courant.com/news/rss2.0.xml",
    "sports": "https://www.courant.com/sports/feed/",
}


@asset
def courant_raw() -> list[dict]:
    """Fetch all Hartford Courant RSS feeds"""
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
def courant_csv(courant_raw: list[dict]) -> str:
    """Save Hartford Courant feed data to CSV"""
    df = pd.DataFrame(courant_raw)
    output_dir = Path("dagster_data/courant")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Courant assets
courant_job = define_asset_job(
    "courant_job",
    selection=["courant_raw", "courant_csv"],
    description="Job to fetch and process Courant RSS feeds"
)

# Schedule to run every 8 hours
courant_schedule = ScheduleDefinition(
    job=courant_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="courant_schedule",
    description="Run Courant RSS feed collection every morning"
)
