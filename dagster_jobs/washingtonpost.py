import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://feeds.washingtonpost.com/rss/homepage",
    "business": "https://feeds.washingtonpost.com/rss/business",
    "local": "https://feeds.washingtonpost.com/rss/local",
    "national": "https://feeds.washingtonpost.com/rss/national",
    "opinions": "https://feeds.washingtonpost.com/rss/opinions",
    "politics": "https://feeds.washingtonpost.com/rss/politics",
    "sports": "http://feeds.washingtonpost.com/rss/sports",
    "technology": "https://feeds.washingtonpost.com/rss/business/technology",
    "world": "https://feeds.washingtonpost.com/rss/world",
}


@asset
def washingtonpost_raw() -> list[dict]:
    """Fetch all Washington Post RSS feeds"""
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
def washingtonpost_csv(washingtonpost_raw: list[dict]) -> str:
    """Save Washington Post feed data to CSV"""
    df = pd.DataFrame(washingtonpost_raw)
    output_dir = Path("dagster_data/washingtonpost")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Washington Post assets
washingtonpost_job = define_asset_job(
    "washingtonpost_job",
    selection=["washingtonpost_raw", "washingtonpost_csv"],
    description="Job to fetch and process Washington Post RSS feeds"
)

# Schedule to run every 2 hours (major news source)
washingtonpost_schedule = ScheduleDefinition(
    job=washingtonpost_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="washingtonpost_schedule",
    description="Run Washington Post RSS feed collection every morning"
)
