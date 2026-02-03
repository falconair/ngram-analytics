import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all_news": "https://www.latimes.com/rss2.0.xml",
    "business": "https://www.latimes.com/business/rss2.0.xml",
    "california": "https://www.latimes.com/california/rss2.0.xml",
    "entertainment": "https://www.latimes.com/entertainment-arts/rss2.0.xml",
    "food": "https://www.latimes.com/food/rss2.0.xml",
    "health": "https://www.latimes.com/health/rss2.0.xml",
    "lifestyle": "https://www.latimes.com/lifestyle/rss2.0.xml",
    "opinion": "https://www.latimes.com/opinion/rss2.0.xml",
    "politics": "https://www.latimes.com/politics/rss2.0.xml",
    "science": "https://www.latimes.com/science/rss2.0.xml",
    "sports": "https://www.latimes.com/sports/rss2.0.xml",
    "travel": "https://www.latimes.com/travel/rss2.0.xml",
    "world_nation": "https://www.latimes.com/world-nation/rss2.0.xml",
}


@asset
def latimes_raw() -> list[dict]:
    """Fetch all Los Angeles Times RSS feeds"""
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
def latimes_csv(latimes_raw: list[dict]) -> str:
    """Save Los Angeles Times feed data to CSV"""
    df = pd.DataFrame(latimes_raw)
    output_dir = Path("dagster_data/latimes")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for LA Times assets
latimes_job = define_asset_job(
    "latimes_job",
    selection=["latimes_raw", "latimes_csv"],
    description="Job to fetch and process LA Times RSS feeds"
)

# Schedule to run every 3 hours
latimes_schedule = ScheduleDefinition(
    job=latimes_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="latimes_schedule",
    description="Run LA Times RSS feed collection every morning"
)
