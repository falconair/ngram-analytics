import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "homepage": "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
    "arts": "https://rss.nytimes.com/services/xml/rss/nyt/Arts.xml",
    "books": "https://rss.nytimes.com/services/xml/rss/nyt/Books.xml",
    "business": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "health": "https://rss.nytimes.com/services/xml/rss/nyt/Health.xml",
    "movies": "https://rss.nytimes.com/services/xml/rss/nyt/Movies.xml",
    "opinion": "https://rss.nytimes.com/services/xml/rss/nyt/Opinion.xml",
    "politics": "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
    "science": "https://rss.nytimes.com/services/xml/rss/nyt/Science.xml",
    "sports": "https://rss.nytimes.com/services/xml/rss/nyt/Sports.xml",
    "technology": "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
    "theater": "https://rss.nytimes.com/services/xml/rss/nyt/Theater.xml",
    "travel": "https://rss.nytimes.com/services/xml/rss/nyt/Travel.xml",
    "us": "https://rss.nytimes.com/services/xml/rss/nyt/US.xml",
    "world": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
}


@asset
def nytimes_raw() -> list[dict]:
    """Fetch all New York Times RSS feeds"""
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
def nytimes_csv(nytimes_raw: list[dict]) -> str:
    """Save New York Times feed data to CSV"""
    df = pd.DataFrame(nytimes_raw)
    output_dir = Path("dagster_data/nytimes")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for NYTimes assets
nytimes_job = define_asset_job(
    "nytimes_job",
    selection=["nytimes_raw", "nytimes_csv"],
    description="Job to fetch and process New York Times RSS feeds"
)

# Schedule to run every 2 hours (major news source)
nytimes_schedule = ScheduleDefinition(
    job=nytimes_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="nytimes_schedule",
    description="Run New York Times RSS feed collection every morning"
)
