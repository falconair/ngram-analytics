import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "all": "https://www.orlandosentinel.com/arcio/feed/",
    "news": "https://www.orlandosentinel.com/news/rss2.0.xml",
    "sports": "https://www.orlandosentinel.com/sports/feed/",
}


@asset
def orlandosentinel_raw() -> list[dict]:
    """Fetch all Orlando Sentinel RSS feeds"""
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
def orlandosentinel_csv(orlandosentinel_raw: list[dict]) -> str:
    """Save Orlando Sentinel feed data to CSV"""
    df = pd.DataFrame(orlandosentinel_raw)
    output_dir = Path("dagster_data/orlandosentinel")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Orlando Sentinel assets
orlandosentinel_job = define_asset_job(
    "orlandosentinel_job",
    selection=["orlandosentinel_raw", "orlandosentinel_csv"],
    description="Job to fetch and process Orlando Sentinel RSS feeds"
)

# Schedule to run every 6 hours
orlandosentinel_schedule = ScheduleDefinition(
    job=orlandosentinel_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="orlandosentinel_schedule",
    description="Run Orlando Sentinel RSS feed collection every morning"
)
