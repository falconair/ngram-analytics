import feedparser
import pandas as pd
from dagster import asset, define_asset_job, ScheduleDefinition
from datetime import date
from pathlib import Path

FEEDS = {
    "news": "https://www.post-gazette.com/rss/",
    "sports": "https://www.post-gazette.com/sports/rss/",
}


@asset
def post_gazette_raw() -> list[dict]:
    """Fetch all Pittsburgh Post-Gazette RSS feeds"""
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
def post_gazette_csv(post_gazette_raw: list[dict]) -> str:
    """Save Pittsburgh Post-Gazette feed data to CSV"""
    df = pd.DataFrame(post_gazette_raw)
    output_dir = Path("dagster_data/post_gazette")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{date.today().isoformat()}.csv"
    df.to_csv(output_path, index=False)
    return str(output_path)


# Define a job specifically for Post Gazette assets
post_gazette_job = define_asset_job(
    "post_gazette_job",
    selection=["post_gazette_raw", "post_gazette_csv"],
    description="Job to fetch and process Post Gazette RSS feeds"
)

# Schedule to run every 6 hours
post_gazette_schedule = ScheduleDefinition(
    job=post_gazette_job,
    cron_schedule="0 6 * * *",  # Every morning at 6 am
    name="post_gazette_schedule",
    description="Run Post Gazette RSS feed collection every morning"
)
