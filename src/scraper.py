"""
Telegram Scraper for Ethiopian Business & Economics Channels
=============================================================
Scrapes public Telegram channels and stores:
- Raw messages as JSON (partitioned by date): data/raw/telegram_messages/YYYY-MM-DD/channel.json
- Images: data/raw/images/{channel_name}/{message_id}.jpg
- CSV backup: data/raw/csv/YYYY-MM-DD/telegram_data.csv
- Logs: logs/scrape_YYYY-MM-DD.log

Usage:
    python src/scraper.py --demo --path data --limit 50
    python src/scraper.py --path data --limit 500   # live Telegram auth
"""

import os
import csv
import json
import asyncio
import argparse
import logging
import random
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.datalake import write_channel_messages_json, write_manifest

load_dotenv()

api_id_str = os.getenv("Tg_API_ID")
api_hash = os.getenv("Tg_API_HASH")

TODAY = datetime.today().strftime("%Y-%m-%d")

DEFAULT_CHANNEL_DELAY = 3.0
DEFAULT_MESSAGE_DELAY = 1.0

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("telegram_scraper")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(
    os.path.join(LOG_DIR, f"scrape_{TODAY}.log"), encoding="utf-8"
)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# =============================================================================
# LIVE SCRAPING (requires Telegram auth)
# =============================================================================

async def scrape_channel(client, channel, writer, base_path, date_str,
                         limit=100, message_delay=DEFAULT_MESSAGE_DELAY,
                         channel_delay=DEFAULT_CHANNEL_DELAY, max_retries=3):
    from telethon.tl.types import MessageMediaPhoto
    from telethon.errors import FloodWaitError

    channel_name = channel.strip('@')
    retries = 0

    while True:
        try:
            entity = await client.get_entity(channel)
            channel_title = entity.title
            messages = []

            channel_image_dir = os.path.join(base_path, "raw", "images", channel_name)
            os.makedirs(channel_image_dir, exist_ok=True)

            logger.info(f"Starting scrape of {channel} (limit={limit})")

            async for message in client.iter_messages(entity, limit=limit):
                image_path: Optional[str] = None
                has_media = message.media is not None

                if has_media and isinstance(message.media, MessageMediaPhoto):
                    filename = f"{message.id}.jpg"
                    image_path = os.path.join(channel_image_dir, filename)
                    try:
                        await client.download_media(message.media, image_path)
                    except Exception as e:
                        logger.warning(f"Failed to download image for message {message.id}: {e}")
                        image_path = None

                message_dict = {
                    "message_id": message.id,
                    "channel_name": channel_name,
                    "channel_title": channel_title,
                    "message_date": message.date.isoformat(),
                    "message_text": message.message or "",
                    "has_media": has_media,
                    "image_path": image_path,
                    "views": message.views or 0,
                    "forwards": message.forwards or 0,
                }

                writer.writerow(list(message_dict.values()))
                messages.append(message_dict)

                if message_delay and message_delay > 0:
                    await asyncio.sleep(message_delay)

            write_channel_messages_json(
                base_path=base_path, date_str=date_str,
                channel_name=channel_name, messages=messages,
            )

            logger.info(f"Finished scraping {channel}: {len(messages)} messages saved")
            if channel_delay and channel_delay > 0:
                await asyncio.sleep(channel_delay)
            return len(messages)

        except FloodWaitError as e:
            wait_seconds = max(int(getattr(e, "seconds", 0) or 0), 1)
            logger.warning(f"FloodWaitError for {channel}: sleeping {wait_seconds}s")
            await asyncio.sleep(wait_seconds)
            retries += 1
            if retries > max_retries:
                logger.error(f"Too many FloodWait retries for {channel}. Skipping.")
                return 0
        except Exception as e:
            logger.error(f"Error scraping {channel}: {e}")
            return 0


async def scrape_all_channels(client, channels, base_path, limit=100,
                              message_delay=DEFAULT_MESSAGE_DELAY,
                              channel_delay=DEFAULT_CHANNEL_DELAY):
    await client.start()
    logger.info(f"Client authenticated. Scraping {len(channels)} channels...")

    csv_dir = os.path.join(base_path, "raw", "csv", TODAY)
    os.makedirs(csv_dir, exist_ok=True)
    os.makedirs(os.path.join(base_path, "raw", "telegram_messages", TODAY), exist_ok=True)
    os.makedirs(os.path.join(base_path, "raw", "images"), exist_ok=True)

    csv_file_path = os.path.join(csv_dir, "telegram_data.csv")
    stats = {}

    with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'message_id', 'channel_name', 'channel_title', 'message_date',
            'message_text', 'has_media', 'image_path', 'views', 'forwards'
        ])

        channel_counts = {}
        for channel in channels:
            logger.info(f"Scraping {channel}...")
            count = await scrape_channel(
                client, channel, writer, base_path, TODAY, limit,
                message_delay, channel_delay,
            )
            stats[channel] = count
            channel_counts[channel.strip("@")] = count

        write_manifest(base_path=base_path, date_str=TODAY,
                       channel_message_counts=channel_counts)

    total = sum(stats.values())
    logger.info(f"Scraping complete. Total messages: {total}")
    for ch, count in stats.items():
        logger.info(f"  {ch}: {count} messages")
    return stats




# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Telegram Scraper for Ethiopian Business & Economics Channels",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
    python src/scraper.py --path data --limit 500
"""
    )

    parser.add_argument(
        "--path",
        type=str,
        default="data",
        help="Base directory for storing scraped data."
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of messages to scrape per channel."
    )

    parser.add_argument(
        "--message-delay",
        type=float,
        default=DEFAULT_MESSAGE_DELAY,
        help="Delay (seconds) between downloading consecutive messages."
    )

    parser.add_argument(
        "--channel-delay",
        type=float,
        default=DEFAULT_CHANNEL_DELAY,
        help="Delay (seconds) between scraping channels."
    )

    args = parser.parse_args()

    if not api_id_str or not api_hash:
        logger.error("Missing Tg_API_ID or Tg_API_HASH in the .env file.")
        sys.exit(1)

    from telethon import TelegramClient

    client = TelegramClient(
        "telegram_scraper_session",
        int(api_id_str),
        api_hash,
    )

    logger.info("Telegram client initialized.")

    target_channels = [
        "@CheMed123",
        "@lobelia4cosmetics",
        "@HakimApps_Guideline",
    ]

    async def main():
        async with client:
            await scrape_all_channels(
                client=client,
                channels=target_channels,
                base_path=args.path,
                limit=args.limit,
                message_delay=args.message_delay,
                channel_delay=args.channel_delay,
            )

    asyncio.run(main())