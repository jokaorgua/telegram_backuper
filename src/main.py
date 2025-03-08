# src/main.py
import asyncio
from datetime import datetime, timedelta

from src.handlers.file_handler import FileHandler
from .config import Config
from .client import TelegramClientInterface
from .synchronizer import Synchronizer
from .database import Database
from .repository import Repository
from .message_processor import MessageProcessor
from .handlers.photo_handler import PhotoHandler
from .handlers.video_handler import VideoHandler
from .handlers.audio_handler import AudioHandler
from .handlers.webpage_handler import WebPageHandler
from .handlers.mixed_media_handler import MixedMediaHandler
import logging
import argparse
import os

async def select_pair(config):
    logger = logging.getLogger(__name__)
    print("Available pairs:")
    for i, pair in enumerate(config.pairs, 1):
        print(f"{i}. {pair.name} (Source: {pair.source_chat_id}, Target: {pair.target_chat_id})")

    while True:
        choice = input("Enter the number of the pair to work with: ")
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(config.pairs):
                selected_pair = config.pairs[idx]
                logger.info(f"Selected pair: {selected_pair.name} (Source: {selected_pair.source_chat_id}, Target: {selected_pair.target_chat_id})")
                return selected_pair.name, selected_pair.source_chat_id, selected_pair.target_chat_id
            else:
                print(f"Please enter a number between 1 and {len(config.pairs)}")
        except ValueError:
            print("Please enter a valid number")

async def main(args):
    logger = logging.getLogger(__name__)
    logger.info("Starting Telegram Cloner")

    config = Config.load('./config.yaml')
    config.setup_logging()

    os.makedirs(config.temp_dir, exist_ok=True)

    db = Database("telegram_cloner.db")
    repo = Repository("telegram_cloner.db")
    client = TelegramClientInterface(config)
    await client.start()

    # Регистрация хендлеров
    handlers = [PhotoHandler, VideoHandler, AudioHandler, MixedMediaHandler, FileHandler, WebPageHandler]

    mode = args.mode
    if mode in ["sync", "sync-threads", "sync-topics", "sync-thread"]:
        pair_name, source_chat_id, target_chat_id = await select_pair(config)
        processor = MessageProcessor(client, source_chat_id, target_chat_id, repo, config.temp_dir, handlers, config.caption_limit)
        synchronizer = Synchronizer(client, source_chat_id, target_chat_id, repo, config.temp_dir, processor)

        if mode == "sync":
            start_date = args.date if args.date else datetime.now() - timedelta(days=1)
            logger.info(f"Selected sync mode for pair '{pair_name}' (Source: {source_chat_id}, Target: {target_chat_id}) with start date: {start_date}")
            await synchronizer.sync_history(start_date)
        elif mode == "sync-threads":
            start_date = args.date if args.date else datetime.now() - timedelta(days=1)
            logger.info(f"Selected sync-threads mode for pair '{pair_name}' (Source: {source_chat_id}, Target: {target_chat_id}) with start date: {start_date}")
            await synchronizer.sync_threads(start_date)
        elif mode == "sync-topics":
            logger.info(f"Selected sync-topics mode for pair '{pair_name}' (Source: {source_chat_id}, Target: {target_chat_id})")
            await synchronizer.sync_topics()
        elif mode == "sync-thread":
            default_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            date_input = input(f"Enter start date (YYYY-MM-DD, default {default_date}): ") or default_date
            try:
                start_date = datetime.strptime(date_input, "%Y-%m-%d")
            except ValueError:
                logger.error(f"Invalid date format: {date_input}")
                raise ValueError("Date must be in YYYY-MM-DD format")

            source_topics = await synchronizer._get_source_topics()
            if not source_topics:
                logger.error(f"No topics found in source chat for pair '{pair_name}' (Source: {source_chat_id})")
                return

            print("Available topics in source chat:")
            for topic_id, title in source_topics.items():
                print(f"ID: {topic_id} - {title}")

            topic_id_input = input("Enter the topic ID to sync: ")
            try:
                topic_id = int(topic_id_input)
                if topic_id not in source_topics:
                    raise ValueError(f"Topic ID {topic_id} not found in source chat")
            except ValueError as e:
                logger.error(f"Invalid topic ID: {e}")
                raise ValueError("Topic ID must be a valid integer from the list")

            logger.info(f"Selected sync-thread mode for pair '{pair_name}' (Source: {source_chat_id}, Target: {target_chat_id}), topic {topic_id} with start date: {start_date}")
            await synchronizer.sync_thread(topic_id, start_date)
    elif mode == "listen":
        logger.info("Selected listen mode - monitoring all pairs")
        tasks = []
        for pair in config.pairs:
            processor = MessageProcessor(client, pair.source_chat_id, pair.target_chat_id, repo, config.temp_dir, handlers, config.caption_limit)
            synchronizer = Synchronizer(client, pair.source_chat_id, pair.target_chat_id, repo, config.temp_dir, processor)
            tasks.append(synchronizer.listen_new_messages())
        await asyncio.gather(*tasks)
    else:
        logger.error(f"Invalid mode: {mode}")
        raise ValueError(f"Mode must be 'sync', 'sync-threads', 'sync-topics', 'sync-thread', or 'listen', got '{mode}'")

def parse_args():
    parser = argparse.ArgumentParser(description="Telegram Group/Channel Cloner")
    parser.add_argument(
        "mode",
        choices=["sync", "sync-threads", "sync-topics", "sync-thread", "listen"],
        help="Operation mode: 'sync' for full history, 'sync-threads' for threads only, 'sync-topics' for topic names only, 'sync-thread' for specific thread, 'listen' for real-time listening"
    )
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        default=None,
        help="Start date for sync/sync-threads mode (YYYY-MM-DD), defaults to yesterday if not specified"
    )
    return parser.parse_args()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    asyncio.run(main(args))