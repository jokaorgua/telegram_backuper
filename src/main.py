import asyncio
from datetime import datetime, timedelta
from .config import Config
from .client import TelegramClientInterface
from .synchronizer import Synchronizer
from .database import Database
from .repository import Repository
import logging
import argparse
import os


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

    synchronizer = Synchronizer(client, config.source_chat_id, config.target_chat_id, repo, config.temp_dir)

    mode = args.mode
    if mode == "sync":
        start_date = args.date if args.date else datetime.now() - timedelta(days=1)
        logger.info(f"Selected sync mode with start date: {start_date}")
        await synchronizer.sync_history(start_date)
    elif mode == "sync-threads":
        start_date = args.date if args.date else datetime.now() - timedelta(days=1)
        logger.info(f"Selected sync-threads mode with start date: {start_date}")
        await synchronizer.sync_threads(start_date)
    elif mode == "sync-topics":
        logger.info("Selected sync-topics mode")
        await synchronizer.sync_topics()
    elif mode == "sync-thread":
        # Запрашиваем дату начала синхронизации
        default_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        date_input = input(f"Enter start date (YYYY-MM-DD, default {default_date}): ") or default_date
        try:
            start_date = datetime.strptime(date_input, "%Y-%m-%d")
        except ValueError:
            logger.error(f"Invalid date format: {date_input}")
            raise ValueError("Date must be in YYYY-MM-DD format")

        # Получаем список тем
        source_topics = await synchronizer._get_source_topics()
        if not source_topics:
            logger.error("No topics found in source chat")
            return

        # Выводим список тем
        print("Available topics in source chat:")
        for topic_id, title in source_topics.items():
            print(f"ID: {topic_id} - {title}")

        # Запрашиваем ID темы
        topic_id_input = input("Enter the topic ID to sync: ")
        try:
            topic_id = int(topic_id_input)
            if topic_id not in source_topics:
                raise ValueError(f"Topic ID {topic_id} not found in source chat")
        except ValueError as e:
            logger.error(f"Invalid topic ID: {e}")
            raise ValueError("Topic ID must be a valid integer from the list")

        logger.info(f"Selected sync-thread mode for topic {topic_id} with start date: {start_date}")
        await synchronizer.sync_thread(topic_id, start_date)
    elif mode == "listen":
        logger.info("Selected listen mode")
        await synchronizer.listen_new_messages()
    else:
        logger.error(f"Invalid mode: {mode}")
        raise ValueError(
            f"Mode must be 'sync', 'sync-threads', 'sync-topics', 'sync-thread', or 'listen', got '{mode}'")


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