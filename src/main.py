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

    # Создаем папку для временных файлов, если не существует
    os.makedirs(config.temp_dir, exist_ok=True)

    db = Database("telegram_cloner.db")
    repo = Repository("telegram_cloner.db")
    client = TelegramClientInterface(config)
    await client.start()

    synchronizer = Synchronizer(client, config.source_chat_id, config.target_chat_id, repo, config.temp_dir)

    if args.mode == "sync":
        start_date = args.date if args.date else datetime.now() - timedelta(days=1)
        logger.info(f"Selected sync mode with start date: {start_date}")
        await synchronizer.sync_history(start_date)
    elif args.mode == "sync-threads":
        start_date = args.date if args.date else datetime.now() - timedelta(days=1)
        logger.info(f"Selected sync-threads mode with start date: {start_date}")
        await synchronizer.sync_threads(start_date)
    elif args.mode == "sync-topics":
        logger.info("Selected sync-topics mode")
        await synchronizer.sync_topics()
    elif args.mode == "listen":
        logger.info("Selected listen mode")
        await synchronizer.listen_new_messages()
    else:
        logger.error(f"Invalid mode: {args.mode}")
        raise ValueError(f"Mode must be 'sync', 'sync-threads', 'sync-topics', or 'listen', got '{args.mode}'")


def parse_args():
    parser = argparse.ArgumentParser(description="Telegram Group/Channel Cloner")
    parser.add_argument(
        "mode",
        choices=["sync", "sync-threads", "sync-topics", "listen"],
        help="Operation mode: 'sync' for full history, 'sync-threads' for threads only, 'sync-topics' for topic names only, 'listen' for real-time listening"
    )
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        default=None,
        help="Start date for sync/sync-threads mode (YYYY-MM-DD), defaults to yesterday if not specified"
    )
    return parser.parse_args()


if __name__ == "__main__":
    # Предварительная настройка логирования до загрузки конфига
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    asyncio.run(main(args))