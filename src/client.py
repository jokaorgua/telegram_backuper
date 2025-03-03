from telethon import TelegramClient
from telethon.sync import TelegramClient as SyncTelegramClient
import logging

class TelegramClientInterface:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing TelegramClientInterface")
        self.client = SyncTelegramClient(
            'session',
            config.api_id,
            config.api_hash
        )
        self.bot = TelegramClient(
            'bot_session',
            config.api_id,
            config.api_hash
        )
        self.config = config
        self._bot_started = False

    async def start(self):
        self.logger.info("Starting client authentication")
        await self.client.start(phone=self.config.phone)
        self.logger.info("Client authenticated successfully")
        if not self._bot_started:
            self.logger.info("Starting bot authentication")
            await self.bot.start(bot_token=self.config.bot_token)
            self._bot_started = True
            self.logger.info("Bot authenticated successfully")