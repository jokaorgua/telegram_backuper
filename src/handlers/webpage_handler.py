from .base_handler import BaseMediaHandler
from telethon.tl.types import MessageMediaWebPage

class WebPageHandler(BaseMediaHandler):
    def supports(self, message):
        self.logger.info(f"Checking supports in WebPageHandler for message {message.id}, message dump: {message.__dict__}")
        supports_webpage = isinstance(message.media, MessageMediaWebPage)
        self.logger.info(f"WebPageHandler supports check: result={supports_webpage}")
        return supports_webpage

    async def handle(self, message, target_topic_id):
        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(f"Skipping message {message.id} from {message_date} - media is a webpage preview")
        if message.message:
            return await self.processor._handle_text(message, target_topic_id)
        return None