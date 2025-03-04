import logging
import asyncio

class BaseMediaHandler:
    def __init__(self, processor):
        self.processor = processor
        self.logger = logging.getLogger(__name__)
        self.client = processor.client
        self.target_chat_id = processor.target_chat_id
        self.media_manager = processor.media_manager

    def supports(self, message):
        raise NotImplementedError("Handler must implement supports method")

    async def handle(self, message, target_topic_id):
        raise NotImplementedError("Handler must implement handle method")