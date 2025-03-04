from telethon.tl.types import Message
from telethon import utils
import logging
import asyncio
from .media_manager import MediaManager

class MessageProcessor:
    def __init__(self, client, source_chat_id, target_chat_id, repository, temp_dir, handlers):
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initializing MessageProcessor for source {source_chat_id} to target {target_chat_id}")
        self.client = client
        self.source_chat_id = source_chat_id
        self.target_chat_id = target_chat_id
        self.repository = repository
        self.temp_dir = temp_dir
        self.message_map = {}
        self.media_manager = MediaManager(client, temp_dir)
        self.handlers = [handler(self) for handler in handlers]
        self.PART_SIZE = 512 * 1024
        self.MAX_PARTS = 4000
        self.MAX_FILE_SIZE = self.PART_SIZE * self.MAX_PARTS
        self.TARGET_PART_SIZE = 1.9 * 1024 * 1024 * 1024

    async def process_message(self, message):
        source_topic_id = 0
        if hasattr(message, 'reply_to') and message.reply_to and message.reply_to.forum_topic:
            source_topic_id = message.reply_to.reply_to_top_id if message.reply_to.reply_to_top_id else message.reply_to.reply_to_msg_id

        if self.source_chat_id not in self.message_map:
            self.message_map[self.source_chat_id] = {}
        if self.target_chat_id not in self.message_map[self.source_chat_id]:
            self.message_map[self.source_chat_id][self.target_chat_id] = {}
        self.logger.info(f"Processing message {message.id} in topic {source_topic_id}")

        msg_record = self.repository.get_message(message.id, self.source_chat_id, self.target_chat_id)
        target_msg_id = msg_record[1] if msg_record else None
        should_reupload = False

        if msg_record and target_msg_id:
            messages = await self.client.client.get_messages(self.target_chat_id, ids=[target_msg_id])
            if not messages or messages[0] is None:
                self.logger.warning(f"Message {message.id} (Target ID: {target_msg_id}) not found in target, marking for reupload")
                should_reupload = True
            elif msg_record[3] == 1:
                self.logger.info(f"Message {message.id} already synced to {target_msg_id} and exists in target")
                self._store_message_mapping(message.id, target_msg_id)
                return None

        if not msg_record or should_reupload:
            if not msg_record:
                self.repository.add_message(message.id, self.source_chat_id, self.target_chat_id, source_topic_id)
            else:
                self.logger.info(f"Reuploading message {message.id} due to missing target ID {target_msg_id}")

        target_topic_id = self._get_target_topic_id(source_topic_id)

        if message.media:
            result = await self._handle_media(message, target_topic_id)
        elif message.message:
            result = await self._handle_text(message, target_topic_id)
        else:
            self.logger.warning(f"Skipped message {message.id} - no content")
            return None

        if result:
            self.repository.update_message(message.id, self.source_chat_id, self.target_chat_id, result.id)
            self.logger.info(f"Processed message {message.id} to {result.id} in topic {target_topic_id}")
            await asyncio.sleep(0.1)
        return result

    def _get_target_topic_id(self, source_topic_id):
        db_topic = self.repository.get_topic(source_topic_id, self.source_chat_id, self.target_chat_id)
        target_topic_id = db_topic[1] if db_topic else None
        if target_topic_id is None:
            self.logger.info(f"No target topic mapping found for source topic {source_topic_id}, using {source_topic_id}")
            return source_topic_id
        self.logger.info(f"Using target topic ID {target_topic_id} for source topic {source_topic_id}")
        return target_topic_id

    async def _handle_media(self, message, target_topic_id):
        for handler in self.handlers:
            if handler.supports(message):
                self.logger.info(f"Selected handler {handler.__class__.__name__} for message {message.id}")
                return await handler.handle(message, target_topic_id)
        self.logger.error(f"No handler supports media type in message {message.id}, message dump: {message.__dict__}")
        return None

    async def _handle_text(self, message, target_topic_id):
        text = self._process_links(message.message)
        self.logger.info(f"Sending text message {message.id}")
        sent_message = await self.client.bot.send_message(
            self.target_chat_id,
            text,
            reply_to=target_topic_id if target_topic_id != 0 else None,
            link_preview=False,
            formatting_entities=message.entities if message.entities else None
        )
        self._store_message_mapping(message.id, sent_message.id)
        return sent_message

    def _process_links(self, text):
        self.logger.info(f"Processing links in text")
        if 't.me' in text and str(self.source_chat_id) in text:
            for old_id, new_id in self.message_map[self.source_chat_id][self.target_chat_id].items():
                text = text.replace(f'message{old_id}', f'message{new_id}')
        return text

    def _store_message_mapping(self, source_id, target_id):
        self.logger.info(f"Mapping source {source_id} to target {target_id}")
        self.message_map[self.source_chat_id][self.target_chat_id][source_id] = target_id