from telethon.tl.types import Message
from telethon import utils
import logging
import os

class MessageProcessor:
    def __init__(self, client, source_chat_id, target_chat_id, repository, temp_dir):
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initializing MessageProcessor for source {source_chat_id} to target {target_chat_id}")
        self.client = client
        self.source_chat_id = source_chat_id
        self.target_chat_id = target_chat_id
        self.repository = repository
        self.temp_dir = temp_dir
        self.message_map = {}  # {source_topic_id: {source_msg_id: target_msg_id}}

    async def process_message(self, message: Message):
        topic_id = getattr(message, 'message_thread_id', None) or 0
        if topic_id not in self.message_map:
            self.message_map[topic_id] = {}
        self.logger.info(f"Processing message {message.id} in topic {topic_id}")

        # Проверяем сообщение в базе
        msg_record = self.repository.get_message(message.id)
        if msg_record:
            if msg_record[3] == 1 and msg_record[1]:  # synced и есть target_msg_id
                self.logger.debug(f"Message {message.id} already synced to {msg_record[1]}")
                self._store_message_mapping(topic_id, message.id, msg_record[1])
                return None
            elif msg_record[3] == 0:  # не синхронизировано
                self.repository.add_message(message.id, topic_id)
        else:
            self.repository.add_message(message.id, topic_id)

        if message.media:
            result = await self._handle_media(message, topic_id)
            if result:
                self.repository.update_message(message.id, result.id)
                self.logger.info(f"Processed media message {message.id} to {result.id}")
            return result
        elif message.message:
            result = await self._handle_text(message, topic_id)
            if result:
                self.repository.update_message(message.id, result.id)
                self.logger.info(f"Processed text message {message.id} to {result.id}")
            return result
        self.logger.warning(f"Skipped message {message.id} - no content")
        return None

    async def _handle_media(self, message: Message, topic_id: int):
        self.logger.debug(f"Downloading media for message {message.id}")
        file_path = await self.client.client.download_media(message.media, file=self.temp_dir)
        self.logger.debug(f"Sending media for message {message.id} from {file_path}")
        sent_message = await self.client.bot.send_file(
            self.target_chat_id,
            file_path,
            caption=message.message or '',
            reply_to=self._get_reply_id(message, topic_id)
        )
        self._store_message_mapping(topic_id, message.id, sent_message.id)
        os.remove(file_path)  # Удаляем временный файл
        return sent_message

    async def _handle_text(self, message: Message, topic_id: int):
        text = self._process_links(message.message, topic_id)
        self.logger.debug(f"Sending text message {message.id}")
        sent_message = await self.client.bot.send_message(
            self.target_chat_id,
            text,
            reply_to=self._get_reply_id(message, topic_id),
            link_preview=False
        )
        self._store_message_mapping(topic_id, message.id, sent_message.id)
        return sent_message

    def _process_links(self, text: str, topic_id: int) -> str:
        self.logger.debug(f"Processing links in text for topic {topic_id}")
        if 't.me' in text and str(self.source_chat_id) in text:
            if topic_id in self.message_map:
                for old_id, new_id in self.message_map[topic_id].items():
                    text = text.replace(f'message{old_id}', f'message{new_id}')
        return text

    def _get_reply_id(self, message: Message, topic_id: int) -> int:
        if message.reply_to_msg_id and topic_id in self.message_map:
            reply_id = self.message_map[topic_id].get(message.reply_to_msg_id)
            self.logger.debug(f"Found reply ID {reply_id} for message {message.id}")
            return reply_id
        elif topic_id != 0 and topic_id in self.message_map and self.message_map[topic_id]:
            first_msg_id = min(self.message_map[topic_id].values())
            self.logger.debug(f"Using first message {first_msg_id} as reply_to for topic {topic_id}")
            return first_msg_id
        return None

    def _store_message_mapping(self, topic_id: int, source_id: int, target_id: int):
        self.logger.debug(f"Mapping source {source_id} to target {target_id} in topic {topic_id}")
        self.message_map[topic_id][source_id] = target_id