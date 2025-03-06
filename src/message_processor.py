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
        self.processed_group_ids = set()

    async def process_message(self, message):
        source_topic_id = 0
        if hasattr(message, 'reply_to') and message.reply_to and message.reply_to.forum_topic:
            source_topic_id = message.reply_to.reply_to_top_id if message.reply_to.reply_to_top_id else message.reply_to.reply_to_msg_id

        if self.source_chat_id not in self.message_map:
            self.message_map[self.source_chat_id] = {}
        if self.target_chat_id not in self.message_map[self.source_chat_id]:
            self.message_map[self.source_chat_id][self.target_chat_id] = {}
        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(f"Processing message {message.id} from {message_date} in topic {source_topic_id}")

        if message.id in self.processed_group_ids:
            self.logger.info(
                f"Skipping message {message.id} from {message_date} - already processed as part of a group")
            return None

        if message.grouped_id:
            self.logger.info(f"Message {message.id} is part of group {message.grouped_id}, collecting group messages")
            group_messages = await self._collect_group_messages(message)
            if group_messages:
                return await self._process_group_messages(group_messages, source_topic_id)

        return await self._process_single_message(message, source_topic_id)

    async def _collect_group_messages(self, message):
        """Собирает все сообщения из группы по grouped_id, начиная с текущего сообщения."""
        grouped_id = message.grouped_id
        group_messages = [message]
        self.logger.info(f"Collecting group messages for grouped_id {grouped_id} starting from message {message.id}")

        # Просматриваем 30 сообщений после текущего (в сторону новых сообщений)
        async for msg in self.client.client.iter_messages(
                self.source_chat_id,
                min_id=message.id - 1,  # Начинаем с сообщений после текущего
                limit=30,  # Ограничиваем 30 сообщениями
                reverse=True  # Идём от старых к новым
        ):
            if msg.grouped_id == grouped_id and msg.id != message.id:
                group_messages.append(msg)
                self.logger.info(f"Added message {msg.id} to group {grouped_id}")

        # Возвращаем группу только если найдено больше 1 сообщения
        return group_messages if len(group_messages) > 1 else None

    async def _process_group_messages(self, messages, source_topic_id):
        """Обрабатывает группу сообщений как одно сообщение."""
        lead_message = messages[0]
        message_date = lead_message.date.strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(
            f"Processing group of {len(messages)} messages with lead ID {lead_message.id} from {message_date}")

        for msg in messages:
            self.processed_group_ids.add(msg.id)

        msg_record = self.repository.get_message(lead_message.id, self.source_chat_id, self.target_chat_id)
        target_msg_id = msg_record[1] if msg_record else None
        should_reupload = False

        if msg_record and target_msg_id:
            target_messages = await self.client.client.get_messages(self.target_chat_id, ids=[target_msg_id])
            if not target_messages or target_messages[0] is None:
                self.logger.warning(
                    f"Group message {lead_message.id} (Target ID: {target_msg_id}) not found in target, marking for reupload")
                should_reupload = True
            elif msg_record[3] == 1:
                self.logger.info(
                    f"Group message {lead_message.id} from {message_date} already synced to {target_msg_id}")
                self._store_message_mapping(lead_message.id, target_msg_id)
                return None

        if not msg_record or should_reupload:
            if not msg_record:
                self.repository.add_message(lead_message.id, self.source_chat_id, self.target_chat_id, source_topic_id)
            else:
                self.logger.info(
                    f"Reuploading group message {lead_message.id} from {message_date} due to missing target ID {target_msg_id}")

        target_topic_id = self._get_target_topic_id(source_topic_id)

        for handler in self.handlers:
            if handler.supports(messages):
                self.logger.info(
                    f"Selected handler {handler.__class__.__name__} for group message {lead_message.id} from {message_date}")
                result = await handler.handle(messages, target_topic_id)
                if result:
                    target_id = result[0].id if isinstance(result, list) else result.id
                    self.repository.update_message(lead_message.id, self.source_chat_id, self.target_chat_id, target_id)
                    self.logger.info(
                        f"Processed group message {lead_message.id} from {message_date} to {target_id} in topic {target_topic_id}")
                    await asyncio.sleep(0.1)
                return result
        self.logger.error(f"No handler supports media type in group message {lead_message.id} from {message_date}")
        return None

    async def _process_single_message(self, message, source_topic_id):
        """Обрабатывает одиночное сообщение."""
        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')
        msg_record = self.repository.get_message(message.id, self.source_chat_id, self.target_chat_id)
        target_msg_id = msg_record[1] if msg_record else None
        should_reupload = False

        if msg_record and target_msg_id:
            messages = await self.client.client.get_messages(self.target_chat_id, ids=[target_msg_id])
            if not messages or messages[0] is None:
                self.logger.warning(
                    f"Message {message.id} (Target ID: {target_msg_id}) not found in target, marking for reupload")
                should_reupload = True
            elif msg_record[3] == 1:
                self.logger.info(f"Message {message.id} from {message_date} already synced to {target_msg_id}")
                self._store_message_mapping(message.id, target_msg_id)
                return None

        if not msg_record or should_reupload:
            if not msg_record:
                self.repository.add_message(message.id, self.source_chat_id, self.target_chat_id, source_topic_id)
            else:
                self.logger.info(
                    f"Reuploading message {message.id} from {message_date} due to missing target ID {target_msg_id}")

        target_topic_id = self._get_target_topic_id(source_topic_id)

        if message.media:
            result = await self._handle_media(message, target_topic_id)
        elif message.message:
            result = await self._handle_text(message, target_topic_id)
        else:
            self.logger.warning(f"Skipped message {message.id} from {message_date} - no content")
            return None

        if result:
            self.repository.update_message(message.id, self.source_chat_id, self.target_chat_id, result.id)
            self.logger.info(
                f"Processed message {message.id} from {message_date} to {result.id} in topic {target_topic_id}")
            await asyncio.sleep(0.1)
        return result

    def _get_target_topic_id(self, source_topic_id):
        db_topic = self.repository.get_topic(source_topic_id, self.source_chat_id, self.target_chat_id)
        target_topic_id = db_topic[1] if db_topic else None
        if target_topic_id is None:
            self.logger.info(
                f"No target topic mapping found for source topic {source_topic_id}, using {source_topic_id}")
            return source_topic_id
        self.logger.info(f"Using target topic ID {target_topic_id} for source topic {source_topic_id}")
        return target_topic_id

    async def _handle_media(self, message, target_topic_id):
        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')
        for handler in self.handlers:
            if handler.supports(message):
                self.logger.info(
                    f"Selected handler {handler.__class__.__name__} for message {message.id} from {message_date}")
                return await handler.handle(message, target_topic_id)
        self.logger.error(
            f"No handler supports media type in message {message.id} from {message_date}, message dump: {message.__dict__}")
        return None

    async def _handle_text(self, message, target_topic_id):
        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')
        text = self._process_links(message.message)
        self.logger.info(f"Sending text message {message.id} from {message_date}")
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