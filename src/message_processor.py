# src/message_processor.py
import asyncio
import logging

from .media_manager import MediaManager


class MessageProcessor:
    def __init__(self, client, source_chat_id, target_chat_id, repository, temp_dir, handlers, caption_limit):
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initializing MessageProcessor for source {source_chat_id} to target {target_chat_id}")
        self.client = client
        self.source_chat_id = source_chat_id
        self.target_chat_id = target_chat_id
        self.repository = repository
        self.temp_dir = temp_dir
        self.caption_limit = caption_limit  # Новый параметр
        self.message_map = {}
        self.media_manager = MediaManager(client, temp_dir)
        self.handlers = [handler(self) for handler in handlers]  # Передаем self с caption_limit в хендлеры
        self.PART_SIZE = 512 * 1024
        self.MAX_PARTS = 4000
        self.MAX_FILE_SIZE = self.PART_SIZE * self.MAX_PARTS
        self.processed_group_ids = set()

    async def process_message(self, message):
        source_reply_to_msg_id = 0
        source_reply_to_top_id = 0
        if hasattr(message, 'reply_to') and message.reply_to:
            source_reply_to_msg_id = message.reply_to.reply_to_msg_id
            if getattr(message.reply_to, 'forum_topic', False) and message.reply_to.reply_to_top_id:
                source_reply_to_top_id = message.reply_to.reply_to_top_id

        if self.source_chat_id not in self.message_map:
            self.message_map[self.source_chat_id] = {}
        if self.target_chat_id not in self.message_map[self.source_chat_id]:
            self.message_map[self.source_chat_id][self.target_chat_id] = {}
        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(f"Processing message {message.id} from {message_date} with reply_to {source_reply_to_msg_id}")

        if message.id in self.processed_group_ids:
            self.logger.info(
                f"Skipping message {message.id} from {message_date} - already processed as part of a group")
            return None

        if message.grouped_id:
            self.logger.info(f"Message {message.id} is part of group {message.grouped_id}, collecting group messages")
            group_messages = await self._collect_group_messages(message)
            if group_messages:
                return await self._process_group_messages(group_messages, source_reply_to_msg_id,
                                                          source_reply_to_top_id)

        return await self._process_single_message(message, source_reply_to_msg_id, source_reply_to_top_id)

    async def _collect_group_messages(self, message):
        grouped_id = message.grouped_id
        group_messages = [message]
        self.logger.info(f"Collecting group messages for grouped_id {grouped_id} starting from message {message.id}")

        async for msg in self.client.client.iter_messages(
                self.source_chat_id,
                min_id=message.id - 1,
                limit=30,
                reverse=True
        ):
            if msg.grouped_id == grouped_id and msg.id != message.id:
                group_messages.append(msg)
                self.logger.info(f"Added message {msg.id} to group {grouped_id}")

        return group_messages if len(group_messages) > 1 else None

    async def _process_group_messages(self, messages, source_reply_to_msg_id, source_reply_to_top_id):
        lead_message = messages[0]
        message_date = lead_message.date.strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(
            f"Processing group of {len(messages)} messages with lead ID {lead_message.id} from {message_date}")

        for msg in messages:
            self.processed_group_ids.add(msg.id)

        # Проверяем, синхронизирована ли группа (достаточно проверить ведущий ID)
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
                # Маппинг всех ID группы на target_msg_id
                for msg in messages:
                    self._store_message_mapping(msg.id, target_msg_id)
                    # Убеждаемся, что все ID группы есть в базе
                    if not self.repository.get_message(msg.id, self.source_chat_id, self.target_chat_id):
                        self.repository.add_message(msg.id, self.source_chat_id, self.target_chat_id,
                                                    source_reply_to_msg_id)
                        self.repository.update_message(msg.id, self.source_chat_id, self.target_chat_id, target_msg_id)
                return None

        # Если записи нет или требуется перезаливка
        if not msg_record or should_reupload:
            # Добавляем все сообщения группы в базу, если их там нет
            for msg in messages:
                if not self.repository.get_message(msg.id, self.source_chat_id, self.target_chat_id):
                    self.repository.add_message(msg.id, self.source_chat_id, self.target_chat_id,
                                                source_reply_to_msg_id)
            if should_reupload:
                self.logger.info(
                    f"Reuploading group message {lead_message.id} from {message_date} due to missing target ID {target_msg_id}")

        target_reply_to_msg_id = self._get_target_reply_to_msg_id(source_reply_to_msg_id, source_reply_to_top_id)

        for handler in self.handlers:
            if handler.supports(messages):
                self.logger.info(
                    f"Selected handler {handler.__class__.__name__} for group message {lead_message.id} from {message_date}")
                result = await handler.handle(messages, target_reply_to_msg_id)  # Здесь уже всё передано через self
                if result:
                    target_id = result[0].id if isinstance(result, list) else result.id
                    # Обновляем базу и маппинг для всех сообщений группы
                    for msg in messages:
                        self.repository.update_message(msg.id, self.source_chat_id, self.target_chat_id, target_id)
                        self._store_message_mapping(msg.id, target_id)
                    self.logger.info(
                        f"Processed group message {lead_message.id} from {message_date} to {target_id} with reply_to {target_reply_to_msg_id}")
                    await asyncio.sleep(0.1)
                return result
        self.logger.error(f"No handler supports media type in group message {lead_message.id} from {message_date}")
        return None

    async def _process_single_message(self, message, source_reply_to_msg_id, source_reply_to_top_id):
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
                self.repository.add_message(message.id, self.source_chat_id, self.target_chat_id,
                                            source_reply_to_msg_id)
            else:
                self.logger.info(
                    f"Reuploading message {message.id} from {message_date} due to missing target ID {target_msg_id}")

        target_reply_to_msg_id = self._get_target_reply_to_msg_id(source_reply_to_msg_id, source_reply_to_top_id)

        if message.media:
            result = await self._handle_media(message, target_reply_to_msg_id)
        elif message.message:
            result = await self._handle_text(message, target_reply_to_msg_id)
        else:
            self.logger.warning(f"Skipped message {message.id} from {message_date} - no content")
            return None

        if result:
            self.repository.update_message(message.id, self.source_chat_id, self.target_chat_id, result.id)
            self._store_message_mapping(message.id, result.id)
            self.logger.info(
                f"Processed message {message.id} from {message_date} to {result.id} with reply_to {target_reply_to_msg_id}")
            await asyncio.sleep(0.1)
        return result

    def _get_target_reply_to_msg_id(self, source_reply_to_msg_id, source_reply_to_top_id):
        # 1. Если source_reply_to_msg_id is None, возвращаем 0
        if source_reply_to_msg_id is None and source_reply_to_top_id is None:
            self.logger.info("source_reply_to_msg_id is None and source_reply_to_top_id is None, returning 0")
            return None

        # 3. Проверяем в кеше message_map
        if source_reply_to_msg_id in self.message_map.get(self.source_chat_id, {}).get(self.target_chat_id, {}):
            target_reply_to_msg_id = self.message_map[self.source_chat_id][self.target_chat_id][source_reply_to_msg_id]
            self.logger.info(
                f"Mapped source_reply_to_msg_id {source_reply_to_msg_id} to target {target_reply_to_msg_id} from cache")
            return target_reply_to_msg_id

        # 4. Проверяем в репозитории, если данных нет в кеше
        msg_record = self.repository.get_message(source_reply_to_msg_id, self.source_chat_id, self.target_chat_id)
        if msg_record and msg_record[1]:  # msg_record[1] - target_id
            target_reply_to_msg_id = msg_record[1]
            # Записываем в кеш
            if self.source_chat_id not in self.message_map:
                self.message_map[self.source_chat_id] = {}
            if self.target_chat_id not in self.message_map[self.source_chat_id]:
                self.message_map[self.source_chat_id][self.target_chat_id] = {}
            self.message_map[self.source_chat_id][self.target_chat_id][source_reply_to_msg_id] = target_reply_to_msg_id
            self.logger.info(
                f"Mapped source_reply_to_msg_id {source_reply_to_msg_id} to target {target_reply_to_msg_id} from repository, added to cache")
            return target_reply_to_msg_id

        if source_reply_to_top_id is not None and source_reply_to_top_id != 0:
            # 2. Проверяем в репозитории наличие топика по source_reply_to_msg_id
            db_topic = self.repository.get_topic(source_reply_to_top_id, self.source_chat_id, self.target_chat_id)
            if db_topic and db_topic[1]:  # msg_record[2] - topic_id
                self.logger.info(
                    f"Found topic {db_topic[1]} in repository for source_reply_to_top_id {source_reply_to_msg_id}")
                return db_topic[1]

        if source_reply_to_msg_id is not None and source_reply_to_msg_id != 0:
            # 2. Проверяем в репозитории наличие топика по source_reply_to_msg_id
            db_topic = self.repository.get_topic(source_reply_to_msg_id, self.source_chat_id, self.target_chat_id)
            if db_topic and db_topic[1]:  # msg_record[2] - topic_id
                self.logger.info(
                    f"Found topic {db_topic[1]} in repository for source_reply_to_msg_id {source_reply_to_msg_id}")
                return db_topic[1]

        self.logger.info(f"No mapping found for reply_to_msg_id {source_reply_to_msg_id}, returning 0")
        return None

    async def _handle_media(self, message, target_reply_to_msg_id):
        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')
        for handler in self.handlers:
            if handler.supports(message):
                self.logger.info(
                    f"Selected handler {handler.__class__.__name__} for message {message.id} from {message_date}")
                return await handler.handle(message, target_reply_to_msg_id)  # Здесь уже всё передано через self
        self.logger.error(
            f"No handler supports media type in message {message.id} from {message_date}, message dump: {message.__dict__}")
        return None

    async def _handle_text(self, message, target_reply_to_msg_id):
        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')
        text = self._process_links(message.message)
        self.logger.info(f"Sending text message {message.id} from {message_date}")
        sent_message = await self.client.bot.send_message(
            self.target_chat_id,
            text,
            reply_to=target_reply_to_msg_id if target_reply_to_msg_id != 0 else None,
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