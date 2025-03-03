from datetime import datetime
from telethon import events
from telethon.tl.functions.channels import GetForumTopicsRequest, CreateForumTopicRequest, EditForumTopicRequest, \
    GetParticipantRequest
from telethon.errors import RPCError
from telethon.tl.types import ChannelParticipantAdmin
from .message_processor import MessageProcessor
import logging
import asyncio


class Synchronizer:
    def __init__(self, client, source_chat_id, target_chat_id, repository, temp_dir):
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initializing Synchronizer for source {source_chat_id} to target {target_chat_id}")
        self.client = client
        self.source_chat_id = source_chat_id
        self.target_chat_id = target_chat_id
        self.repository = repository
        self.processor = MessageProcessor(client, source_chat_id, target_chat_id, repository, temp_dir)

    async def _check_bot_permissions(self):
        """Проверяет права бота в целевой группе."""
        bot_me = await self.client.bot.get_me()
        try:
            participant = await self.client.bot(GetParticipantRequest(
                channel=self.target_chat_id,
                participant=bot_me.id
            ))
            if not isinstance(participant.participant,
                              ChannelParticipantAdmin) or not participant.participant.admin_rights.manage_topics:
                self.logger.error("Bot lacks 'Manage Topics' permission in target chat")
                raise PermissionError("Bot needs 'Manage Topics' admin permission to create topics")
        except ValueError:
            self.logger.error("Bot is not a participant in the target chat")
            raise PermissionError("Bot must be a participant in the target chat")

    async def _get_source_topics(self):
        """Получает темы из исходной группы."""
        topics = await self.client.client(GetForumTopicsRequest(
            channel=self.source_chat_id,
            offset_date=None,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        source_dict = {t.id: t.title for t in topics.topics} if topics else {}
        self.logger.info("Topics found in source chat:")
        for topic_id, title in source_dict.items():
            self.logger.info(f" - {title} (ID: {topic_id})")
        return source_dict

    async def _get_target_topics(self):
        """Получает темы из целевой группы."""
        topics = await self.client.client(GetForumTopicsRequest(
            channel=self.target_chat_id,
            offset_date=None,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        target_dict = {t.id: t.title for t in topics.topics} if topics else {}
        target_title_to_id = {t.title: t.id for t in topics.topics} if topics else {}
        self.logger.info("Topics found in target chat:")
        for topic_id, title in target_dict.items():
            self.logger.info(f" - {title} (ID: {topic_id})")
        return target_dict, target_title_to_id

    def _get_db_topics(self):
        """Получает темы из базы данных."""
        records = self.repository.get_all_topics()
        db_dict = {row[0]: (row[1], row[2]) for row in records}  # {source_id: (target_id, title)}
        self.logger.info("Topics found in database:")
        for source_id, (target_id, title) in db_dict.items():
            self.logger.info(f" - {title} (Source ID: {source_id}, Target ID: {target_id})")
        return db_dict, records

    async def _create_or_update_topic(self, source_id, source_title, target_id=None):
        """Создаёт новую тему или обновляет маппинг в базе."""
        if target_id:
            try:
                await self.client.bot(
                    EditForumTopicRequest(channel=self.target_chat_id, topic_id=target_id, title=source_title))
                self.logger.debug(
                    f"Topic '{source_title}' (Source ID: {source_id}) already up-to-date in target with ID {target_id}")
                return target_id
            except RPCError as e:
                if "TOPIC_NOT_MODIFIED" in str(e):
                    self.logger.debug(
                        f"Topic '{source_title}' (Source ID: {source_id}) unchanged in target with ID {target_id}")
                    return target_id
                self.logger.warning(f"Topic {target_id} not found in target or invalid: {str(e)}, recreating")

        # Создаём новую тему
        await self.client.bot(CreateForumTopicRequest(channel=self.target_chat_id, title=source_title))
        await asyncio.sleep(1)
        target_topics = await self.client.client(GetForumTopicsRequest(
            channel=self.target_chat_id,
            offset_date=None,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        topic_id = None
        for target_topic in target_topics.topics:
            if target_topic.title == source_title:
                topic_id = target_topic.id
                break
        if topic_id:
            self.repository.update_topic(source_id, topic_id)
            self.logger.info(f"Created topic '{source_title}' (Source ID: {source_id}) in target with ID {topic_id}")
            return topic_id
        else:
            self.logger.warning(
                f"Could not find created topic '{source_title}' in target, using source ID {source_id} as fallback")
            self.repository.update_topic(source_id, source_id)
            return source_id

    async def _rename_deleted_topic(self, target_id, title):
        """Переименовывает удалённую тему в таргете."""
        new_title = f"{title} DELETED"
        try:
            await self.client.bot(
                EditForumTopicRequest(channel=self.target_chat_id, topic_id=target_id, title=new_title))
            self.logger.info(f"Renamed deleted topic to '{new_title}' in target with ID {target_id}")
        except RPCError as e:
            self.logger.warning(f"Failed to rename topic {target_id} to '{new_title}': {str(e)}")

    async def sync_topics(self):
        self.logger.info("Starting topics synchronization")
        try:
            # Проверка прав бота
            await self._check_bot_permissions()

            # 1. Получаем темы из источника
            source_topic_dict = await self._get_source_topics()
            if not source_topic_dict:
                return

            # 2. Получаем темы из таргета
            target_topic_dict, target_title_to_id = await self._get_target_topics()

            # 3. Получаем темы из базы
            db_topic_dict, db_records = self._get_db_topics()

            # 4. Обрабатываем темы из источника
            for source_id, source_title in source_topic_dict.items():
                db_record = db_topic_dict.get(source_id)  # (target_id, title) или None
                target_id = db_record[0] if db_record else None
                db_title = db_record[1] if db_record else None

                # 4.1 Если тема есть в базе и таргете, и всё совпадает
                if db_record and target_id in target_topic_dict and source_title == target_topic_dict[target_id]:
                    self.logger.debug(
                        f"Topic '{source_title}' (Source ID: {source_id}, Target ID: {target_id}) is up-to-date, skipping")
                    continue

                # 4.2 Если тема есть в базе, но ID расходится или недействителен
                elif db_record:
                    if target_id not in target_topic_dict or target_topic_dict[target_id] != source_title:
                        existing_target_id = target_title_to_id.get(source_title)
                        if existing_target_id:
                            self.repository.update_topic(source_id, existing_target_id)
                            self.logger.info(
                                f"Updated mapping for topic '{source_title}' (Source ID: {source_id}) to Target ID {existing_target_id}")
                        else:
                            new_target_id = await self._create_or_update_topic(source_id, source_title)
                            self.logger.info(
                                f"Recreated topic '{source_title}' (Source ID: {source_id}) with new Target ID {new_target_id}")

                # 4.3 Если темы нет в базе
                else:
                    existing_target_id = target_title_to_id.get(source_title)
                    if existing_target_id:
                        self.repository.add_topic(source_id, source_title)
                        self.repository.update_topic(source_id, existing_target_id)
                        self.logger.info(
                            f"Added topic '{source_title}' (Source ID: {source_id}) to database with Target ID {existing_target_id}")
                    else:
                        new_target_id = await self._create_or_update_topic(source_id, source_title)
                        self.logger.info(
                            f"Created topic '{source_title}' (Source ID: {source_id}) with Target ID {new_target_id}")

            # 5. Обрабатываем темы из таргета
            for target_id, target_title in target_topic_dict.items():
                source_id = next((sid for sid, (tid, t) in db_topic_dict.items() if tid == target_id), None)
                if source_id is None and target_title not in source_topic_dict.values():
                    await self._rename_deleted_topic(target_id, target_title)
                    db_record = next((r for r in db_records if r[1] == target_id), None)
                    if db_record:
                        self.repository.delete_topic(db_record[0])
                        self.logger.info(f"Removed topic '{target_title}' (Source ID: {db_record[0]}) from database")

            self.logger.info("Topics synchronization completed")

        except Exception as e:
            self.logger.error(f"Failed to sync topics: {str(e)}")
            raise

    async def listen_new_messages(self):
        self.logger.info("Starting to listen for new messages")

        @self.client.client.on(events.NewMessage(chats=self.source_chat_id))
        async def handler(event):
            self.logger.info(f"Received new message {event.message.id}")
            await self.processor.process_message(event.message)

        await self.client.client.run_until_disconnected()
        self.logger.info("Stopped listening for new messages")