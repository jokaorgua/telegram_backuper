from datetime import datetime
from telethon import events
from telethon.tl.functions.channels import GetForumTopicsRequest, CreateForumTopicRequest, EditForumTopicRequest, \
    GetParticipantRequest
from telethon.errors import RPCError
import logging
import asyncio

class Synchronizer:
    def __init__(self, client, source_chat_id, target_chat_id, repository, temp_dir, processor):
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initializing Synchronizer for source {source_chat_id} to target {target_chat_id}")
        self.client = client
        self.source_chat_id = source_chat_id
        self.target_chat_id = target_chat_id
        self.repository = repository
        self.temp_dir = temp_dir
        self.processor = processor

    async def _is_forum(self, chat_id):
        try:
            await self.client.client(GetForumTopicsRequest(channel=chat_id, offset_date=None, offset_id=0, offset_topic=0, limit=1))
            return True
        except RPCError:
            return False

    async def _check_bot_permissions(self):
        if not await self._is_forum(self.target_chat_id):
            return
        bot_me = await self.client.bot.get_me()
        try:
            participant = await self.client.bot(GetParticipantRequest(channel=self.target_chat_id, participant=bot_me.id))
            if not participant.participant.admin_rights.manage_topics:
                raise PermissionError("Bot needs 'Manage Topics' admin permission for forums")
        except Exception as e:
            self.logger.error(f"Bot permission check failed: {str(e)}")
            raise

    async def _get_source_topics(self):
        if not await self._is_forum(self.source_chat_id):
            return {}
        topics = await self.client.client(GetForumTopicsRequest(channel=self.source_chat_id, offset_date=None, offset_id=0, offset_topic=0, limit=100))
        return {t.id: t.title for t in topics.topics} if topics else {}

    async def _get_target_topics(self):
        if not await self._is_forum(self.target_chat_id):
            return {}, {}
        topics = await self.client.client(GetForumTopicsRequest(channel=self.target_chat_id, offset_date=None, offset_id=0, offset_topic=0, limit=100))
        target_dict = {t.id: t.title for t in topics.topics} if topics else {}
        target_title_to_id = {t.title: t.id for t in topics.topics} if topics else {}
        return target_dict, target_title_to_id

    def _get_db_topics(self):
        records = self.repository.get_all_topics(self.source_chat_id, self.target_chat_id)
        db_dict = {row[0]: (row[1], row[2]) for row in records}
        return db_dict, records

    async def _create_or_update_topic(self, source_id, source_title, target_id=None):
        if not await self._is_forum(self.target_chat_id):
            return source_id
        if target_id:
            try:
                await self.client.bot(EditForumTopicRequest(channel=self.target_chat_id, topic_id=target_id, title=source_title))
                return target_id
            except RPCError as e:
                if "TOPIC_NOT_MODIFIED" not in str(e):
                    self.logger.warning(f"Topic {target_id} not found or invalid: {str(e)}, recreating")

        await self.client.bot(CreateForumTopicRequest(channel=self.target_chat_id, title=source_title))
        await asyncio.sleep(1)
        target_topics = await self.client.client(GetForumTopicsRequest(channel=self.target_chat_id, offset_date=None, offset_id=0, offset_topic=0, limit=100))
        for target_topic in target_topics.topics:
            if target_topic.title == source_title:
                self.repository.update_topic(source_id, self.source_chat_id, self.target_chat_id, target_topic.id)
                return target_topic.id
        return source_id

    async def sync_history(self, start_date=None):
        async for message in self.client.client.iter_messages(self.source_chat_id, offset_date=start_date, reverse=True):
            await self.processor.process_message(message)
        self.logger.info("Full history sync completed")

    async def sync_threads(self, start_date=None):
        if not await self._is_forum(self.source_chat_id):
            self.logger.info("Source is not a forum, falling back to full sync")
            await self.sync_history(start_date)
            return
        async for message in self.client.client.iter_messages(self.source_chat_id, offset_date=start_date, reverse=True):
            topic_id = 0
            if hasattr(message, 'reply_to') and message.reply_to and message.reply_to.forum_topic:
                topic_id = message.reply_to.reply_to_top_id if message.reply_to.reply_to_top_id else message.reply_to.reply_to_msg_id
            if topic_id != 0:
                await self.processor.process_message(message)
        self.logger.info("Threads-only sync completed")

    async def sync_thread(self, topic_id, start_date=None):
        if not await self._is_forum(self.source_chat_id):
            self.logger.info("Source is not a forum, falling back to full sync")
            await self.sync_history(start_date)
            return
        async for message in self.client.client.iter_messages(self.source_chat_id, offset_date=start_date, reverse=True, reply_to=topic_id):
            source_topic_id = 0
            if hasattr(message, 'reply_to') and message.reply_to and message.reply_to.forum_topic:
                source_topic_id = message.reply_to.reply_to_top_id if message.reply_to.reply_to_top_id else message.reply_to.reply_to_msg_id
            if source_topic_id == topic_id:
                await self.processor.process_message(message)
        self.logger.info(f"Thread {topic_id} sync completed")

    async def sync_topics(self):
        if not await self._is_forum(self.source_chat_id) or not await self._is_forum(self.target_chat_id):
            self.logger.info("Skipping topic sync: source or target is not a forum")
            return
        await self._check_bot_permissions()
        source_topic_dict = await self._get_source_topics()
        if not source_topic_dict:
            return
        target_topic_dict, target_title_to_id = await self._get_target_topics()
        db_topic_dict, db_records = self._get_db_topics()

        for source_id, source_title in source_topic_dict.items():
            db_record = db_topic_dict.get(source_id)
            target_id = db_record[0] if db_record else None
            if db_record and target_id in target_topic_dict and source_title == target_topic_dict[target_id]:
                continue
            elif db_record:
                existing_target_id = target_title_to_id.get(source_title)
                if existing_target_id:
                    self.repository.update_topic(source_id, self.source_chat_id, self.target_chat_id, existing_target_id)
                else:
                    await self._create_or_update_topic(source_id, source_title)
            else:
                existing_target_id = target_title_to_id.get(source_title)
                if existing_target_id:
                    self.repository.add_topic(source_id, self.source_chat_id, self.target_chat_id, source_title)
                    self.repository.update_topic(source_id, self.source_chat_id, self.target_chat_id, existing_target_id)
                else:
                    await self._create_or_update_topic(source_id, source_title)

    async def listen_new_messages(self):
        @self.client.client.on(events.NewMessage(chats=self.source_chat_id))
        async def handler(event):
            await self.processor.process_message(event.message)
        await self.client.client.run_until_disconnected()