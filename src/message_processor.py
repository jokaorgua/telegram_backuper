from telethon.tl.types import Message, DocumentAttributeVideo, InputMediaUploadedDocument
from telethon import utils
from tqdm import tqdm
import logging
import os
import asyncio
import ffmpeg
import math


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
        # Лимиты Telegram для Telethon 1.39.0
        self.PART_SIZE = 512 * 1024  # 512 КБ
        self.MAX_PARTS = 4000  # Максимум 4,000 частей
        self.MAX_FILE_SIZE = self.PART_SIZE * self.MAX_PARTS  # 2 ГБ
        self.TARGET_PART_SIZE = 1.9 * 1024 * 1024 * 1024  # 1.9 ГБ для частей

    async def process_message(self, message: Message):
        source_topic_id = 0
        if hasattr(message, 'reply_to') and message.reply_to and message.reply_to.forum_topic:
            source_topic_id = message.reply_to.reply_to_top_id if message.reply_to.reply_to_top_id else message.reply_to.reply_to_msg_id

        if source_topic_id not in self.message_map:
            self.message_map[source_topic_id] = {}
        self.logger.info(f"Processing message {message.id} in topic {source_topic_id}")

        msg_record = self.repository.get_message(message.id)
        target_msg_id = msg_record[1] if msg_record else None
        should_reupload = False

        if msg_record and target_msg_id:
            messages = await self.client.client.get_messages(self.target_chat_id, ids=[target_msg_id])
            if not messages or messages[0] is None:
                self.logger.warning(
                    f"Message {message.id} (Target ID: {target_msg_id}) not found in target, marking for reupload")
                should_reupload = True
            elif msg_record[3] == 1:
                self.logger.debug(f"Message {message.id} already synced to {target_msg_id} and exists in target")
                self._store_message_mapping(source_topic_id, message.id, target_msg_id)
                return None

        if not msg_record or should_reupload:
            if not msg_record:
                self.repository.add_message(message.id, source_topic_id)
            else:
                self.logger.info(f"Reuploading message {message.id} due to missing target ID {target_msg_id}")

        target_topic_id = self._get_target_topic_id(source_topic_id)

        if message.media:
            result = await self._handle_media(message, source_topic_id, target_topic_id)
        elif message.message:
            result = await self._handle_text(message, source_topic_id, target_topic_id)
        else:
            self.logger.warning(f"Skipped message {message.id} - no content")
            return None

        if result:
            self.repository.update_message(message.id, result.id)
            self.logger.info(f"Processed message {message.id} to {result.id} in topic {target_topic_id}")
            await asyncio.sleep(0.1)  # Задержка 100 мс
        return result

    def _get_target_topic_id(self, source_topic_id: int) -> int:
        db_topics = {row[0]: row[1] for row in self.repository.get_all_topics()}
        target_topic_id = db_topics.get(source_topic_id)
        if target_topic_id is None:
            self.logger.warning(
                f"No target topic mapping found for source topic {source_topic_id}, using source ID as fallback")
            return source_topic_id
        self.logger.debug(f"Using target topic ID {target_topic_id} for source topic {source_topic_id}")
        return target_topic_id

    async def _download_progress_callback(self, downloaded, total, message_id):
        if not hasattr(self, '_download_pbar') or self._download_pbar.total != total:
            if hasattr(self, '_download_pbar'):
                self._download_pbar.close()
            self._download_pbar = tqdm(total=total, desc=f"Downloading media for message {message_id}", unit="B",
                                       unit_scale=True)
        self._download_pbar.update(downloaded - self._download_pbar.n)

    async def _upload_progress_callback(self, uploaded, total, message_id):
        if not hasattr(self, '_upload_pbar') or self._upload_pbar.total != total:
            if hasattr(self, '_upload_pbar'):
                self._upload_pbar.close()
            self._upload_pbar = tqdm(total=total, desc=f"Uploading media for message {message_id}", unit="B",
                                     unit_scale=True)
        self._upload_pbar.update(uploaded - self._upload_pbar.n)

    async def _download_media_with_progress(self, message: Message) -> str:
        """Скачивает медиа с поддержкой докачки и прогресс-бара."""
        file_path = os.path.join(self.temp_dir, f"media_{message.id}_{self.source_chat_id}.mp4")
        file_size = message.media.document.size if hasattr(message.media, 'document') else message.media.size
        self.logger.info(f"Starting download of media {message.id} from {self.source_chat_id}, size: {file_size} bytes")

        current_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        if current_size >= file_size:
            self.logger.info(f"Media {message.id} already fully downloaded at {file_path}")
            return file_path

        input_file = message.media.document if hasattr(message.media, 'document') else message.media
        with tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Downloading media {message.id}",
                  initial=current_size) as pbar:
            with open(file_path, 'ab' if current_size > 0 else 'wb') as fd:
                if current_size > 0:
                    fd.seek(current_size)
                    self.logger.info(f"Resuming download from offset {current_size}")
                async for chunk in self.client.client.iter_download(
                        input_file,
                        offset=current_size,
                        chunk_size=1024 * 1024  # 1 MB chunks для скачивания
                ):
                    try:
                        fd.write(chunk)
                        pbar.update(len(chunk))
                    except Exception as e:
                        self.logger.error(
                            f"Download interrupted for {message.id} at offset {current_size + pbar.n}: {str(e)}")
                        raise

        downloaded_size = os.path.getsize(file_path)
        if downloaded_size != file_size:
            self.logger.error(f"Download incomplete for {message.id}: {downloaded_size}/{file_size} bytes")
            raise ValueError("File size mismatch after download")

        self.logger.info(f"Media downloaded to {file_path}")
        return file_path

    async def _split_video(self, input_path: str, message_id: int) -> list:
        """Разрезает видео на части меньше 2 ГБ с помощью ffmpeg."""
        file_size = os.path.getsize(input_path)
        if file_size <= self.MAX_FILE_SIZE:
            return [input_path]  # Нет необходимости резать

        # Получаем длительность видео
        probe = ffmpeg.probe(input_path)
        duration = float(probe['format']['duration'])
        part_size_bytes = self.TARGET_PART_SIZE  # 1.9 ГБ
        num_parts = math.ceil(file_size / part_size_bytes)
        part_duration = duration / num_parts

        output_files = []
        for i in range(num_parts):
            output_file = os.path.join(self.temp_dir, f"media_{message_id}_{self.source_chat_id}_part{i + 1}.mp4")
            try:
                self.logger.info(f"Cutting part {i + 1} of {num_parts} for message {message_id}")
                stream = ffmpeg.input(input_path, ss=i * part_duration, t=part_duration)
                stream = ffmpeg.output(stream, output_file, c='copy', f='mp4', map_metadata='-1', reset_timestamps=1,
                                       loglevel='quiet')
                ffmpeg.run(stream)
                output_files.append(output_file)
            except ffmpeg.Error as e:
                self.logger.error(f"Failed to cut part {i + 1} for message {message_id}: {str(e)}")
                raise

        return output_files

    async def _handle_media(self, message: Message, source_topic_id: int, target_topic_id: int):
        self.logger.debug(f"Downloading media for message {message.id}")
        file_path = await self._download_media_with_progress(message)

        self.logger.debug(f"Sending media for message {message.id} from {file_path}")
        attributes = []
        duration = w = h = 0
        if hasattr(message.media, 'document'):
            for attr in message.media.document.attributes:
                if isinstance(attr, DocumentAttributeVideo):
                    attributes.append(DocumentAttributeVideo(
                        duration=attr.duration,
                        w=attr.w,
                        h=attr.h,
                        supports_streaming=True
                    ))
                    duration, w, h = attr.duration, attr.w, attr.h
                    break
        elif hasattr(message.media, 'video'):
            attributes.append(DocumentAttributeVideo(
                duration=message.media.video.duration,
                w=message.media.video.w,
                h=message.media.video.h,
                supports_streaming=True
            ))
            duration, w, h = message.media.video.duration, message.media.video.w, message.media.video.h

        # Считаем размер файла и количество частей
        file_size = os.path.getsize(file_path)
        parts = (file_size + self.PART_SIZE - 1) // self.PART_SIZE  # Округление вверх
        self.logger.debug(f"File size: {file_size} bytes, Parts: {parts}")

        # Если файл больше 2 ГБ, режем его
        file_paths = [file_path]
        was_split = False
        if file_size > self.MAX_FILE_SIZE:
            self.logger.info(
                f"File size {file_size} bytes exceeds {self.MAX_FILE_SIZE} bytes limit. Splitting into parts.")
            file_paths = await self._split_video(file_path, message.id)
            was_split = True
            # Оригинал не удаляем, если разрезка успешна он остаётся в темпе

        # Загружаем каждую часть
        sent_messages = []
        for i, part_path in enumerate(file_paths, 1):
            part_size = os.path.getsize(part_path)
            part_parts = (part_size + self.PART_SIZE - 1) // self.PART_SIZE
            self.logger.debug(f"Processing part {i}: Size {part_size} bytes, Parts: {part_parts}")

            uploaded_file = None
            # 1. Пробуем загрузить ботом
            try:
                if part_parts <= self.MAX_PARTS:
                    self.logger.info(
                        f"Attempting upload of part {i} ({part_size} bytes) with bot (parts: {part_parts})")
                    with open(part_path, 'rb') as f:
                        uploaded_file = await self.client.bot.upload_file(
                            f,
                            progress_callback=lambda u, t: asyncio.ensure_future(
                                self._upload_progress_callback(u, t, message.id))
                        )
                else:
                    raise ValueError(
                        f"Part {i} size {part_size} bytes exceeds limit of {self.MAX_FILE_SIZE} bytes ({self.MAX_PARTS} parts)")

            except Exception as bot_error:
                self.logger.debug(f"Bot upload failed for part {i}: {str(bot_error)}. Attempting with account.")
                # 2. Пробуем загрузить аккаунтом
                try:
                    if part_parts <= self.MAX_PARTS:
                        self.logger.info(
                            f"Attempting upload of part {i} ({part_size} bytes) with account (parts: {part_parts})")
                        with open(part_path, 'rb') as f:
                            uploaded_file = await self.client.client.upload_file(
                                f,
                                progress_callback=lambda u, t: asyncio.ensure_future(
                                    self._upload_progress_callback(u, t, message.id))
                            )
                    else:
                        raise ValueError(
                            f"Part {i} size {part_size} bytes exceeds limit of {self.MAX_FILE_SIZE} bytes ({self.MAX_PARTS} parts)")

                except Exception as account_error:
                    self.logger.error(
                        f"Failed to upload part {i} for message {message.id} with both bot and account: {str(account_error)}. Skipping part, file retained in temp.")
                    continue  # Не удаляем файл, если загрузка не удалась

            # Отправляем часть с текстом
            if uploaded_file:
                # Используем "Файл разрезан" только если файл был разрезан
                part_text = f"Файл разрезан. Часть {i}\n{message.message or ''}" if was_split else message.message or ''
                try:
                    sent_message = await self.client.bot.send_message(
                        self.target_chat_id,
                        message=part_text,
                        file=InputMediaUploadedDocument(
                            file=uploaded_file,
                            mime_type="video/mp4",
                            attributes=attributes,
                            thumb=None
                        ),
                        reply_to=self._get_reply_id(message, source_topic_id) or (
                            target_topic_id if target_topic_id != 0 else None),
                        formatting_entities=message.entities,
                    )
                    if hasattr(self, '_upload_pbar'):
                        self._upload_pbar.close()
                        delattr(self, '_upload_pbar')

                    self.logger.debug(
                        f"Sent part {i} as message {sent_message.id} with attributes: {sent_message.media.document.attributes if hasattr(sent_message.media, 'document') else 'N/A'}")
                    sent_messages.append(sent_message)
                    os.remove(part_path)  # Удаляем только после успешной загрузки
                except Exception as e:
                    self.logger.error(
                        f"Failed to send part {i} for message {message.id}: {str(e)}. File retained in temp.")
                    # Не удаляем файл, если отправка не удалась

        # Если хотя бы одна часть загружена, возвращаем первое сообщение
        return sent_messages[0] if sent_messages else None

    async def _handle_text(self, message: Message, source_topic_id: int, target_topic_id: int):
        text = self._process_links(message.message, source_topic_id)
        self.logger.debug(f"Sending text message {message.id}")
        sent_message = await self.client.bot.send_message(
            self.target_chat_id,
            text,
            reply_to=self._get_reply_id(message, source_topic_id) or (
                target_topic_id if target_topic_id != 0 else None),
            link_preview=False,
            formatting_entities=message.entities
        )
        self._store_message_mapping(source_topic_id, message.id, sent_message.id)
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
        return None

    def _store_message_mapping(self, topic_id: int, source_id: int, target_id: int):
        self.logger.debug(f"Mapping source {source_id} to target {target_id} in topic {topic_id}")
        self.message_map[topic_id][source_id] = target_id