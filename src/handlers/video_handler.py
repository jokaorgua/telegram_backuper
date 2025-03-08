import os
from telethon.tl.types import DocumentAttributeVideo
from tqdm import tqdm
from .base_handler import BaseMediaHandler


class VideoHandler(BaseMediaHandler):
    def supports(self, message_or_group):
        if isinstance(message_or_group, list):
            messages = message_or_group
        else:
            messages = [message_or_group]

        return all(
            (hasattr(msg.media, 'video') and msg.media.video) or
            (hasattr(msg.media, 'document') and hasattr(msg.media.document, 'mime_type') and
             msg.media.document.mime_type.startswith('video'))
            for msg in messages
        )

    def _is_round_video(self, message):
        return hasattr(message.media, 'round') and message.media.round

    async def _handle_single_video(self, message, target_reply_to_msg_id):
        """Обрабатывает одиночное видео, большие разрезанные видео заливает как альбом."""
        file_path = os.path.join(self.processor.temp_dir, f"media_{message.id}_{self.processor.source_chat_id}.mp4")
        downloaded_path = await self.media_manager.download_media(message, file_path)

        attributes = [DocumentAttributeVideo(
            duration=attr.duration,
            w=attr.w,
            h=attr.h,
            supports_streaming=True
        ) for attr in (
            message.media.document.attributes if hasattr(message.media, 'document') else [message.media.video])
            if isinstance(attr, DocumentAttributeVideo)][0:1]

        self.logger.info("Video media: {}".format(message.media.__dict__))
        is_round = self._is_round_video(message)
        self.logger.info("Round flag: {}".format(is_round))
        file_size = os.path.getsize(downloaded_path)
        file_paths = [downloaded_path]
        if not is_round and file_size > self.processor.MAX_FILE_SIZE:
            self.logger.info(f"File size {file_size} bytes exceeds limit, splitting")
            file_paths = await self.media_manager.split_video(downloaded_path, message.id)
        was_split = len(file_paths) > 1

        sent_messages = []
        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')

        if was_split:
            # Если видео разрезанное, заливаем как альбом с уточнением
            captions = [f"Разрезанное видео. Часть {i}\n{message.message or ''}" for i in range(1, len(file_paths) + 1)]
            self.logger.info(f"Uploading {len(file_paths)} split video parts as album for message {message.id} from {message_date}")
            with tqdm(total=sum(os.path.getsize(part_path) for part_path in file_paths), unit='B', unit_scale=True,
                      desc=f"Uploading split video album for message {message.id}") as pbar:
                def progress_callback(current, total):
                    pbar.update(current - pbar.n)

                sent_message_group = await self.client.bot.send_file(
                    self.target_chat_id,
                    file=file_paths,
                    caption=captions,
                    supports_streaming=True,
                    attributes=attributes,
                    reply_to=target_reply_to_msg_id if target_reply_to_msg_id != 0 else None,
                    formatting_entities=message.entities if message.entities else None,
                    progress_callback=progress_callback
                )
            sent_messages.extend(sent_message_group if isinstance(sent_message_group, list) else [sent_message_group])
            for part_path in file_paths:
                os.remove(part_path)
                self.logger.info(f"Removed temporary file {part_path} for message {message.id} from {message_date}")
        else:
            # Если видео не разрезанное, отправляем как одиночное сообщение
            part_path = file_paths[0]
            part_size = os.path.getsize(part_path)
            self.logger.info(f"Preparing to send video with size {part_size} bytes from {part_path} for message {message.id} from {message_date}")
            if not os.path.exists(part_path):
                self.logger.error(f"File {part_path} does not exist before sending")
                return None

            part_text = message.message or ''
            with tqdm(total=part_size, unit='B', unit_scale=True,
                      desc=f"Uploading {'video note' if is_round else 'video'} for message {message.id}") as pbar:
                def progress_callback(current, total):
                    pbar.update(current - pbar.n)

                if is_round:
                    sent_message = await self.client.bot.send_file(
                        self.target_chat_id,
                        file=part_path,
                        video_note=True,
                        progress_callback=progress_callback
                    )
                else:
                    sent_message = await self.client.bot.send_file(
                        self.target_chat_id,
                        file=part_path,
                        caption=part_text[:1023],
                        supports_streaming=True,
                        attributes=attributes,
                        reply_to=target_reply_to_msg_id if target_reply_to_msg_id != 0 else None,
                        formatting_entities=message.entities if message.entities else None,
                        progress_callback=progress_callback
                    )
            sent_messages.append(sent_message)
            os.remove(part_path)
            self.logger.info(f"Removed temporary file {part_path} for message {message.id} from {message_date}")

        return sent_messages[0] if sent_messages else None

    async def handle(self, message_or_group, target_reply_to_msg_id):
        if isinstance(message_or_group, list):
            messages = message_or_group
            lead_message = messages[0]
            message_date = lead_message.date.strftime('%Y-%m-%d %H:%M:%S')
            self.logger.info(f"Processing video album with {len(messages)} videos for message {lead_message.id} from {message_date}")

            # Собираем информацию о видео
            video_info = []
            for msg in messages:
                file_path = os.path.join(self.processor.temp_dir, f"media_{msg.id}_{self.processor.source_chat_id}.mp4")
                downloaded_path = await self.media_manager.download_media(msg, file_path)
                file_size = os.path.getsize(downloaded_path)
                is_round = self._is_round_video(msg)
                video_info.append({
                    'message': msg,
                    'path': downloaded_path,
                    'size': file_size,
                    'is_round': is_round
                })

            # Разделяем видео на те, что < 2 ГБ и > 2 ГБ
            small_videos = [info for info in video_info if info['size'] <= self.media_manager.TARGET_PART_SIZE]
            large_videos = [info for info in video_info if info['size'] > self.media_manager.TARGET_PART_SIZE]
            sent_messages = []

            # Обрабатываем видео < 2 ГБ как альбом
            if small_videos:
                file_paths = [info['path'] for info in small_videos]
                captions = [info['message'].message or '' for info in small_videos]
                self.logger.info(f"Uploading {len(small_videos)} videos within 2GB as album for message {lead_message.id} from {message_date}")
                with tqdm(total=sum(info['size'] for info in small_videos), unit='B', unit_scale=True,
                          desc=f"Uploading video album for message {lead_message.id}") as pbar:
                    def progress_callback(current, total):
                        pbar.update(current - pbar.n)

                    sent_message_group = await self.client.bot.send_file(
                        self.target_chat_id,
                        file=file_paths,
                        caption=captions,
                        supports_streaming=True,
                        reply_to=target_reply_to_msg_id if target_reply_to_msg_id != 0 else None,
                        formatting_entities=lead_message.entities if lead_message.entities else None,
                        progress_callback=progress_callback
                    )
                sent_messages.extend(sent_message_group if isinstance(sent_message_group, list) else [sent_message_group])
                for info in small_videos:
                    os.remove(info['path'])
                    self.logger.info(f"Removed temporary file {info['path']} for message {info['message'].id} from {message_date}")

            # Обрабатываем видео > 2 ГБ отдельно
            for info in large_videos:
                self.logger.info(f"Processing large video (>2GB) for message {info['message'].id} from {message_date}")
                sent_message = await self._handle_single_video(info['message'], target_reply_to_msg_id)
                if sent_message:
                    sent_messages.append(sent_message)

            return sent_messages[0] if sent_messages else None

        else:
            # Одиночное видео
            return await self._handle_single_video(message_or_group, target_reply_to_msg_id)