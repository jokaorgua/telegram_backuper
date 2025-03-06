import os
from .base_handler import BaseMediaHandler
from telethon.tl.types import DocumentAttributeVideo
from tqdm import tqdm

class VideoHandler(BaseMediaHandler):
    def supports(self, message):
        supports_video = hasattr(message.media, 'video') and message.media.video == True
        supports_video = supports_video or (hasattr(message.document,
                                                    'mime_type') and message.document.mime_type.startswith('video'))
        self.logger.info(f"VideoHandler supports check: result={supports_video}")
        return supports_video

    def _is_round_video(self, message):
        return hasattr(message.media, 'round') and message.media.round is True

    async def handle(self, message, target_topic_id):
        file_path = os.path.join(self.processor.temp_dir, f"media_{message.id}_{self.processor.source_chat_id}.mp4")
        downloaded_path = await self.media_manager.download_media(message, file_path)

        attributes = [DocumentAttributeVideo(
            duration=attr.duration,
            w=attr.w,
            h=attr.h,
            supports_streaming=True
        ) for attr in (message.media.document.attributes if hasattr(message.media, 'document') else [message.media.video])
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
        for i, part_path in enumerate(file_paths, 1):
            part_size = os.path.getsize(part_path)
            self.logger.info(f"Preparing to send part {i} with size {part_size} bytes from {part_path} for message {message.id} from {message_date}")
            if not os.path.exists(part_path):
                self.logger.error(f"File {part_path} does not exist before sending")
                continue

            part_text = f"Файл разрезан. Часть {i}\n{message.message or ''}" if was_split else message.message or ''
            with tqdm(total=part_size, unit='B', unit_scale=True,
                      desc=f"Uploading {'video note' if is_round else 'video part'} {i} for message {message.id}") as pbar:
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
                        caption=part_text,
                        supports_streaming=True,
                        attributes=attributes,
                        reply_to=target_topic_id if target_topic_id != 0 else None,
                        formatting_entities=message.entities if message.entities else None,
                        progress_callback=progress_callback
                    )
            sent_messages.append(sent_message)
            os.remove(part_path)
            self.logger.info(f"Removed temporary file {part_path} for message {message.id} from {message_date}")

        return sent_messages[0] if sent_messages else None