# src/handlers/mixed_media_handler.py
import os
from .base_handler import BaseMediaHandler
from tqdm import tqdm

class MixedMediaHandler(BaseMediaHandler):
    def supports(self, message_or_group):
        if not isinstance(message_or_group, list):
            return False

        has_photo = any(
            hasattr(msg.media, 'photo') or
            (hasattr(msg.media, 'document') and hasattr(msg.media.document, 'mime_type') and
             msg.media.document.mime_type.startswith('image'))
            for msg in message_or_group
        )
        has_video = any(
            hasattr(msg.media, 'video') or
            (hasattr(msg.media, 'document') and hasattr(msg.media.document, 'mime_type') and
             msg.media.document.mime_type.startswith('video'))
            for msg in message_or_group
        )
        has_audio = any(
            hasattr(msg.media, 'voice') or
            (hasattr(msg.media, 'document') and hasattr(msg.media.document, 'mime_type') and
             msg.media.document.mime_type.startswith('audio'))
            for msg in message_or_group
        )

        supports_mixed = sum([has_photo, has_video, has_audio]) >= 2
        self.logger.info(f"MixedMediaHandler supports check: has_photo={has_photo}, has_video={has_video}, has_audio={has_audio}, result={supports_mixed}")
        return supports_mixed

    async def handle(self, messages, target_reply_to_msg_id):
        lead_message = messages[0]
        message_date = lead_message.date.strftime('%Y-%m-%d %H:%M:%S')
        text_parts = [msg.message.strip() for msg in messages if msg.message and msg.message.strip()]
        original_text = '\n'.join(text_parts) or ''
        part_text = original_text[:self.caption_limit]
        if len(original_text) > self.caption_limit:
            self.logger.info(f"Caption for message {lead_message.id} truncated from {len(original_text)} to {self.caption_limit} characters")
        entities = next((msg.entities for msg in messages if msg.entities), None)
        if entities:
            entities = self._adjust_entities(original_text, part_text, entities)

        file_paths = []
        for msg in messages:
            if hasattr(msg.media, 'photo') or (hasattr(msg.media, 'document') and hasattr(msg.media.document, 'mime_type') and
                                              msg.media.document.mime_type.startswith('image')):
                file_path = os.path.join(self.processor.temp_dir, f"media_{msg.id}_{self.processor.source_chat_id}.jpg")
            elif hasattr(msg.media, 'video') or (hasattr(msg.media, 'document') and hasattr(msg.media.document, 'mime_type') and
                                                msg.media.document.mime_type.startswith('video')):
                file_path = os.path.join(self.processor.temp_dir, f"media_{msg.id}_{self.processor.source_chat_id}.mp4")
            elif hasattr(msg.media, 'voice') or (hasattr(msg.media, 'document') and hasattr(msg.media.document, 'mime_type') and
                                                msg.media.document.mime_type.startswith('audio')):
                file_path = os.path.join(self.processor.temp_dir, f"media_{msg.id}_{self.processor.source_chat_id}.mp3")
            else:
                continue

            downloaded_path = await self.media_manager.download_media(msg, file_path)
            file_paths.append(downloaded_path)
            self.logger.info(f"Downloaded mixed media {msg.id} from {message_date} to {downloaded_path}")

        sent_messages = []
        total_size = sum(os.path.getsize(f) for f in file_paths)
        self.logger.info(f"Sending group of {len(file_paths)} mixed media for message {lead_message.id} from {message_date} with message: '{part_text}'")
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Uploading mixed media for message {lead_message.id}") as pbar:
            def progress_callback(current, total):
                pbar.update(current - pbar.n)

            sent_message = await self.client.bot.send_message(
                self.target_chat_id,
                message=part_text,
                file=file_paths,
                force_document=False,
                reply_to=target_reply_to_msg_id if target_reply_to_msg_id != 0 else None,
                formatting_entities=entities,
                supports_streaming=True,
                progress_callback=progress_callback
            )
            sent_messages.append(sent_message)

        for file_path in file_paths:
            os.remove(file_path)
            self.logger.info(f"Removed temporary file {file_path} for group message {lead_message.id}")

        return sent_messages[0] if sent_messages else None