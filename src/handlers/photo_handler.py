# src/handlers/photo_handler.py
import os
from .base_handler import BaseMediaHandler

class PhotoHandler(BaseMediaHandler):
    def supports(self, message_or_group):
        if isinstance(message_or_group, list):
            messages = message_or_group
        else:
            messages = [message_or_group]

        return all(
            hasattr(msg.media, 'photo') or
            (hasattr(msg.media, 'document') and hasattr(msg.media.document, 'mime_type') and
             msg.media.document.mime_type.startswith('image'))
            for msg in messages
        )

    async def handle(self, message_or_group, target_reply_to_msg_id):
        if isinstance(message_or_group, list):
            messages = message_or_group
            lead_message = messages[0]
            message_date = lead_message.date.strftime('%Y-%m-%d %H:%M:%S')
            file_paths = []
            text_parts = []

            for msg in messages:
                file_path = os.path.join(self.processor.temp_dir, f"media_{msg.id}_{self.processor.source_chat_id}.jpg")
                downloaded_path = await self.media_manager.download_media(msg, file_path)
                file_paths.append(downloaded_path)
                self.logger.info(f"Downloaded photo {msg.id} from {message_date} to {downloaded_path}")
                if msg.message and msg.message.strip():
                    text_parts.append(msg.message.strip())
                    self.logger.info(f"Found text in message {msg.id}: '{msg.message.strip()}'")

            original_text = '\n'.join(text_parts) or ''
            part_text = original_text[:self.caption_limit]
            if len(original_text) > self.caption_limit:
                self.logger.info(f"Caption for message {lead_message.id} truncated from {len(original_text)} to {self.caption_limit} characters")
            entities = lead_message.entities if lead_message.entities else None
            if entities:
                entities = self._adjust_entities(original_text, part_text, entities)

            self.logger.info(f"Sending group of {len(file_paths)} photos for message {lead_message.id} from {message_date} with message: '{part_text}'")
            sent_message = await self.client.bot.send_message(
                self.target_chat_id,
                message=part_text,
                file=file_paths,
                force_document=False,
                reply_to=target_reply_to_msg_id if target_reply_to_msg_id != 0 else None,
                formatting_entities=entities
            )

            for file_path in file_paths:
                os.remove(file_path)
                self.logger.info(f"Removed temporary file {file_path} for group message {lead_message.id} from {message_date}")
            return sent_message

        message = message_or_group
        file_path = os.path.join(self.processor.temp_dir, f"media_{message.id}_{self.processor.source_chat_id}.jpg")
        downloaded_path = await self.media_manager.download_media(message, file_path)

        original_text = message.message or ''
        part_text = original_text[:self.caption_limit]
        if len(original_text) > self.caption_limit:
            self.logger.info(f"Caption for message {message.id} truncated from {len(original_text)} to {self.caption_limit} characters")
        entities = message.entities if message.entities else None
        if entities:
            entities = self._adjust_entities(original_text, part_text, entities)

        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(f"Sending photo for message {message.id} from {message_date} from {downloaded_path} with message: '{part_text}'")
        sent_message = await self.client.bot.send_message(
            self.target_chat_id,
            message=part_text,
            file=downloaded_path,
            force_document=False,
            reply_to=target_reply_to_msg_id if target_reply_to_msg_id != 0 else None,
            formatting_entities=entities
        )
        os.remove(downloaded_path)
        self.logger.info(f"Removed temporary file {downloaded_path} for message {message.id} from {message_date}")
        return sent_message