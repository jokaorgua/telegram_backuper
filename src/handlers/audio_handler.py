# src/handlers/audio_handler.py
import os
from .base_handler import BaseMediaHandler
from telethon.tl.types import DocumentAttributeAudio
from tqdm import tqdm

class AudioHandler(BaseMediaHandler):
    def supports(self, message_or_group):
        if isinstance(message_or_group, list):
            messages = message_or_group
        else:
            messages = [message_or_group]

        return all(
            (hasattr(msg.media, 'voice') and msg.media.voice == True) or
            (hasattr(msg.media, 'document') and hasattr(msg.media.document, 'mime_type') and
             msg.media.document.mime_type.startswith('audio'))
            for msg in messages
        )

    async def handle(self, message_or_group, target_reply_to_msg_id):
        # Пока AudioHandler не поддерживает группы, только одиночные сообщения
        message = message_or_group
        file_path = os.path.join(self.processor.temp_dir, f"media_{message.id}_{self.processor.source_chat_id}.mp3")
        downloaded_path = await self.media_manager.download_media(message, file_path)

        attributes = [
            DocumentAttributeAudio(
                duration=attr.duration if attr.duration else 0,
                voice=True,
                title=None,
                performer=None
            ) for attr in message.media.document.attributes if isinstance(attr, DocumentAttributeAudio)
        ] or [DocumentAttributeAudio(duration=0, voice=True)]

        original_text = message.message or ''
        part_text = original_text[:self.caption_limit]
        if len(original_text) > self.caption_limit:
            self.logger.info(f"Caption for message {message.id} truncated from {len(original_text)} to {self.caption_limit} characters")
        adjusted_entities = self._adjust_entities(original_text, part_text, message.entities)

        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')
        file_size = os.path.getsize(downloaded_path)
        with tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Uploading voice note {message.id}") as pbar:
            def progress_callback(current, total):
                pbar.update(current - pbar.n)

            self.logger.info(f"Sending voice note for message {message.id} from {message_date} from {downloaded_path} with attributes: {attributes}")
            sent_message = await self.client.bot.send_file(
                self.target_chat_id,
                file=downloaded_path,
                caption=part_text,
                voice_note=True,
                attributes=attributes,
                reply_to=target_reply_to_msg_id if target_reply_to_msg_id != 0 else None,
                formatting_entities=adjusted_entities,
                progress_callback=progress_callback
            )
        os.remove(downloaded_path)
        self.logger.info(f"Removed temporary file {downloaded_path} for message {message.id} from {message_date}")
        return sent_message