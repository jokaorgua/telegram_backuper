import os
from .base_handler import BaseMediaHandler
from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeVideo
from tqdm import tqdm

class AudioHandler(BaseMediaHandler):
    def supports(self, message):
        supports_audio = hasattr(message.media, 'voice') and message.media.voice == True
        supports_audio = supports_audio or (hasattr(message.document,
                                                    'mime_type') and message.document.mime_type.startswith('audio'))
        self.logger.info(f"AudioHandler supports check: result={supports_audio}")
        return supports_audio

    async def handle(self, message, target_topic_id):
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

        part_text = message.message or ''
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
                reply_to=target_topic_id if target_topic_id != 0 else None,
                formatting_entities=message.entities if message.entities else None,
                progress_callback=progress_callback
            )
        os.remove(downloaded_path)
        self.logger.info(f"Removed temporary file {downloaded_path} for message {message.id} from {message_date}")
        return sent_message