import os
from .base_handler import BaseMediaHandler

class PhotoHandler(BaseMediaHandler):
    def supports(self, message):
        supports_photo = hasattr(message.media, 'photo')
        supports_photo = supports_photo or (hasattr(message.document,
                                                    'mime_type') and message.document.mime_type.startswith('image'))
        self.logger.info(f"PhotoHandler supports check: result={supports_photo}")
        return supports_photo

    async def handle(self, message, target_topic_id):
        file_path = os.path.join(self.processor.temp_dir, f"media_{message.id}_{self.processor.source_chat_id}.jpg")
        downloaded_path = await self.media_manager.download_media(message, file_path)

        part_text = message.message or ''
        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(f"Sending photo for message {message.id} from {message_date} from {downloaded_path}")
        sent_message = await self.client.bot.send_message(
            self.target_chat_id,
            message=part_text,
            file=downloaded_path,
            force_document=False,
            reply_to=target_topic_id if target_topic_id != 0 else None,
            formatting_entities=message.entities if message.entities else None
        )
        os.remove(downloaded_path)
        self.logger.info(f"Removed temporary file {downloaded_path} for message {message.id} from {message_date}")
        return sent_message