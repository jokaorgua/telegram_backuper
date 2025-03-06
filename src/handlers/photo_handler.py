import os

from .base_handler import BaseMediaHandler


class PhotoHandler(BaseMediaHandler):
    def supports(self, message_or_group):
        if isinstance(message_or_group, list):
            # Проверяем, что все сообщения содержат только фото
            messages = message_or_group
        else:
            messages = [message_or_group]

        return all(
            hasattr(msg.media, 'photo') or
            (hasattr(msg.media, 'document') and hasattr(msg.media.document, 'mime_type') and
             msg.media.document.mime_type.startswith('image'))
            for msg in messages
        )

    async def handle(self, message_or_group, target_topic_id):
        # Если передан список сообщений (группа), обрабатываем как группу
        if isinstance(message_or_group, list):
            messages = message_or_group
            lead_message = messages[0]
            message_date = lead_message.date.strftime('%Y-%m-%d %H:%M:%S')
            file_paths = []
            text_parts = []

            # Собираем файлы и тексты
            for msg in messages:
                file_path = os.path.join(self.processor.temp_dir, f"media_{msg.id}_{self.processor.source_chat_id}.jpg")
                downloaded_path = await self.media_manager.download_media(msg, file_path)
                file_paths.append(downloaded_path)
                self.logger.info(f"Downloaded photo {msg.id} from {message_date} to {downloaded_path}")
                if msg.message and msg.message.strip():
                    text_parts.append(msg.message.strip())
                    self.logger.info(f"Found text in message {msg.id}: '{msg.message.strip()}'")

            # Склеиваем все непустые тексты с разделителем
            part_text = '\n'.join(text_parts) if text_parts else ''

            self.logger.info(
                f"Sending group of {len(file_paths)} photos for message {lead_message.id} from {message_date} with message: '{part_text}'")
            sent_message = await self.client.bot.send_message(
                self.target_chat_id,
                message=part_text,  # Используем message для текста
                file=file_paths,  # Передаем список файлов
                force_document=False,
                reply_to=target_topic_id if target_topic_id != 0 else None,
                formatting_entities=lead_message.entities if lead_message.entities else None
            )

            for file_path in file_paths:
                os.remove(file_path)
                self.logger.info(
                    f"Removed temporary file {file_path} for group message {lead_message.id} from {message_date}")
            return sent_message

        # Одиночное фото
        message = message_or_group
        file_path = os.path.join(self.processor.temp_dir, f"media_{message.id}_{self.processor.source_chat_id}.jpg")
        downloaded_path = await self.media_manager.download_media(message, file_path)

        part_text = message.message or ''
        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(
            f"Sending photo for message {message.id} from {message_date} from {downloaded_path} with message: '{part_text}'")
        sent_message = await self.client.bot.send_message(
            self.target_chat_id,
            message=part_text,  # Используем message для текста
            file=downloaded_path,
            force_document=False,
            reply_to=target_topic_id if target_topic_id != 0 else None,
            formatting_entities=message.entities if message.entities else None
        )
        os.remove(downloaded_path)
        self.logger.info(f"Removed temporary file {downloaded_path} for message {message.id} from {message_date}")
        return sent_message
