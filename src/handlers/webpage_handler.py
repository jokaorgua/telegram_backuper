import os
from .base_handler import BaseMediaHandler
from telethon.tl.types import MessageMediaWebPage


class WebPageHandler(BaseMediaHandler):
    def supports(self, message_or_group):
        # Поддерживаем только одиночные сообщения, так как веб-страницы не группируются в альбомы
        if isinstance(message_or_group, list):
            return False

        message = message_or_group
        supports_webpage = isinstance(message.media, MessageMediaWebPage) and hasattr(message.media,
                                                                                      'webpage') and message.media.webpage
        self.logger.info(f"WebPageHandler supports check for message {message.id}: result={supports_webpage}")
        return supports_webpage

    async def handle(self, message_or_group, target_reply_to_msg_id):
        # Обрабатываем только одиночное сообщение
        message = message_or_group
        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')

        # Извлекаем текст сообщения (может включать ссылку и дополнительный текст)
        part_text = message.message or ''
        if not part_text:
            self.logger.warning(f"Message {message.id} from {message_date} has no text, skipping")
            return None

        self.logger.info(f"Sending webpage message {message.id} from {message_date} with text: '{part_text}'")
        sent_message = await self.client.bot.send_message(
            self.target_chat_id,
            message=part_text,
            reply_to=target_reply_to_msg_id if target_reply_to_msg_id != 0 else None,
            link_preview=True,  # Включаем превью ссылки
            formatting_entities=message.entities if message.entities else None
        )

        self.logger.info(f"Processed webpage message {message.id} from {message_date} to {sent_message.id}")
        return sent_message