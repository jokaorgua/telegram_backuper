import os

from telethon.tl.types import DocumentAttributeFilename
from tqdm import tqdm

from .base_handler import BaseMediaHandler


class FileHandler(BaseMediaHandler):
    def supports(self, message_or_group):
        if isinstance(message_or_group, list):
            messages = message_or_group
        else:
            messages = [message_or_group]
        return all(
            (((hasattr(msg.media, 'voice') and msg.media.voice == False) or (not hasattr(msg.media, 'voice'))) and
             ((hasattr(msg.media, 'round') and msg.media.round == False) or (not hasattr(msg.media, 'round'))) and
             ((hasattr(msg.media, 'video') and msg.media.video == False) or (not hasattr(msg.media, 'video'))) and
             (not hasattr(msg.media, 'photo'))) or
            (hasattr(msg.media, 'document') and hasattr(msg.media.document, 'mime_type') and
             msg.media.document.mime_type.startswith('application/'))
            for msg in messages
        )

    async def _process_single_message(self, message, target_reply_to_msg_id):
        message_date = message.date.strftime('%Y-%m-%d %H:%M:%S')
        text_part = message.message.strip()
        entities = None
        if message.entities:
            entities = next(message.entities)

        document_extension = message.file.ext
        file_path = os.path.join(self.processor.temp_dir,
                                 f"media_{message.id}_{self.processor.source_chat_id}{document_extension}")
        real_file_name = message.document.attributes[0].file_name
        downloaded_path = await self.media_manager.download_media(message, file_path)
        self.logger.info(f"Downloaded file {message.id} from {message_date} to {downloaded_path}")

        total_size = os.path.getsize(downloaded_path)
        self.logger.info(
            f"Sending file for message {message.id} from {message_date} with message: '{text_part}'")
        with tqdm(total=total_size, unit='B', unit_scale=True,
                  desc=f"Uploading files for message {message.id}") as pbar:
            def progress_callback(current, total):
                pbar.update(current - pbar.n)

            attributes = [DocumentAttributeFilename(file_name=real_file_name)]
            sent_message = await self.client.bot.send_file(
                self.target_chat_id,
                attributes=attributes,
                message=text_part,
                file=downloaded_path,
                force_document=True,
                reply_to=target_reply_to_msg_id if target_reply_to_msg_id != 0 else None,
                formatting_entities=entities if entities else None,
                progress_callback=progress_callback  # Включили обратно progress_callback
            )

        os.remove(file_path)
        self.logger.info(f"Removed temporary file {file_path} for group message {message.id}")

        return sent_message if sent_message else None

    async def handle(self, message_or_group, target_reply_to_msg_id):
        if isinstance(message_or_group, list):
            lead_message = message_or_group[0]
            message_date = lead_message.date.strftime('%Y-%m-%d %H:%M:%S')
            text_parts = [msg.message.strip() for msg in message_or_group if msg.message and msg.message.strip()]
            part_text = '\n'.join(text_parts) if text_parts else ''
            entities = next((msg.entities for msg in message_or_group if msg.entities), None)

            file_paths = []
            for msg in message_or_group:
                document_extension = msg.file.ext
                file_path = os.path.join(self.processor.temp_dir,
                                         f"media_{msg.id}_{self.processor.source_chat_id}{document_extension}")
                downloaded_path = await self.media_manager.download_media(msg, file_path)
                file_paths.append(downloaded_path)
                self.logger.info(f"Downloaded file {msg.id} from {message_date} to {downloaded_path}")

            sent_messages = []
            total_size = sum(os.path.getsize(f) for f in file_paths)
            self.logger.info(
                f"Sending group of {len(file_paths)} files for message {lead_message.id} from {message_date} with message: '{part_text}'")
            with tqdm(total=total_size, unit='B', unit_scale=True,
                      desc=f"Uploading files for message {lead_message.id}") as pbar:
                def progress_callback(current, total):
                    pbar.update(current - pbar.n)

                sent_message = await self.client.bot.send_file(
                    self.target_chat_id,
                    message=part_text,
                    file=file_paths,
                    force_document=True,
                    reply_to=target_reply_to_msg_id if target_reply_to_msg_id != 0 else None,
                    formatting_entities=entities if entities else None,
                    progress_callback=progress_callback  # Включили обратно progress_callback
                )
                sent_messages.append(sent_message)

            for file_path in file_paths:
                os.remove(file_path)
                self.logger.info(f"Removed temporary file {file_path} for group message {lead_message.id}")

            return sent_messages[0] if sent_messages else None
        else:
            return await self._process_single_message(message_or_group, target_reply_to_msg_id)
