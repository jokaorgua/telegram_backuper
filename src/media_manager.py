import os
import logging
import asyncio
import ffmpeg
import math
from tqdm import tqdm
from telethon import utils

class MediaManager:
    def __init__(self, client, temp_dir):
        self.logger = logging.getLogger(__name__)
        self.client = client
        self.temp_dir = temp_dir
        self.PART_SIZE = 512 * 1024
        self.MAX_PARTS = 4000
        self.MAX_FILE_SIZE = self.PART_SIZE * self.MAX_PARTS
        self.TARGET_PART_SIZE = 1.9 * 1024 * 1024 * 1024

    async def download_media(self, message, file_path):
        """Скачивает медиа с поддержкой докачки и прогресс-бара в указанный путь."""
        file_size = message.media.document.size if hasattr(message.media, 'document') else getattr(message.media, 'size', None)
        self.logger.info(f"Starting download of media {message.id} to {file_path}, size: {file_size or 'unknown'} bytes")

        current_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        if file_size and current_size >= file_size:
            self.logger.info(f"Media {message.id} already fully downloaded at {file_path}")
            return file_path

        input_file = message.media.document if hasattr(message.media, 'document') else message.media
        with tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Downloading media {message.id}", initial=current_size) as pbar:
            with open(file_path, 'ab' if current_size > 0 else 'wb') as fd:
                if current_size > 0:
                    fd.seek(current_size)
                    self.logger.info(f"Resuming download from offset {current_size}")
                async for chunk in self.client.client.iter_download(
                        input_file,
                        offset=current_size,
                        chunk_size=1024 * 1024
                ):
                    try:
                        fd.write(chunk)
                        pbar.update(len(chunk))
                    except Exception as e:
                        self.logger.error(f"Download interrupted for {message.id} at offset {current_size + pbar.n}: {str(e)}")
                        raise

        downloaded_size = os.path.getsize(file_path)
        if file_size and downloaded_size != file_size:
            self.logger.error(f"Download incomplete for {message.id}: {downloaded_size}/{file_size} bytes")
            raise ValueError("File size mismatch after download")

        self.logger.info(f"Media downloaded to {file_path}")
        return file_path

    async def split_video(self, input_path, message_id):
        """Разрезает видео на части меньше 2 ГБ с помощью ffmpeg."""
        file_size = os.path.getsize(input_path)
        if file_size <= self.MAX_FILE_SIZE:
            return [input_path]

        probe = ffmpeg.probe(input_path)
        duration = float(probe['format']['duration'])
        part_size_bytes = self.TARGET_PART_SIZE
        num_parts = math.ceil(file_size / part_size_bytes)
        part_duration = duration / num_parts

        output_files = []
        for i in range(num_parts):
            output_file = os.path.join(self.temp_dir, f"media_{message_id}_part{i + 1}.mp4")
            try:
                self.logger.info(f"Cutting part {i + 1} of {num_parts} for message {message_id}")
                stream = ffmpeg.input(input_path, ss=i * part_duration, t=part_duration)
                stream = ffmpeg.output(stream, output_file, c='copy', f='mp4', map_metadata='-1', reset_timestamps=1,
                                       loglevel='quiet')
                ffmpeg.run(stream)
                output_files.append(output_file)
            except ffmpeg.Error as e:
                self.logger.error(f"Failed to cut part {i + 1} for message {message_id}: {str(e)}")
                raise

        return output_files