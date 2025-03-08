# src/handlers/base_handler.py
import logging
import asyncio

class BaseMediaHandler:
    def __init__(self, processor):
        self.processor = processor
        self.logger = logging.getLogger(__name__)
        self.client = processor.client
        self.target_chat_id = processor.target_chat_id
        self.media_manager = processor.media_manager
        self.caption_limit = processor.caption_limit

    def supports(self, message):
        raise NotImplementedError("Handler must implement supports method")

    async def handle(self, message, target_topic_id):
        raise NotImplementedError("Handler must implement handle method")

    def _adjust_entities(self, original_text, truncated_text, entities):
        """Корректирует entities, чтобы они соответствовали обрезанному тексту."""
        if not entities or len(original_text) <= len(truncated_text):
            return entities

        adjusted_entities = []
        new_length = len(truncated_text)

        for entity in entities:
            offset = entity.offset
            length = entity.length
            end = offset + length

            # Пропускаем сущности, которые полностью за пределами обрезанного текста
            if offset >= new_length:
                continue

            # Корректируем сущности, которые частично обрезаны
            if end > new_length:
                entity.length = new_length - offset
                if entity.length <= 0:
                    continue  # Пропускаем сущности с нулевой длиной

            adjusted_entities.append(entity)

        return adjusted_entities if adjusted_entities else None