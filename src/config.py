# src/config.py
import yaml
import logging
import colorlog
from typing import List
from dataclasses import dataclass

@dataclass
class Pair:
    name: str
    source_chat_id: int
    target_chat_id: int

@dataclass
class Config:
    api_id: int
    api_hash: str
    phone: str
    bot_token: str
    pairs: List[Pair]
    log_level: str
    log_file: str
    temp_dir: str
    caption_limit: int  # Новый параметр

    @classmethod
    def load(cls, path: str) -> 'Config':
        logger = logging.getLogger(__name__)
        logger.info(f"Loading configuration from {path}")
        try:
            with open(path, 'r') as f:
                data = yaml.safe_load(f)
                pairs = [Pair(name=p['name'], source_chat_id=p['source_chat_id'], target_chat_id=p['target_chat_id'])
                         for p in data['pairs']]
                return cls(
                    api_id=data['client']['api_id'],
                    api_hash=data['client']['api_hash'],
                    phone=data['client']['phone'],
                    bot_token=data['bot']['token'],
                    pairs=pairs,
                    log_level=data['logging']['level'],
                    log_file=data['logging']['file'],
                    temp_dir=data['temp_dir'],
                    caption_limit=data.get('caption_limit', 1000)  # Значение по умолчанию 1000
                )
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise

    def setup_logging(self):
        level = getattr(logging, self.log_level.upper(), logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            }
        )
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setFormatter(file_formatter)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        logging.getLogger().setLevel(level)
        logging.getLogger().handlers = []
        logging.getLogger().addHandler(file_handler)
        logging.getLogger().addHandler(console_handler)