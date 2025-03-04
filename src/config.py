import yaml
from dataclasses import dataclass
import logging
import colorlog


@dataclass
class Config:
    api_id: int
    api_hash: str
    phone: str
    bot_token: str
    source_chat_id: int
    target_chat_id: int
    log_level: str
    log_file: str
    temp_dir: str

    @classmethod
    def load(cls, path: str) -> 'Config':
        logger = logging.getLogger(__name__)
        logger.info(f"Loading configuration from {path}")
        try:
            with open(path, 'r') as f:
                data = yaml.safe_load(f)
                return cls(
                    api_id=data['client']['api_id'],
                    api_hash=data['client']['api_hash'],
                    phone=data['client']['phone'],
                    bot_token=data['bot']['token'],
                    source_chat_id=data['source']['chat_id'],
                    target_chat_id=data['target']['chat_id'],
                    log_level=data['logging']['level'],
                    log_file=data['logging']['file'],
                    temp_dir=data['temp_dir']
                )
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            raise

    def setup_logging(self):
        level = getattr(logging, self.log_level.upper(), logging.INFO)
        # Формат для файла (без цвета)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # Формат для консоли (с цветом)
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

        # Обработчик для файла
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setFormatter(file_formatter)

        # Обработчик для консоли
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)

        # Настройка корневого логгера
        logging.getLogger().setLevel(level)
        logging.getLogger().handlers = []  # Очищаем существующие обработчики
        logging.getLogger().addHandler(file_handler)
        logging.getLogger().addHandler(console_handler)