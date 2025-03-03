import sqlite3
import logging

class Database:
    def __init__(self, path: str = "telegram_cloner.db"):
        self.logger = logging.getLogger(__name__)
        self.path = path
        self._init_db()

    def _init_db(self):
        self.logger.info(f"Initializing database at {self.path}")
        with sqlite3.connect(self.path) as conn:
            cursor = conn.cursor()
            # Таблица для тем
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS topics (
                    source_topic_id INTEGER PRIMARY KEY,
                    target_topic_id INTEGER,
                    title TEXT NOT NULL,
                    synced INTEGER DEFAULT 0  -- 0: not synced, 1: synced
                )
            """)
            # Таблица для сообщений
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    source_msg_id INTEGER PRIMARY KEY,
                    target_msg_id INTEGER,
                    topic_id INTEGER,
                    synced INTEGER DEFAULT 0
                )
            """)
            conn.commit()