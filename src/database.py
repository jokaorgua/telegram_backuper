import sqlite3
import logging

class Database:
    def __init__(self, path="telegram_cloner.db"):
        self.logger = logging.getLogger(__name__)
        self.path = path
        self._init_db()

    def _init_db(self):
        self.logger.info(f"Initializing database at {self.path}")
        with sqlite3.connect(self.path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS topics (
                    source_topic_id INTEGER,
                    source_chat_id INTEGER,
                    target_chat_id INTEGER,
                    target_topic_id INTEGER,
                    title TEXT NOT NULL,
                    synced INTEGER DEFAULT 0,
                    PRIMARY KEY (source_topic_id, source_chat_id, target_chat_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    source_msg_id INTEGER,
                    source_chat_id INTEGER,
                    target_chat_id INTEGER,
                    target_msg_id INTEGER,
                    topic_id INTEGER,
                    synced INTEGER DEFAULT 0,
                    PRIMARY KEY (source_msg_id, source_chat_id, target_chat_id)
                )
            """)
            conn.commit()