import sqlite3
import logging

class Repository:
    def __init__(self, db_path):
        self.logger = logging.getLogger(__name__)
        self.db_path = db_path

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def get_topic(self, source_topic_id, source_chat_id, target_chat_id):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT source_topic_id, target_topic_id, title, synced FROM topics WHERE source_topic_id = ? AND source_chat_id = ? AND target_chat_id = ?",
                          (source_topic_id, source_chat_id, target_chat_id))
            return cursor.fetchone()

    def add_topic(self, source_topic_id, source_chat_id, target_chat_id, title):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT OR IGNORE INTO topics (source_topic_id, source_chat_id, target_chat_id, title) VALUES (?, ?, ?, ?)",
                          (source_topic_id, source_chat_id, target_chat_id, title))
            conn.commit()
            self.logger.debug(f"Added topic {source_topic_id} for source {source_chat_id} to target {target_chat_id} with title '{title}'")

    def update_topic(self, source_topic_id, source_chat_id, target_chat_id, target_topic_id, synced=1):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE topics SET target_topic_id = ?, synced = ? WHERE source_topic_id = ? AND source_chat_id = ? AND target_chat_id = ?",
                          (target_topic_id, synced, source_topic_id, source_chat_id, target_chat_id))
            conn.commit()
            self.logger.debug(f"Updated topic {source_topic_id} for source {source_chat_id} to target {target_chat_id} with target ID {target_topic_id}")

    def delete_topic(self, source_topic_id, source_chat_id, target_chat_id):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM topics WHERE source_topic_id = ? AND source_chat_id = ? AND target_chat_id = ?",
                          (source_topic_id, source_chat_id, target_chat_id))
            conn.commit()
            self.logger.debug(f"Deleted topic {source_topic_id} for source {source_chat_id} to target {target_chat_id}")

    def get_all_topics(self, source_chat_id, target_chat_id):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT source_topic_id, target_topic_id, title, synced FROM topics WHERE source_chat_id = ? AND target_chat_id = ?",
                          (source_chat_id, target_chat_id))
            return cursor.fetchall()

    def get_message(self, source_msg_id, source_chat_id, target_chat_id):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT source_msg_id, target_msg_id, topic_id, synced FROM messages WHERE source_msg_id = ? AND source_chat_id = ? AND target_chat_id = ?",
                          (source_msg_id, source_chat_id, target_chat_id))
            return cursor.fetchone()

    def add_message(self, source_msg_id, source_chat_id, target_chat_id, topic_id):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT OR IGNORE INTO messages (source_msg_id, source_chat_id, target_chat_id, topic_id) VALUES (?, ?, ?, ?)",
                          (source_msg_id, source_chat_id, target_chat_id, topic_id))
            conn.commit()
            self.logger.debug(f"Added message {source_msg_id} for source {source_chat_id} to target {target_chat_id} in topic {topic_id}")

    def update_message(self, source_msg_id, source_chat_id, target_chat_id, target_msg_id, synced=1):
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE messages SET target_msg_id = ?, synced = ? WHERE source_msg_id = ? AND source_chat_id = ? AND target_chat_id = ?",
                          (target_msg_id, synced, source_msg_id, source_chat_id, target_chat_id))
            conn.commit()
            self.logger.debug(f"Updated message {source_msg_id} for source {source_chat_id} to target {target_chat_id} with target ID {target_msg_id}")