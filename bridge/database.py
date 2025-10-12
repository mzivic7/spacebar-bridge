import logging
import threading
import time

import apsw

logger = logging.getLogger(__name__)
DISCORD_EPOCH = 1420070400000


def snowflake_to_timestamp(snowflake):
    """Convert discord snowflake to unix time in ms"""
    return (int(snowflake) >> 22) + DISCORD_EPOCH


class PairStore:
    """Discord-Spacebar message snowflake pair database"""

    def __init__(self, db_path="pairs.db", cleanup_days=3, pair_lifetime_days=30, name="Unknown"):
        self.db_path = db_path
        self.cleanup_days = cleanup_days
        self.pair_lifetime_days = pair_lifetime_days
        self.name = name

        self.conn = apsw.Connection(self.db_path)
        self.cleanup_conn = apsw.Connection(self.db_path)
        self.init_db()

        self.run  = True
        if pair_lifetime_days and cleanup_days:
            self.cleanup_thread = threading.Thread(target=self.cleanup_loop, daemon=True)
            self.cleanup_thread.start()


    def init_db(self):
        """Initialize database"""
        cur = self.conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        # table to track which channel tables exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                name TEXT PRIMARY KEY
            )
        """)
        logger.info(f"{self.name} Database initializes successfully")


    def create_table(self, channel_pair: str):
        """Create table for channel if it doesnt exist"""
        with self.conn:
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {channel_pair} (
                    id1 TEXT PRIMARY KEY,
                    id2 TEXT NOT NULL
                )
            """)
            self.conn.execute("INSERT OR IGNORE INTO channels (name) VALUES (?)", (channel_pair,))
        return channel_pair



    def add_pair(self, channel_pair, msg_id_1, msg_id_2):
        """Add a pair of snowflakes"""
        print(channel_pair, msg_id_1, msg_id_2)
        with self.conn:
            self.conn.execute(f"INSERT OR REPLACE INTO {channel_pair} (id1, id2) VALUES (?, ?)", (msg_id_1, msg_id_2))


    def get_pair(self, channel_pair, msg_id):
        """Get pair if msg_id from first id in a pair, else none"""
        row = self.conn.execute(f"SELECT id1, id2 FROM {channel_pair} WHERE id1 = ? LIMIT 1", (msg_id,)).fetchone()
        if row:
            return row
        return None


    def delete_pair(self, channel_pair, msg_id):
        """Delete a pair by any id"""
        with self.conn:
            self.conn.execute(f"DELETE FROM {channel_pair} WHERE id1 = ?", (msg_id,))


    def cleanup_old_pairs(self):
        """Delete pairs older than the configured lifetime"""
        cutoff = (time.time() - self.pair_lifetime_days * 86400) * 1000
        old_pairs = []
        deleted = 0

        for (channel_pair,) in self.cleanup_conn.execute("SELECT name FROM channels"):
            old_pairs = []
            for (id1,) in self.cleanup_conn.execute(f"SELECT id1 FROM {channel_pair}"):
                if snowflake_to_timestamp(id1) < cutoff:
                    old_pairs.append((id1,))
            if old_pairs:
                with self.cleanup_conn:
                    self.cleanup_conn.executemany(f"DELETE FROM {channel_pair} WHERE id1=?", old_pairs)
                logger.debug(f"({self.name}) Cleanup: {channel_pair}: removed {len(old_pairs)} old pairs")
                deleted += len(old_pairs)
        if not deleted:
            logger.debug(f"({self.name}) Cleanup: No old pairs found")


    def cleanup_loop(self):
        """Loop that runs cleanup every N configured days"""
        while self.run:
            try:
                self.cleanup_old_pairs()
            except Exception as e:
                logger.debug(f"({self.name}) Cleanup error: {e}")
            time.sleep(self.cleanup_days * 86400)
