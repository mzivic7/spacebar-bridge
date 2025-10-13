import logging
import threading
import time

import psycopg

logger = logging.getLogger(__name__)
DISCORD_EPOCH = 1420070400000


def snowflake_to_timestamp(snowflake):
    """Convert discord snowflake to unix time in ms"""
    return (int(snowflake) >> 22) + DISCORD_EPOCH


class PairStore:
    """Discord-Spacebar message snowflake pair database"""

    def __init__(self, host, user, password, dbname, cleanup_days=3, pair_lifetime_days=30, name="Unknown"):
        self.cleanup_days = cleanup_days
        self.pair_lifetime_days = pair_lifetime_days
        self.name = name

        # ensure database exists
        with psycopg.connect(host=host, user=user, password=password, dbname="postgres", autocommit=True) as admin_conn:
            with admin_conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
                if cur.fetchone() is None:
                    cur.execute(f"CREATE DATABASE {dbname}")
                    logger.info(f"{self.name} Created database: {dbname}")

        # connect to database
        self.conn = psycopg.connect(host=host, user=user, password=password, dbname=dbname, autocommit=True)
        self.cleanup_conn = psycopg.connect(host=host, user=user, password=password, dbname=dbname, autocommit=True)
        self.init_db()

        self.run  = True
        if pair_lifetime_days and cleanup_days:
            self.cleanup_thread = threading.Thread(target=self.cleanup_loop, daemon=True)
            self.cleanup_thread.start()


    def init_db(self):
        """Initialize database"""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    name TEXT PRIMARY KEY
                )
            """)
        logger.info(f"{self.name} Database initializes successfully")


    def create_table(self, channel_pair):
        """Create table for channel if it doesnt exist"""
        with self.conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {channel_pair} (
                    source TEXT PRIMARY KEY,
                    target TEXT NOT NULL
                )
            """)
            cur.execute("INSERT INTO channels (name) VALUES (%s) ON CONFLICT DO NOTHING", (channel_pair,))
        return channel_pair


    def add_pair(self, channel_pair, source, target):
        """Add a pair of source and target message snowflakes"""
        with self.conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {channel_pair} (source, target)
                VALUES (%s, %s)
                ON CONFLICT (source) DO UPDATE SET target = EXCLUDED.target
            """, (source, target))


    def get_target(self, channel_pair, source):
        """Get target id from source in a pair, if not found return none"""
        with self.conn.cursor() as cur:
            row = cur.execute(f"SELECT target FROM {channel_pair} WHERE source = %s LIMIT 1", (source,)).fetchone()
        if row:
            return row[0]
        return None


    def get_source(self, channel_pair, target):
        """Get source id from target in a pair, if not found return none"""
        with self.conn.cursor() as cur:
            row = cur.execute(f"SELECT source FROM {channel_pair} WHERE target = %s LIMIT 1", (target,)).fetchone()
        if row:
            return row[0]
        return None


    def delete_pair(self, channel_pair, source):
        """Delete a pair by source"""
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {channel_pair} WHERE source = %s", (source,))


    def cleanup_old_pairs(self):
        """Delete pairs older than the configured lifetime"""
        cutoff = (time.time() - self.pair_lifetime_days * 86400) * 1000
        old_pairs = []
        deleted = 0

        with self.cleanup_conn.cursor() as cur:
            cur.execute("SELECT name FROM channels")
            for (channel_pair,) in cur.fetchall():
                old_pairs = []
                cur_2 = self.cleanup_conn.cursor()
                cur_2.execute(f"SELECT source FROM {channel_pair}")
                for (id_1,) in cur_2.fetchall():
                    if snowflake_to_timestamp(id_1) < cutoff:
                        old_pairs.append((id_1,))
                cur_2.close()
            if old_pairs:
                with self.cleanup_conn.cursor() as cur_2:
                    cur_2.executemany(f"DELETE FROM {channel_pair} WHERE source = %s", old_pairs)
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
