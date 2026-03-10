import sqlite3
import logging

logger = logging.getLogger(__name__)

def init_db(db_path="data/taglishbench.db"):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            entry_id TEXT PRIMARY KEY,
            source TEXT,
            origin TEXT,
            text TEXT,
            thread_id TEXT,
            parent_id TEXT,
            depth INTEGER,
            platform_id TEXT,
            author_hash TEXT,
            timestamp TEXT,
            engagement_score INTEGER,
            engagement_replies INTEGER,
            cmi REAL,
            m_index REAL,
            i_index REAL
        )
        ''')
        
        # Add some indexes for common queries we will run
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_thread_id ON comments(thread_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_source ON comments(source)')
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error initializing database: {e}")

def save_data(data, db_path="data/taglishbench.db"):
    """Upserts a TaglishBench record into the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Flatten the nested schema to tabular format
        entry_id = data.get('entry_id')
        source = data.get('source')
        origin = data.get('origin')
        text = data.get('text')
        
        thread_info = data.get('thread_info', {})
        thread_id = thread_info.get('thread_id')
        parent_id = thread_info.get('parent_id')
        depth = thread_info.get('depth')
        
        metadata = data.get('metadata', {})
        platform_id = metadata.get('platform_id')
        author_hash = metadata.get('author_hash')
        timestamp = metadata.get('timestamp')
        
        engagement = metadata.get('engagement', {})
        score = engagement.get('score', 0)
        replies = engagement.get('replies', 0)
        
        metrics = data.get('complexity_metrics', {})
        cmi = metrics.get('cmi')
        m_index = metrics.get('m_index')
        i_index = metrics.get('i_index')
        
        query = '''
        INSERT INTO comments (
            entry_id, source, origin, text, thread_id, parent_id, depth, 
            platform_id, author_hash, timestamp, engagement_score, engagement_replies, 
            cmi, m_index, i_index
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(entry_id) DO UPDATE SET
            engagement_score = excluded.engagement_score,
            engagement_replies = excluded.engagement_replies
        '''
        
        cursor.execute(query, (
            entry_id, source, origin, text, thread_id, parent_id, depth,
            platform_id, author_hash, timestamp, score, replies,
            cmi, m_index, i_index
        ))
        
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error saving data to db: {e}")
