import os
import logging
import asyncio  # Add this import
import psycopg2
import re
from psycopg2 import sql
from typing import List, Tuple
from dotenv import load_dotenv
from datetime import datetime, timedelta  # Add this import

load_dotenv()

# Get database credentials from environment variables
PGUSER = os.getenv('PGUSER')
PGPASSWORD = os.getenv('PGPASSWORD')
PGHOST = os.getenv('PGHOST', 'localhost')  # Default to localhost if not set
PGDATABASE = os.getenv('PGDATABASE')
PGPORT = os.getenv('PGPORT', 5432)  # Default to 5432 if not set

# Function to get a database connection
def get_db_connection():
    return psycopg2.connect(
        user=PGUSER,
        password=PGPASSWORD,
        host=PGHOST,
        database=PGDATABASE,
        port=PGPORT
    )

def init_db():
    """Initialize all database tables without dropping existing data"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Create videos table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            id SERIAL PRIMARY KEY,
            video_name VARCHAR(255) NOT NULL,
            caption TEXT,
            keywords TEXT[],
            file_id VARCHAR(255) NOT NULL,
            access_hash VARCHAR(255) NOT NULL,
            upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Create video tokens table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS video_tokens (
            id SERIAL PRIMARY KEY,
            token VARCHAR(255) NOT NULL,
            video_id INTEGER REFERENCES videos(id),
            user_id BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP NOT NULL,
            used BOOLEAN DEFAULT FALSE
        );
        """)
        
        # Create premium users table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS premium_users (
            id SERIAL PRIMARY KEY,
            user_id BIGINT UNIQUE NOT NULL,
            start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expiry_date TIMESTAMP NOT NULL
        );
        """)
        
        # Create users table for tracking
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            user_id BIGINT UNIQUE NOT NULL,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Create searches table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS searches (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            search_term TEXT,
            search_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Create all necessary indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_video_name ON videos(video_name);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_token ON video_tokens(token);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expires ON video_tokens(expires_at);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_keywords ON videos USING GIN (keywords);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_premium_user_id ON premium_users(user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_premium_expiry ON premium_users(expiry_date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_search_time ON searches(search_time);")
        
        conn.commit()
        logging.info("All database tables verified successfully!")
    except Exception as e:
        logging.error(f"Error checking database tables: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# Function to save a video to the database
def save_video_to_db(video_name: str, caption: str, file_id: str, access_hash: str, upload_date: str):
    """Save video with improved keyword extraction"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Extract keywords from both video name and caption
        keywords = []
        if caption:
            keywords.extend(extract_keywords(caption))
        keywords.extend(extract_keywords(video_name))
        
        # Remove duplicates while preserving order
        keywords = list(dict.fromkeys(keywords))
        
        logging.info(f"Attempting to save video:")
        logging.info(f"Name: {video_name}")
        logging.info(f"File ID: {file_id}")
        logging.info(f"Keywords: {keywords}")
        
        # First check if file_id exists
        cur.execute("SELECT id FROM videos WHERE file_id = %s", (file_id,))
        existing = cur.fetchone()
        
        if existing:
            # Update existing record
            cur.execute("""
                UPDATE videos 
                SET 
                    video_name = %s,
                    caption = %s,
                    keywords = %s,
                    access_hash = %s,
                    upload_date = %s
                WHERE file_id = %s
                RETURNING id
            """, (video_name, caption, keywords, access_hash, upload_date, file_id))
        else:
            # Insert new record
            cur.execute("""
                INSERT INTO videos (video_name, caption, keywords, file_id, access_hash, upload_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (video_name, caption, keywords, file_id, access_hash, upload_date))
        
        video_id = cur.fetchone()[0]
        conn.commit()
        logging.info(f"Successfully saved video with ID: {video_id}")
        return True
        
    except Exception as e:
        logging.error(f"Error saving video to database: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

# Function to get videos by name from the database (with pagination and better search)
def get_videos_by_name(search_term: str, page: int = 1, limit: int = 10) -> Tuple[int, List[Tuple]]:
    """Get videos with Redis cache integration"""
    # Try to get from cache first
    from main import redis_cache
    cached_results = redis_cache.get_cached_search(search_term)
    if cached_results:
        total_count, videos = cached_results
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        return total_count, videos[start_idx:end_idx]

    conn = get_db_connection()
    cur = conn.cursor()
    offset = (page - 1) * limit
    
    try:
        search_term = search_term.strip()
        normalized_term = search_term.lower()
        search_words = normalized_term.split()
        
        # Build dynamic query with improved ranking
        query = """
            WITH RankedVideos AS (
                SELECT 
                    id,
                    video_name,
                    caption,
                    file_id,
                    upload_date,
                    CASE
                        WHEN LOWER(video_name) = %s THEN 100  -- Exact match
                        WHEN LOWER(video_name) LIKE %s THEN 90  -- Starts with
                        WHEN ( -- All words match (in any order)
                            SELECT bool_and(LOWER(video_name) LIKE concat('%%', word, '%%'))
                            FROM unnest(%s::text[]) AS word
                        ) THEN 85
                        WHEN LOWER(video_name) LIKE %s THEN 80  -- Contains
                        WHEN %s = ANY(keywords) THEN 70  -- Keyword exact match
                        WHEN EXISTS (
                            SELECT 1 FROM unnest(keywords) k
                            WHERE k LIKE %s
                        ) THEN 60  -- Keyword partial match
                        ELSE 0
                    END as rank
                FROM videos
                WHERE 
                    LOWER(video_name) = %s
                    OR LOWER(video_name) LIKE %s
                    OR LOWER(video_name) LIKE %s
                    OR %s = ANY(keywords)
                    OR EXISTS (
                        SELECT 1 FROM unnest(keywords) k
                        WHERE k LIKE %s
                    )
                    OR ( -- Match all words condition
                        SELECT bool_and(LOWER(video_name) LIKE concat('%%', word, '%%'))
                        FROM unnest(%s::text[]) AS word
                    )
            )
            SELECT COUNT(*) OVER() as total_count, 
                   id, video_name, caption, file_id, upload_date
            FROM RankedVideos
            WHERE rank > 0
            ORDER BY rank DESC, upload_date DESC
            LIMIT %s OFFSET %s
        """
        
        # Split search term into words for multi-word matching
        search_words = [w.strip() for w in normalized_term.split() if w.strip()]
        
        # Parameters for the query
        params = [
            normalized_term,  # For exact match
            f"{normalized_term}%",  # For starts with
            search_words,  # For all words match array
            f"%{normalized_term}%",  # For contains
            normalized_term,  # For keyword exact match
            f"%{normalized_term}%",  # For keyword partial match
            normalized_term,  # For WHERE exact
            f"{normalized_term}%",  # For WHERE starts with
            f"%{normalized_term}%",  # For WHERE contains
            normalized_term,  # For WHERE keyword exact
            f"%{normalized_term}%",  # For WHERE keyword partial
            search_words,  # For WHERE all words match array
            limit,
            offset
        ]
        
        cur.execute(query, params)
        results = cur.fetchall()
        
        if not results:
            return 0, []
        
        total_count = results[0][0]
        videos = [(r[1], r[2], r[3], r[4], r[5]) for r in results]
        
        # Cache the results before returning
        if results:
            redis_cache.cache_search_results(search_term, videos, total_count)
        
        return total_count, videos
        
    except Exception as e:
        logging.error(f"Search error: {e}")
        return 0, []
    finally:
        cur.close()
        conn.close()

def extract_keywords(text: str) -> list:
    """Extract meaningful keywords from text with better tokenization"""
    # Convert to lowercase and normalize spaces
    text = text.lower()
    
    # Create a list of video attributes to preserve
    video_attributes = [
        '1080p', '720p', '480p', '2160p', 
        'hdrip', 'webrip', 'brrip', 'dvdrip',
        'malaydub', 'malaysub'
    ]
    
    # First extract year if present (4 digits)
    years = re.findall(r'\b(19|20)\d{2}\b', text)
    
    # Split by dots, spaces, and underscores
    words = re.split(r'[.\s_-]+', text)
    
    keywords = []
    
    # Process each word
    i = 0
    while i < len(words):
        word = words[i]
        
        # Add original word
        if word:
            keywords.append(word)
        
        # Try combining with next word for compound terms
        if i < len(words) - 1:
            compound = f"{word}{words[i+1]}"
            combined = f"{word} {words[i+1]}"
            if any(attr.lower() in compound.lower() for attr in video_attributes):
                keywords.append(combined)
                i += 1
        i += 1
    
    # Add years separately to ensure they're searchable
    keywords.extend(years)
    
    # Add the full original name
    keywords.append(text)
    
    # Remove duplicates while preserving order
    seen = set()
    keywords = [x for x in keywords if not (x in seen or seen.add(x))]
    
    return keywords

def get_video_by_token(token: str) -> dict:
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Query the videos directly using token from the URL
        cur.execute("""
            SELECT v.id, v.video_name, v.file_id, v.access_hash 
            FROM videos v 
            WHERE v.id = (
                SELECT video_id FROM video_tokens 
                WHERE token = %s 
                AND expires_at > NOW() 
                AND used = FALSE
            )
        """, (token,))
        
        result = cur.fetchone()
        if result:
            # Mark token as used
            cur.execute("""
                UPDATE video_tokens 
                SET used = TRUE 
                WHERE token = %s
            """, (token,))
            conn.commit()
            
            return {
                'id': result[0],
                'video_name': result[1],
                'file_id': result[2],
                'access_hash': result[3]
            }
        return None
    finally:
        cur.close()
        conn.close()

def save_token(video_id: int, user_id: int, token: str):
    """Save token to database with expiration"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO video_tokens (token, video_id, user_id, expires_at)
            VALUES (%s, %s, %s, NOW() + INTERVAL '1 hour')
        """, (token, video_id, user_id))
        conn.commit()
    finally:
        cur.close()
        conn.close()

def add_or_update_user(user_id: int, username: str = None, first_name: str = None, last_name: str = None):
    """Add new user or update existing user's activity"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO users (user_id, username, first_name, last_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) 
            DO UPDATE SET 
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                last_active = CURRENT_TIMESTAMP
        """, (user_id, username, first_name, last_name))
        conn.commit()
        logging.info(f"User tracked: {user_id} ({username or first_name})")
    except Exception as e:
        logging.error(f"Error tracking user: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

async def get_all_users(exclude_admins=True):
    """Get all user IDs as a list"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        if exclude_admins:
            from constants import AUTHORIZED_USER_IDS
            admin_list = tuple(AUTHORIZED_USER_IDS)
            cur.execute("SELECT user_id FROM users WHERE user_id NOT IN %s", (admin_list,))
        else:
            cur.execute("SELECT user_id FROM users")
        
        return [user[0] for user in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

def get_bot_stats():
    """Get bot statistics"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get total users
        cur.execute("SELECT COUNT(*) FROM users")
        total_users = cur.fetchone()[0]
        
        # Get active users (last 24 hours)
        cur.execute("""
            SELECT COUNT(*) FROM users 
            WHERE last_active >= NOW() - INTERVAL '24 hours'
        """)
        active_users = cur.fetchone()[0]
        
        # Get total videos
        cur.execute("SELECT COUNT(*) FROM videos")
        total_videos = cur.fetchone()[0]
        
        # Get today's searches
        cur.execute("""
            SELECT COUNT(*) FROM searches 
            WHERE search_time >= CURRENT_DATE
        """)
        daily_searches = cur.fetchone()[0] or 0
        
        return {
            'total_users': total_users,
            'active_users': active_users,
            'total_videos': total_videos,
            'daily_searches': daily_searches
        }
    finally:
        cur.close()
        conn.close()

# Add a table for tracking searches
def init_search_tracking():
    """Initialize search tracking table"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS searches (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                search_term TEXT,
                search_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_search_time ON searches(search_time)")
        conn.commit()
    except Exception as e:
        logging.error(f"Error creating search tracking table: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def update_user_activity(user_id: int):
    """Update user's last active timestamp"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            UPDATE users 
            SET last_active = CURRENT_TIMESTAMP 
            WHERE user_id = %s
        """, (user_id,))
        conn.commit()
    except Exception as e:
        logging.error(f"Error updating user activity: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def track_search(user_id: int, search_term: str):
    """Track a search query and update top searches"""
    # Update user's last active timestamp
    update_user_activity(user_id)
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Insert search record
        cur.execute("""
            INSERT INTO searches (user_id, search_term)
            VALUES (%s, %s)
        """, (user_id, search_term))

        # Update or insert into top_searches
        cur.execute("""
            INSERT INTO top_searches (search_date, search_term, search_count)
            VALUES (CURRENT_DATE, %s, 1)
            ON CONFLICT (search_date, search_term)
            DO UPDATE SET search_count = top_searches.search_count + 1;
        """, (search_term,))
        
        conn.commit()
        logging.info(f"Search tracked - User: {user_id}, Term: {search_term}")
    except Exception as e:
        logging.error(f"Error logging search: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def get_detailed_stats():
    """Get detailed bot statistics"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Use a single query for better performance
        cur.execute("""
            WITH user_stats AS (
                SELECT 
                    COUNT(*) as total_users,
                    COUNT(CASE WHEN last_active >= NOW() - INTERVAL '24 hours' THEN 1 END) as active_today,
                    COUNT(CASE WHEN last_active >= NOW() - INTERVAL '30 days' THEN 1 END) as active_month
                FROM users
            ),
            premium_stats AS (
                SELECT COUNT(*) as premium_count
                FROM premium_users
                WHERE expiry_date > NOW()
            ),
            video_stats AS (
                SELECT COUNT(*) as video_count
                FROM videos
            ),
            search_stats AS (
                SELECT COUNT(*) as searches_today
                FROM searches
                WHERE DATE(search_time) = CURRENT_DATE
            )
            SELECT 
                user_stats.total_users,
                user_stats.active_today,
                user_stats.active_month,
                premium_stats.premium_count,
                search_stats.searches_today,
                video_stats.video_count
            FROM user_stats, premium_stats, video_stats, search_stats
        """)
        
        result = cur.fetchone()
        
        # Get top searches in a separate query
        cur.execute("""
            SELECT search_term, COUNT(*) as count
            FROM searches 
            WHERE DATE(search_time) = CURRENT_DATE
            GROUP BY search_term
            ORDER BY count DESC
            LIMIT 5
        """)
        top_searches = cur.fetchall()
        
        return {
            'total_users': result[0] or 0,
            'active_today': result[1] or 0,
            'active_month': result[2] or 0,
            'premium_users': result[3] or 0,
            'searches_today': result[4] or 0,
            'total_videos': result[5] or 0,
            'top_searches': top_searches
        }
        
    except Exception as e:
        logging.error(f"Error getting detailed stats: {e}", exc_info=True)
        return {
            'total_users': 0,
            'active_today': 0,
            'active_month': 0,
            'premium_users': 0,
            'searches_today': 0,
            'total_videos': 0,
            'top_searches': []
        }
    finally:
        cur.close()
        conn.close()

def init_stats_table():
    """Initialize stats tracking table"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Create stats table that stores daily and monthly counters
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_stats (
                id SERIAL PRIMARY KEY,
                stat_date DATE DEFAULT CURRENT_DATE UNIQUE,  -- Add UNIQUE constraint
                total_users INTEGER DEFAULT 0,
                active_users_today INTEGER DEFAULT 0,
                active_users_month INTEGER DEFAULT 0,
                premium_users INTEGER DEFAULT 0,
                searches_today INTEGER DEFAULT 0,
                total_videos INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Create table for top searches
            CREATE TABLE IF NOT EXISTS top_searches (
                id SERIAL PRIMARY KEY,
                search_date DATE DEFAULT CURRENT_DATE,
                search_term TEXT,
                search_count INTEGER DEFAULT 1,
                UNIQUE(search_date, search_term)
            );
        """)
        
        # Create indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stat_date ON bot_stats(stat_date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_search_date ON top_searches(search_date);")
        
        # Initialize today's stats if not exists
        cur.execute("""
            INSERT INTO bot_stats (stat_date)
            SELECT CURRENT_DATE
            WHERE NOT EXISTS (
                SELECT 1 FROM bot_stats WHERE stat_date = CURRENT_DATE
            );
        """)
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error creating stats tables: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def update_stats_counters():
    """Update daily statistics (run this once per day)"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get current counts
        cur.execute("""
            WITH stats AS (
                SELECT 
                    COUNT(*) as total_users,
                    COUNT(CASE WHEN last_active >= NOW() - INTERVAL '24 hours' THEN 1 END) as active_today,
                    COUNT(CASE WHEN last_active >= NOW() - INTERVAL '30 days' THEN 1 END) as active_month
                FROM users
            ),
            premium AS (
                SELECT COUNT(*) as premium_count 
                FROM premium_users 
                WHERE expiry_date > NOW()
            ),
            videos AS (
                SELECT COUNT(*) as video_count 
                FROM videos
            )
            UPDATE bot_stats 
            SET 
                total_users = stats.total_users,
                active_users_today = stats.active_today,
                active_users_month = stats.active_month,
                premium_users = premium.premium_count,
                total_videos = videos.video_count,
                updated_at = NOW()
            FROM stats, premium, videos
            WHERE stat_date = CURRENT_DATE
        """)
        conn.commit()
    except Exception as e:
        logging.error(f"Error updating stats: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def increment_search_count(search_term: str):
    """Increment search counter and update top searches"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Update daily search counter
        cur.execute("""
            UPDATE bot_stats 
            SET searches_today = searches_today + 1
            WHERE stat_date = CURRENT_DATE;
        """)
        
        # Update top searches
        cur.execute("""
            INSERT INTO top_searches (search_date, search_term, search_count)
            VALUES (CURRENT_DATE, %s, 1)
            ON CONFLICT (search_date, search_term)
            DO UPDATE SET search_count = top_searches.search_count + 1;
        """, (search_term,))
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error incrementing search count: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def get_cached_stats():
    """Get cached statistics from bot_stats table"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get fresh counts directly from tables
        cur.execute("""
            WITH counts AS (
                SELECT 
                    (SELECT COUNT(*) FROM users) as total_users,
                    (SELECT COUNT(*) FROM users WHERE last_active >= NOW() - INTERVAL '24 hours') as active_today,
                    (SELECT COUNT(*) FROM users WHERE last_active >= NOW() - INTERVAL '30 days') as active_month,
                    (SELECT COUNT(*) FROM premium_users WHERE expiry_date > NOW()) as premium_users,
                    (SELECT COUNT(*) FROM searches WHERE DATE(search_time) = CURRENT_DATE) as searches_today,
                    (SELECT COUNT(*) FROM videos) as total_videos
            )
            SELECT * FROM counts
        """)
        
        result = cur.fetchone()
        if result:
            stats = {
                'total_users': result[0],
                'active_today': result[1],
                'active_month': result[2],
                'premium_users': result[3],
                'searches_today': result[4],
                'total_videos': result[5]
            }
        else:
            stats = {
                'total_users': 0,
                'active_today': 0,
                'active_month': 0,
                'premium_users': 0,
                'searches_today': 0,
                'total_videos': 0
            }
        
        # Get today's top searches
        cur.execute("""
            SELECT search_term, COUNT(*) as count
            FROM searches
            WHERE DATE(search_time) = CURRENT_DATE
            GROUP BY search_term
            ORDER BY count DESC
            LIMIT 5
        """)
        stats['top_searches'] = cur.fetchall() or []
        
        return stats
        
    except Exception as e:
        logging.error(f"Error fetching stats: {e}")
        return {
            'total_users': 0,
            'active_today': 0,
            'active_month': 0,
            'premium_users': 0,
            'searches_today': 0,
            'total_videos': 0,
            'top_searches': []
        }
    finally:
        cur.close()
        conn.close()

async def update_stats_periodically():
    """Update stats every hour"""
    while True:
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            
            # Update bot_stats table with corrected WITH clause
            cur.execute("""
                WITH stats AS (
                    SELECT 
                        COUNT(*) as total_users,
                        COUNT(CASE WHEN last_active >= NOW() - INTERVAL '24 hours' THEN 1 END) as active_today,
                        COUNT(CASE WHEN last_active >= NOW() - INTERVAL '30 days' THEN 1 END) as active_month
                    FROM users
                ),
                premium AS (
                    SELECT COUNT(*) as premium_count 
                    FROM premium_users 
                    WHERE expiry_date > NOW()
                ),
                videos AS (
                    SELECT COUNT(*) as video_count 
                    FROM videos
                ),
                daily_searches AS (
                    SELECT COALESCE(COUNT(*), 0) as searches_count
                    FROM searches
                    WHERE DATE(search_time) = CURRENT_DATE
                )
                INSERT INTO bot_stats (
                    stat_date,
                    total_users,
                    active_users_today,
                    active_users_month,
                    premium_users,
                    searches_today,
                    total_videos
                )
                SELECT 
                    CURRENT_DATE,
                    stats.total_users,
                    stats.active_today,
                    stats.active_month,
                    premium.premium_count,
                    daily_searches.searches_count,
                    videos.video_count
                FROM stats, premium, videos, daily_searches
                ON CONFLICT (stat_date) 
                DO UPDATE SET
                    total_users = EXCLUDED.total_users,
                    active_users_today = EXCLUDED.active_users_today,
                    active_users_month = EXCLUDED.active_users_month,
                    premium_users = EXCLUDED.premium_users,
                    searches_today = EXCLUDED.searches_today,
                    total_videos = EXCLUDED.total_videos,
                    updated_at = NOW()
            """)
            
            conn.commit()
            
        except Exception as e:
            logging.error(f"Error updating stats: {e}")
        finally:
            cur.close()
            conn.close()
            
        # Wait for an hour before next update
        await asyncio.sleep(3600)

async def reset_daily_stats():
    """Reset daily statistics at midnight"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Archive yesterday's stats before resetting
        cur.execute("""
            INSERT INTO bot_stats (stat_date)
            SELECT CURRENT_DATE
            WHERE NOT EXISTS (
                SELECT 1 FROM bot_stats 
                WHERE stat_date = CURRENT_DATE
            );
        """)
        
        # Reset daily search counts
        cur.execute("""
            UPDATE bot_stats 
            SET searches_today = 0
            WHERE stat_date = CURRENT_DATE;
        """)
        
        # Archive and clear top searches
        cur.execute("""
            DELETE FROM top_searches 
            WHERE search_date < CURRENT_DATE;
        """)
        
        conn.commit()
        logging.info("Daily stats reset successfully")
    except Exception as e:
        logging.error(f"Error resetting daily stats: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def create_search_indexes():
    """Create optimized indexes for search"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Create GiST index for faster LIKE queries
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_video_name_gist ON videos 
            USING gist (video_name gist_trgm_ops);
        """)
        
        # Create index for exact matches
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_video_name_lower ON videos 
            USING btree (LOWER(video_name));
        """)
        
        # Create index for keywords array
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_keywords_gin ON videos 
            USING gin (keywords);
        """)
        
        conn.commit()
        logging.info("Search indexes created successfully")
    except Exception as e:
        logging.error(f"Error creating search indexes: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
