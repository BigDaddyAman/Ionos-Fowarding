import os
from dotenv import load_dotenv
import psycopg2
import logging

load_dotenv()

def init_db():
    # Database connection parameters
    conn = psycopg2.connect(
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD'),
        host=os.getenv('PGHOST', 'localhost'),
        database=os.getenv('PGDATABASE'),
        port=os.getenv('PGPORT', 5432)
    )
    
    cur = conn.cursor()
    
    try:
        # Create videos table with is_document column
        cur.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            id SERIAL PRIMARY KEY,
            video_name VARCHAR(255) NOT NULL,
            caption TEXT,
            keywords TEXT[],
            file_id VARCHAR(255) NOT NULL,
            access_hash VARCHAR(255) NOT NULL,
            upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_document BOOLEAN DEFAULT FALSE
        )
        """)
        
        # Add is_document column if it doesn't exist (for existing tables)
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'videos' AND column_name = 'is_document'
            ) THEN
                ALTER TABLE videos ADD COLUMN is_document BOOLEAN DEFAULT FALSE;
            END IF;
        END $$;
        """)
        
        # Create video_tokens table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS video_tokens (
            id SERIAL PRIMARY KEY,
            token VARCHAR(255) NOT NULL,
            video_id INTEGER REFERENCES videos(id),
            user_id BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP NOT NULL,
            used BOOLEAN DEFAULT FALSE
        )
        """)
        
        # Create premium_users table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS premium_users (
            id SERIAL PRIMARY KEY,
            user_id BIGINT UNIQUE NOT NULL,
            start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expiry_date TIMESTAMP NOT NULL
        )
        """)
        
        # Create or update indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_video_name ON videos(video_name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_file_id ON videos(file_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_is_document ON videos(is_document)")  # Add index for is_document
        cur.execute("CREATE INDEX IF NOT EXISTS idx_token ON video_tokens(token)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expires ON video_tokens(expires_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_keywords ON videos USING GIN (keywords)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_premium_user_id ON premium_users(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_premium_expiry ON premium_users(expiry_date)")
        
        # Update existing videos to have is_document=false if null
        cur.execute("""
        UPDATE videos 
        SET is_document = FALSE 
        WHERE is_document IS NULL
        """)
        
        conn.commit()
        print("✅ Database tables and indexes created/updated successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error initializing database: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    init_db()
