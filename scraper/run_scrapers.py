import psycopg2
import logging
import asyncio
import os
from scrape_jobsearchmalawi import scrape_jobsearchmalawi
from scrape_ntchito import scrape_ntchito
from scrape_careers import scrape_careersmw

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Database connection from environment
DB_URL = os.getenv("DB_URL")

def init_db():
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                company VARCHAR(255),
                location VARCHAR(255),
                job_type VARCHAR(100),
                date_posted DATE,
                url TEXT,
                source VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            SELECT constraint_name 
            FROM information_schema.table_constraints 
            WHERE table_name = 'jobs' AND constraint_type = 'UNIQUE' AND constraint_name = 'unique_url'
        """)
        constraint_exists = cursor.fetchone()
        
        if not constraint_exists:
            cursor.execute("""
                SELECT url, COUNT(*) 
                FROM jobs 
                GROUP BY url 
                HAVING COUNT(*) > 1
            """)
            duplicates = cursor.fetchall()
            if duplicates:
                logger.warning(f"Cannot add unique_url constraint due to {len(duplicates)} duplicate URLs.")
            else:
                cursor.execute("ALTER TABLE jobs ADD CONSTRAINT unique_url UNIQUE (url)")
                logger.info("Added unique_url constraint to jobs table")
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

async def run_all_scrapers():
    logger.info("Starting all scrapers")
    jobs = []

    try:
        jobs.extend(await scrape_jobsearchmalawi())
        logger.info("Completed jobsearchmalawi.com scraper")
    except Exception as e:
        logger.error(f"jobsearchmalawi.com scraper failed: {e}")
    
    try:
        jobs.extend(await scrape_ntchito())
        logger.info("Completed ntchito.com scraper")
    except Exception as e:
        logger.error(f"ntchito.com scraper failed: {e}")
    
    try:
        jobs.extend(await scrape_careersmw())
        logger.info("Completed careersmw.com scraper")
    except Exception as e:
        logger.error(f"careersmw.com scraper failed: {e}")
    
    logger.info(f"Total jobs scraped: {len(jobs)}")
    return jobs

async def main():
    init_db()
    await run_all_scrapers()

if __name__ == "__main__":
    asyncio.run(main())
