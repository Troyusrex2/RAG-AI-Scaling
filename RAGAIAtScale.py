import logging
import signal
import sys
import aiohttp
import asyncio
import os
import json
from spider import Spider
from bs4 import BeautifulSoup
from bs4.builder import ParserRejectedMarkup
import re
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, DocumentTooLarge
import hashlib
from datetime import datetime
import psutil
import requests

# Configuration parameters
SPIDER_API_KEY = os.environ.get("SPIDER_API_KEY")  # API Key from Spider.cloud
UPDATE_EXISTING = False  # Whether to update documents that already exist in the database
URL_PROCESS_LIMIT = 20  # How many URLs to process at a time
MAX_WORKERS = 1
MONGO_URI = os.environ.get("MONGO_URI")  # API from MongoDB.com
DB_NAME = 'scraped_data'
COLLECTION_NAME = 'documents'
SCHOOL_DATA_COLLECTION = 'school_data'
RETRY_LIMIT = 3  # Number of times to retry processing a URL
MAX_DOC_SIZE = 1024 * 1024  # 1MB in bytes

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
documents_collection = db[COLLECTION_NAME]
school_data_collection = db[SCHOOL_DATA_COLLECTION]

# Initialize the Spider object with the API key
app = Spider(api_key=SPIDER_API_KEY)

# Global flag to indicate if the script was interrupted
interrupted = False

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def signal_handler(sig, frame):
    """Handles interrupt signals (like CTRL+C) to allow graceful shutdown."""
    global interrupted
    interrupted = True
    logger.info("Interrupt received, exiting gracefully...")
    sys.exit(0)

# Register the signal handler for SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, signal_handler)

def hash_content(content):
    """Generate a SHA-256 hash of the given content."""
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

def create_indexes():
    """Create indexes for the MongoDB collection to ensure uniqueness."""
    documents_collection.create_index([('Base_URL', 1), ('content_hash', 1)], unique=True)

def normalize_base_url(url):
    """Normalize the base URL by ensuring it starts with http/https and stripping trailing slashes."""
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    return url.rstrip('/')

def document_exists(base_url, content_hash):
    """Check if a document with the given base URL and content hash already exists in the database."""
    normalized_url = normalize_base_url(base_url)
    return documents_collection.count_documents({'Base_URL': normalized_url, 'content_hash': content_hash}) > 0

def truncate_content(content, max_size):
    """Truncate the content to ensure it does not exceed the maximum size in bytes."""
    content_bytes = content.encode('utf-8')
    if len(content_bytes) > max_size:
        truncated_bytes = content_bytes[:max_size]
        return truncated_bytes.decode('utf-8', errors='ignore')
    return content

def insert_document(doc_id, base_url, unitid, website, url, content):
    """Insert a document into the MongoDB collection, ensuring it does not already exist."""
    content_hash = hash_content(content)
    truncated_content = truncate_content(content, MAX_DOC_SIZE)
    if not document_exists(base_url, content_hash) or UPDATE_EXISTING:
        try:
            documents_collection.insert_one({
                'doc_id': doc_id,
                'Base_URL': base_url,
                'UNITID': unitid,
                'website': website,
                'url': url,
                'date_scraped': datetime.now().isoformat(),
                'content': truncated_content,
                'content_hash': content_hash
            })
        except DuplicateKeyError:
            logger.warning(f"Document with doc_id {doc_id} already exists. Skipping insertion.")
        except DocumentTooLarge:
            logger.error(f"Document with doc_id {doc_id} is too large even after truncation. Skipping insertion.")
    else:
        logger.info(f"Skipping document insertion for URL {base_url} as it already exists with the same content.")

def baseurl_already_processed(base_url):
    """Check if the base URL has already been processed."""
    normalized_url = normalize_base_url(base_url)
    document = documents_collection.find_one({'Base_URL': normalized_url})
    if document:
        # Mark the document as processed in the school_data collection
        school_data_collection.update_one({'WEBADDR': base_url}, {'$set': {'processed': True, 'processing': False}})
        return True
    return False

def mark_as_error(base_url):
    """Mark the given base URL as an error if processing fails repeatedly."""
    school_data_collection.update_one({'WEBADDR': base_url}, {'$inc': {'retry_count': 1}})
    school = school_data_collection.find_one({'WEBADDR': base_url})
    if school and school.get('retry_count', 0) >= RETRY_LIMIT:
        school_data_collection.update_one({'WEBADDR': base_url}, {'$set': {'error': True, 'processing': False}})

async def process_data(data):
    """Process the data for a given web address, including crawling and saving the content."""
    global interrupted
    
    base_url = data.get('WEBADDR')
    unitid = data.get('UNITID')

    if not base_url or not unitid:
        logger.warning("Missing required data fields. Skipping this entry.")
        return
    
    logger.info(f"Processing: {base_url}")
    normalized_base_url = normalize_base_url(base_url)

    # Check if the document has already been processed
    if baseurl_already_processed(normalized_base_url):
        logger.info(f"Skipping processing for {normalized_base_url} as it has already been processed.")
        return

    # Use Spider to crawl the URL
    crawler_params = {
        'limit': 500,
        'proxy_enabled': True,
        'store_data': False,
        'metadata': False,
        'request': 'smart',
        'stream': True
    }
    crawl_results = None
    headers = {
        'Authorization': SPIDER_API_KEY,
        'Content-Type': 'application/json',
    }

    try:
        # Make a POST request to the Spider API with all crawler parameters
        response = requests.post(
            'https://api.spider.cloud/crawl',
            headers=headers,
            json={
                "limit": crawler_params['limit'],
                "proxy_enabled": crawler_params['proxy_enabled'],
                "store_data": crawler_params['store_data'],
                "metadata": crawler_params['metadata'],
                "request": crawler_params['request'],
                "stream": crawler_params['stream'],
                "url": normalized_base_url
            },
            stream=True
        )
        response.raise_for_status()
        buffer = b""
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                buffer += chunk
                try:
                    data = json.loads(buffer.decode('utf-8'))
                    crawl_results = data.get('items', [])
                    buffer = b""
                except json.JSONDecodeError:
                    continue

    except Exception as e:
        logger.error(f"Failed to fetch data for {normalized_base_url}, skipping: {e}")
        # Mark as error if fetching fails
        mark_as_error(base_url)
        return

    if crawl_results is None:
        logger.error(f"Failed to fetch data for {normalized_base_url}, skipping.")
        # Mark as error if fetching fails
        mark_as_error(base_url)
        return

    for item in crawl_results:
        if interrupted:
            school_data_collection.update_one({'WEBADDR': base_url}, {'$set': {'processing': False}})
            return

        html_content = item['content']
        if html_content is None:
            logger.warning(f"No content retrieved for {item['url']}")
            continue  # Skip processing this item if there is no content
        specific_page_url = item['url']
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
        except ParserRejectedMarkup:
            soup = BeautifulSoup(html_content, 'lxml')  # fallback parser

        # Clean up the content by removing unwanted tags and elements
        tags_to_remove = ['script', 'style', 'nav', 'footer', 'header', 'aside', 'form']
        classes_or_ids_to_remove = ['menu', 'sidebar', 'ad-section', 'navbar', 'modal', 'footer', 'masthead', 'comment', 'widget']
        for tag in tags_to_remove:
            for element in soup.find_all(tag):
                element.decompose()

        for identifier in classes_or_ids_to_remove:
            for element in soup.find_all(class_=identifier):
                element.decompose()
            for element in soup.find_all(id=identifier):
                element.decompose()

        # Extract and clean up text content
        text = soup.get_text(separator=' ')
        text = re.sub(r'[\r\n]+', '\n', text)
        text = re.sub(r'\s{2,}', ' ', text)
        text = re.sub(r'&[a-z]+;', '', text)

        # Generate a unique document ID based on content
        doc_id = hash_content(text + specific_page_url)
        try:
            insert_document(doc_id, normalized_base_url, unitid, normalized_base_url, specific_page_url, text)
        except DocumentTooLarge:
            logger.error(f"Document for URL {specific_page_url} is too large even after truncation. Skipping insertion.")
        logger.info(normalized_base_url)

    # Mark as processed after successful processing
    school_data_collection.update_one({'WEBADDR': base_url}, {'$set': {'processed': True, 'processing': False}})

async def fetch_and_process_next_url():
    """Fetch the next URL to process from the school_data collection and process it."""
    # Fetch the next web address to process
    data = school_data_collection.find_one_and_update(
        {
            'processed': {'$ne': True},  # Has not been processed
            'processing': {'$ne': True},  # Is not currently being processed
            'error': {'$exists': False}   # Does not have an error flag
        },
        {'$set': {'processing': True}},  # Set 'processing' to True to indicate it's being worked on
        return_document=True
    )
    
    if data is None:
        logger.info("No more unprocessed data available.")
        return

    # Process the retrieved data
    await process_data(data)

async def scrape_and_store():
    """Main function to start the scraping and storing process."""
    # Create indexes in MongoDB collection
    create_indexes()
    processed_count = 0

    # Continue processing URLs until the limit is reached or the process is interrupted
    while processed_count < URL_PROCESS_LIMIT and not interrupted:
        await fetch_and_process_next_url()
        processed_count += 1

        # Log resource usage for monitoring purposes
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        logger.info(f"Memory Usage: RSS = {memory_info.rss / (1024 ** 2)} MB")

    logger.info("Data scraping and storage process completed successfully.")

if __name__ == "__main__":
    # Run the main scraping and storing function using asyncio
    asyncio.run(scrape_and_store())

