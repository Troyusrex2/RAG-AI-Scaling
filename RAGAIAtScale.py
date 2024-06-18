import logging
import os
import json
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
SPIDER_API_KEY = os.environ.get("SPIDER_API_KEY")
UPDATE_EXISTING = False # I generally never update existing doucments. I delete them and rescrape them. But this flag is for the rare ocassions I do update.
URL_PROCESS_LIMIT = 200 # I usually set this very low for testing and high when running at scale. On the t2.nano 200 allows for a lot of URLs to be processed but not so many that I hit a lot of memory issues.
MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = 'scraped_data'
COLLECTION_NAME = 'documents'
SCHOOL_DATA_COLLECTION = 'school_data'
RETRY_LIMIT = 3 # This is the Spider retry. Frankly, if it fails once it usually fails 3 times. 
MAX_DOC_SIZE = 1024 * 1024 # 1MB. For me this is probably way over what's truly useful.

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
documents_collection = db[COLLECTION_NAME]
school_data_collection = db[SCHOOL_DATA_COLLECTION]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

crawler_params = {
    'limit': 500, # This limits spider to 500 pages on a website. Some school websites are much larger. If a website returns all 500 I'll look to see if I think the pages returned are represntative or if I need to get more pages.
    'proxy_enabled': True,
    'store_data': False, # Sometimes Spider can get things itself I can't get through the API. In those cases, this can be turned on and I can get it through the end point.
    'metadata': False,
    'request': 'smart', 
    'stream': True
}
crawl_results = None

def hash_content(content): # This is to weed out duplicates as many sites have a huge number of dupes. don't include the URL in the hash.
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

def normalize_base_url(url): # I'm pulling from a list of URLs formatted in different ways. I want a standard way to store them when referencing the content so I normailze them.
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    return url.rstrip('/')

def document_exists(base_url, content_hash): # This is the actual dupe check.
    normalized_url = normalize_base_url(base_url)
    return documents_collection.count_documents({'Base_URL': normalized_url, 'content_hash': content_hash}) > 0

def truncate_content(content, max_size): # for my purposes data beyond the size I need is fine to truncate. I had been chunking it but found the additional data did not add to accuracy of RAG results. YMMV depending on your use case.
    content_bytes = content.encode('utf-8')
    if len(content_bytes) > max_size:
        truncated_bytes = content_bytes[:max_size]
        return truncated_bytes.decode('utf-8', errors='ignore')
    return content

def insert_document(doc_id, base_url, unitid, website, url, content): # Save the results.
    content_hash = hash_content(content)
    truncated_content = truncate_content(content, MAX_DOC_SIZE)
    if not document_exists(base_url, content_hash) or UPDATE_EXISTING:
        try:
            documents_collection.insert_one({
                'doc_id': doc_id,
                'Base_URL': base_url,  #because the data I'm pulling this list from isn't normlized, I keep the origional url to reference back. If you can avoid having to do something like this, you should.
                'UNITID': unitid, # A Unique ID per normalized URL
                'website': website, #this is the normalized URL
                'url': url, #This is the actual page being saved. Probably not the best name as it's confusing with wesbite and Base_URL.
                'date_scraped': datetime.now().isoformat(),
                'content': truncated_content, #this is the cleaned and truncated content of the web page.
                'content_hash': content_hash
            })
        except DuplicateKeyError:
            logger.warning(f"Document with doc_id {doc_id} already exists. Skipping insertion.")
        except DocumentTooLarge:
            logger.error(f"Document with doc_id {doc_id} is too large even after truncation. Skipping insertion.")
    else:
        logger.info(f"Skipping document insertion for URL {base_url} as it already exists with the same content.")

def baseurl_already_processed(base_url): # just a double check. If this school already has pages saved, skip it.
    normalized_url = normalize_base_url(base_url)
    document = documents_collection.find_one({'Base_URL': normalized_url})
    if document:
        school_data_collection.update_one({'WEBADDR': base_url}, {'$set': {'processed': True, 'processing': False}})
        return True
    return False

def mark_as_error(base_url):
    school_data_collection.update_one({'WEBADDR': base_url}, {'$inc': {'retry_count': 1}})
    school = school_data_collection.find_one({'WEBADDR': base_url})
    if school and school.get('retry_count', 0) >= RETRY_LIMIT:
        school_data_collection.update_one({'WEBADDR': base_url}, {'$set': {'error': True, 'processing': False}})

def clean_html_content(html_content): #98% of these pages is styling and stuff other than the data a RAG needs. 
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
    except ParserRejectedMarkup:
        soup = BeautifulSoup(html_content, 'lxml')

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
    
    text = soup.get_text(separator=' ')
    text = re.sub(r'[\r\n]+', '\n', text)
    text = re.sub(r'\s{2,}', ' ', text)
    text = re.sub(r'&[a-z]+;', '', text)
    return text

def process_data(data):
    global interrupted
    
    base_url = data.get('WEBADDR')
    unitid = data.get('UNITID')

    if not base_url or not unitid:
        logger.warning("Missing required data fields. Skipping this entry.")
        return
    
    logger.info(f"Processing: {base_url}")
    normalized_base_url = normalize_base_url(base_url)

    if baseurl_already_processed(normalized_base_url):
        logger.info(f"Skipping processing for {normalized_base_url} as it has already been processed.")
        return

    headers = {
        'Authorization': SPIDER_API_KEY,
        'Content-Type': 'application/json',
    }

    try:
        response = requests.post(
            'https://api.spider.cloud/crawl',
            headers=headers,
            json={"limit": crawler_params['limit'], "url": normalized_base_url},
            stream=True
        )
        response.raise_for_status()
        buffer = b""
        crawl_results = None
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
        mark_as_error(base_url)
        return

    if crawl_results is None:
        logger.error(f"Failed to fetch data for {normalized_base_url}, skipping.")
        mark_as_error(base_url)
        return

    for item in crawl_results:
        if interrupted:
            school_data_collection.update_one({'WEBADDR': base_url}, {'$set': {'processing': False}})
            return

        html_content = item['content']
        if html_content is None:
            logger.warning(f"No content retrieved for {item['url']}")
            continue

        specific_page_url = item['url']
        cleaned_text = clean_html_content(html_content)
        
        doc_id = hash_content(cleaned_text + specific_page_url)
        try:
            insert_document(doc_id, normalized_base_url, unitid, normalized_base_url, specific_page_url, cleaned_text)
        except DocumentTooLarge:
            logger.error(f"Document for URL {specific_page_url} is too large even after truncation. Skipping insertion.")
        logger.info(normalized_base_url)

    school_data_collection.update_one({'WEBADDR': base_url}, {'$set': {'processed': True, 'processing': False}})

def fetch_and_process_next_url():
    data = school_data_collection.find_one_and_update(
        {'processed': {'$ne': True}, 'processing': {'$ne': True}, 'error': {'$exists': False}},
        {'$set': {'processing': True}},
        return_document=True
    )
    
    if data is None:
        logger.info("No more unprocessed data available.")
        return

    process_data(data)

def scrape_and_store():
    processed_count = 0

    while processed_count < URL_PROCESS_LIMIT:
        fetch_and_process_next_url()
        processed_count += 1

    logger.info("Data scraping and storage process completed successfully.")

if __name__ == "__main__":
    scrape_and_store()
