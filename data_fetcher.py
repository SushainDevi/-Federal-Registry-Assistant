import asyncio
import aiohttp
import json
import datetime
import logging
from pathlib import Path
from pipeline_config import FR_API_BASE_URL, RAW_DATA_DIR, DOCUMENT_TYPES, BATCH_SIZE

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("data_fetcher")

class FederalRegistryFetcher:
    def __init__(self):
        self.api_base_url = FR_API_BASE_URL
        self.raw_data_dir = RAW_DATA_DIR
    
    async def fetch_documents(self, start_date, end_date, document_type=None, per_page=BATCH_SIZE):
        """Fetch documents from Federal Registry API within a date range"""
        params = {
            'per_page': per_page,
            'order': 'newest',
            'conditions[publication_date][gte]': start_date,
            'conditions[publication_date][lte]': end_date,
        }
        
        if document_type:
            params['conditions[type]'] = document_type
        
        url = f"{self.api_base_url}/documents"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.error(f"Error fetching documents: {response.status}")
                        return None
        except Exception as e:
            logger.error(f"Exception while fetching documents: {str(e)}")
            return None
    
    async def fetch_document_details(self, document_number):
        """Fetch detailed information for a specific document"""
        url = f"{self.api_base_url}/documents/{document_number}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Ensure the document has an ID
                        if not data.get('id') and document_number:
                            data['id'] = f"fr-{document_number}"
                        
                        return data
                    else:
                        logger.error(f"Error fetching document details: {response.status}")
                        return None
        except Exception as e:
            logger.error(f"Exception while fetching document details: {str(e)}")
            return None
    
    async def save_raw_data(self, data, filename):
        """Save raw API response to file"""
        filepath = self.raw_data_dir / filename
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved raw data to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error saving raw data: {str(e)}")
            return None
    
    async def fetch_and_save_by_date_range(self, start_date, end_date):
        """Fetch documents for each document type and save raw data"""
        run_date = datetime.datetime.now().strftime("%Y-%m-%d")
        saved_files = []
        
        for doc_type in DOCUMENT_TYPES:
            logger.info(f"Fetching {doc_type} documents from {start_date} to {end_date}")
            
            data = await self.fetch_documents(start_date, end_date, doc_type)
            if data:
                filename = f"{run_date}_{doc_type}.json"
                saved_file = await self.save_raw_data(data, filename)
                if saved_file:
                    saved_files.append(saved_file)
                    
                    # Fetch detailed information for each document
                    if 'results' in data:
                        for doc in data['results']:
                            if 'document_number' in doc:
                                doc_number = doc['document_number']
                                logger.info(f"Fetching details for document {doc_number}")
                                
                                details = await self.fetch_document_details(doc_number)
                                if details:
                                    detail_filename = f"{run_date}_{doc_type}_{doc_number}_details.json"
                                    detail_file = await self.save_raw_data(details, detail_filename)
                                    if detail_file:
                                        saved_files.append(detail_file)
                                
                                # Avoid overwhelming the API
                                await asyncio.sleep(0.5)
        
        return saved_files

async def main():
    fetcher = FederalRegistryFetcher()
    
    # Get yesterday's date to fetch most recent documents
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    
    logger.info(f"Starting data fetch from {yesterday} to {today}")
    saved_files = await fetcher.fetch_and_save_by_date_range(yesterday, today)
    logger.info(f"Completed data fetch. Saved {len(saved_files)} files.")

if __name__ == "__main__":
    asyncio.run(main())