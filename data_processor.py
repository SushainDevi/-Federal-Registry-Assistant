import asyncio
import json
import logging
import datetime
import aiofiles
from pathlib import Path
from pipeline_config import RAW_DATA_DIR, PROCESSED_DATA_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("data_processor")

class FederalRegistryProcessor:
    def __init__(self):
        self.raw_data_dir = RAW_DATA_DIR
        self.processed_data_dir = PROCESSED_DATA_DIR
    
    async def read_raw_file(self, filepath):
        """Read raw data file asynchronously"""
        try:
            async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                content = await f.read()
                return json.loads(content)
        except Exception as e:
            logger.error(f"Error reading file {filepath}: {str(e)}")
            return None
    
    async def process_document(self, document):
        """Process a single document"""
        try:
            # Generate an ID if it doesn't exist - use document_number as fallback
            doc_id = document.get('id')
            doc_number = document.get('document_number')
            
            if not doc_id and doc_number:
                # Use document_number as ID if ID is missing
                doc_id = f"fr-{doc_number}"
            elif not doc_id and not doc_number:
                # Skip documents with no ID and no document number
                logger.warning(f"Skipping document with no ID or document number: {document.get('title', 'Unknown')}")
                return None
            
            # Extract relevant fields
            processed_doc = {
                'id': doc_id,
                'document_number': doc_number,
                'title': document.get('title'),
                'type': document.get('type'),
                'agency': document.get('agencies', [{}])[0].get('name') if document.get('agencies') else None,
                'agency_id': document.get('agencies', [{}])[0].get('id') if document.get('agencies') else None,
                'publication_date': document.get('publication_date'),
                'effective_date': document.get('effective_on'),
                'action': document.get('action'),
                'presidential_document_type': document.get('presidential_document_type', {}).get('name') if document.get('presidential_document_type') else None,
                'executive_order_number': document.get('executive_order_number'),
                'html_url': document.get('html_url'),
                'pdf_url': document.get('pdf_url'),
                'abstract': document.get('abstract'),
                'full_text': document.get('body') or document.get('raw_text_url'),
                'topics': [topic.get('name') for topic in document.get('topics', [])]
            }
            
            return processed_doc
        except Exception as e:
            logger.error(f"Error processing document {document.get('document_number')}: {str(e)}")
            return None
    
    async def process_raw_file(self, filepath):
        """Process a raw data file"""
        data = await self.read_raw_file(filepath)
        if not data:
            return None
        
        processed_documents = []
        
        # Handle both list results and single document details
        if 'results' in data:
            for doc in data['results']:
                processed_doc = await self.process_document(doc)
                if processed_doc:
                    processed_documents.append(processed_doc)
        else:
            # This is a detail file for a single document
            processed_doc = await self.process_document(data)
            if processed_doc:
                processed_documents.append(processed_doc)
        
        return processed_documents
    
    async def save_processed_data(self, data, filename):
        """Save processed data to file"""
        filepath = self.processed_data_dir / filename
        try:
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))
            logger.info(f"Saved processed data to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error saving processed data: {str(e)}")
            return None
    
    async def process_raw_files(self, date_str=None):
        """Process all raw files for a specific date"""
        if date_str:
            pattern = f"{date_str}_*.json"
        else:
            # Default to today's date
            pattern = f"{datetime.datetime.now().strftime('%Y-%m-%d')}_*.json"
        
        all_processed_docs = []
        processed_files = []
        
        for filepath in self.raw_data_dir.glob(pattern):
            logger.info(f"Processing file: {filepath}")
            
            processed_docs = await self.process_raw_file(filepath)
            if processed_docs:
                all_processed_docs.extend(processed_docs)
                
                # Save processed data with same filename but in processed directory
                processed_filename = filepath.name
                processed_filepath = await self.save_processed_data(processed_docs, processed_filename)
                if processed_filepath:
                    processed_files.append(processed_filepath)
        
        return all_processed_docs, processed_files

async def main():
    processor = FederalRegistryProcessor()
    
    # Process files from today
    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Starting data processing for {date_str}")
    
    processed_docs, processed_files = await processor.process_raw_files(date_str)
    
    logger.info(f"Completed data processing. Processed {len(processed_docs)} documents and saved to {len(processed_files)} files.")

if __name__ == "__main__":
    asyncio.run(main())