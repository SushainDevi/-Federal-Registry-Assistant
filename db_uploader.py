import asyncio
import json
import logging
import datetime
from pathlib import Path
import aiomysql
from config import DB_CONFIG
from db_utils import get_connection, insert_document, log_pipeline_run
from pipeline_config import PROCESSED_DATA_DIR, BATCH_SIZE

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("db_uploader")

class FederalRegistryUploader:
    def __init__(self):
        self.processed_data_dir = PROCESSED_DATA_DIR
        self.batch_size = BATCH_SIZE
    
    async def read_processed_file(self, filepath):
        """Read processed data file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading processed file {filepath}: {str(e)}")
            return None
    
    async def upload_document(self, document):
        """Upload a single document to the database"""
        try:
            # Check for required fields
            if not document.get('id'):
                logger.warning(f"Skipping document with no ID: {document.get('document_number', 'Unknown')}")
                return False
            
            # Extract topics for the document topics table
            topics = document.pop('topics', []) if 'topics' in document else []
            
            # Insert the document
            await insert_document(document)
            
            # Handle topics (if any)
            if topics:
                await self.handle_document_topics(document.get('id'), topics)
            
            return True
        except ValueError as ve:
            logger.warning(f"Validation error for document {document.get('document_number')}: {str(ve)}")
            return False
        except Exception as e:
            logger.error(f"Error uploading document {document.get('document_number')}: {str(e)}")
            return False
    
    async def handle_document_topics(self, document_id, topics):
        """Insert topics and link them to the document"""
        if not topics or not document_id:
            return
        
        conn = await get_connection()
        try:
            async with conn.cursor() as cursor:
                # Insert topics if they don't exist
                for topic in topics:
                    if not topic:
                        continue
                    
                    # Insert topic if it doesn't exist
                    await cursor.execute(
                        "INSERT IGNORE INTO topics (name) VALUES (%s)",
                        (topic,)
                    )
                    
                    # Get topic id
                    await cursor.execute(
                        "SELECT id FROM topics WHERE name = %s",
                        (topic,)
                    )
                    result = await cursor.fetchone()
                    
                    if result:
                        topic_id = result['id']
                        
                        # Link document to topic
                        await cursor.execute(
                            "INSERT IGNORE INTO document_topics (document_id, topic_id) VALUES (%s, %s)",
                            (document_id, topic_id)
                        )
        finally:
            conn.close()
    
    async def upload_processed_file(self, filepath):
        """Upload all documents from a processed file"""
        data = await self.read_processed_file(filepath)
        if not data:
            return 0
        
        success_count = 0
        
        for document in data:
            if await self.upload_document(document):
                success_count += 1
                
            # Avoid overwhelming the database
            await asyncio.sleep(0.01)
        
        return success_count
    
    async def upload_all_processed_files(self, date_str=None):
        """Upload all processed files for a specific date"""
        if date_str:
            pattern = f"{date_str}_*.json"
        else:
            # Default to today's date
            pattern = f"{datetime.datetime.now().strftime('%Y-%m-%d')}_*.json"
        
        total_documents = 0
        uploaded_documents = 0
        
        # Log pipeline run start
        run_date = datetime.datetime.now().date()
        start_time = datetime.datetime.now()
        
        log_data = {
            'run_date': run_date,
            'start_time': start_time,
            'status': 'RUNNING',
            'records_processed': 0,
            'new_records': 0,
            'error_message': None
        }
        
        log_id = await log_pipeline_run(log_data)
        
        try:
            for filepath in self.processed_data_dir.glob(pattern):
                logger.info(f"Uploading file: {filepath}")
                
                # Count documents in file
                data = await self.read_processed_file(filepath)
                if data:
                    file_doc_count = len(data)
                    total_documents += file_doc_count
                    
                    # Upload documents
                    success_count = await self.upload_processed_file(filepath)
                    uploaded_documents += success_count
                    
                    logger.info(f"Uploaded {success_count}/{file_doc_count} documents from {filepath}")
            
            # Update pipeline log
            end_time = datetime.datetime.now()
            log_data = {
                'run_date': run_date,
                'start_time': start_time,
                'end_time': end_time,
                'status': 'COMPLETED',
                'records_processed': total_documents,
                'new_records': uploaded_documents,
                'updated_records': total_documents - uploaded_documents,
                'error_message': None
            }
            
            await log_pipeline_run(log_data)
            
            return uploaded_documents, total_documents
        
        except Exception as e:
            # Update pipeline log with error
            end_time = datetime.datetime.now()
            log_data = {
                'run_date': run_date,
                'start_time': start_time,
                'end_time': end_time,
                'status': 'FAILED',
                'records_processed': total_documents,
                'new_records': uploaded_documents,
                'updated_records': total_documents - uploaded_documents,
                'error_message': str(e)
            }
            
            await log_pipeline_run(log_data)
            
            logger.error(f"Error in upload process: {str(e)}")
            return uploaded_documents, total_documents

async def main():
    uploader = FederalRegistryUploader()
    
    # Upload files from today
    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Starting database upload for {date_str}")
    
    uploaded, total = await uploader.upload_all_processed_files(date_str)
    
    logger.info(f"Completed database upload. Uploaded {uploaded}/{total} documents.")

if __name__ == "__main__":
    asyncio.run(main())