import asyncio
import logging
import datetime
import argparse
from data_fetcher import FederalRegistryFetcher
from data_processor import FederalRegistryProcessor
from db_uploader import FederalRegistryUploader
from pipeline_config import DEFAULT_START_DATE, DEFAULT_END_DATE

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("pipeline_orchestrator")

class FederalRegistryPipeline:
    def __init__(self):
        self.fetcher = FederalRegistryFetcher()
        self.processor = FederalRegistryProcessor()
        self.uploader = FederalRegistryUploader()
    
    async def run_pipeline(self, start_date=None, end_date=None):
        """Run the complete data pipeline"""
        if not start_date:
            start_date = DEFAULT_START_DATE
        
        if not end_date:
            end_date = DEFAULT_END_DATE
        
        logger.info(f"Starting Federal Registry pipeline from {start_date} to {end_date}")
        
        try:
            # Step 1: Fetch data
            logger.info("Step 1: Fetching data")
            saved_files = await self.fetcher.fetch_and_save_by_date_range(start_date, end_date)
            logger.info(f"Fetched and saved {len(saved_files)} files")
            
            # Step 2: Process data
            logger.info("Step 2: Processing data")
            processed_docs, processed_files = await self.processor.process_raw_files()
            logger.info(f"Processed {len(processed_docs)} documents into {len(processed_files)} files")
            
            # Step 3: Upload to database
            logger.info("Step 3: Uploading to database")
            uploaded, total = await self.uploader.upload_all_processed_files()
            logger.info(f"Uploaded {uploaded}/{total} documents to database")
            
            logger.info("Pipeline completed successfully")
            return True
        
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return False

async def main():
    parser = argparse.ArgumentParser(description='Federal Registry Data Pipeline')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    pipeline = FederalRegistryPipeline()
    await pipeline.run_pipeline(args.start_date, args.end_date)

if __name__ == "__main__":
    asyncio.run(main())