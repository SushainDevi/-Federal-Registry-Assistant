import asyncio
import aiomysql
from config import DB_CONFIG
import datetime

async def get_connection():
    return await aiomysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        db=DB_CONFIG['db'],
        autocommit=True,
        cursorclass=aiomysql.DictCursor
    )

async def execute_query(query, params=None):
    conn = await get_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(query, params)
            return await cursor.fetchall()
    finally:
        conn.close()

async def execute_many(query, params_list):
    conn = await get_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.executemany(query, params_list)
            await conn.commit()
    finally:
        conn.close()

async def insert_document(document_data):
    # Ensure required fields have values
    if not document_data.get('id'):
        raise ValueError("Document ID cannot be null")
    
    if not document_data.get('document_number'):
        # Use ID as document number if missing
        document_data['document_number'] = document_data['id']
    
    if not document_data.get('title'):
        document_data['title'] = 'Untitled Document'
    
    if not document_data.get('type'):
        document_data['type'] = 'UNKNOWN'
    
    if not document_data.get('publication_date'):
        # Use current date as fallback
        document_data['publication_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
    
    query = """
    INSERT INTO documents (
        id, document_number, title, type, agency, 
        publication_date, effective_date, action,
        presidential_document_type, executive_order_number,
        html_url, pdf_url, abstract, full_text
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) ON DUPLICATE KEY UPDATE
        title = VALUES(title),
        agency = VALUES(agency),
        publication_date = VALUES(publication_date),
        effective_date = VALUES(effective_date),
        action = VALUES(action),
        presidential_document_type = VALUES(presidential_document_type),
        executive_order_number = VALUES(executive_order_number),
        html_url = VALUES(html_url),
        pdf_url = VALUES(pdf_url),
        abstract = VALUES(abstract),
        full_text = VALUES(full_text)
    """
    
    params = (
        document_data.get('id'),
        document_data.get('document_number'),
        document_data.get('title'),
        document_data.get('type'),
        document_data.get('agency'),
        document_data.get('publication_date'),
        document_data.get('effective_date'),
        document_data.get('action'),
        document_data.get('presidential_document_type'),
        document_data.get('executive_order_number'),
        document_data.get('html_url'),
        document_data.get('pdf_url'),
        document_data.get('abstract'),
        document_data.get('full_text')
    )
    
    return await execute_query(query, params)

async def log_pipeline_run(run_data):
    query = """
    INSERT INTO pipeline_logs (
        run_date, start_time, end_time, status,
        records_processed, new_records, updated_records, error_message
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    
    params = (
        run_data.get('run_date'),
        run_data.get('start_time'),
        run_data.get('end_time'),
        run_data.get('status'),
        run_data.get('records_processed', 0),
        run_data.get('new_records', 0),
        run_data.get('updated_records', 0),
        run_data.get('error_message')
    )
    
    return await execute_query(query, params)