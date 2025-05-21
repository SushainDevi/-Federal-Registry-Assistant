# db_tools.py
import asyncio
import datetime
import logging
from typing import List, Dict, Any, Optional, Tuple
import aiomysql
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("db_tools")

async def get_connection():
    """Get an async MySQL connection"""
    return await aiomysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        db=DB_CONFIG['db'],
        autocommit=True,
        cursorclass=aiomysql.DictCursor
    )

# Database query tools
async def search_documents(
    query: str = "",
    doc_type: str = "",
    agency: str = "",
    start_date: str = "",
    end_date: str = "",
    limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Search documents based on various criteria
    
    Args:
        query: Text to search for in title or abstract
        doc_type: Type of document (RULE, PRORULE, NOTICE, PRESDOCU)
        agency: Agency name or partial name
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        limit: Maximum number of results
        
    Returns:
        List of matching documents
    """
    conn = await get_connection()
    try:
        conditions = []
        params = []
        
        # Build search conditions
        if query:
            conditions.append("(title LIKE %s OR abstract LIKE %s)")
            query_param = f"%{query}%"
            params.extend([query_param, query_param])
        
        if doc_type:
            conditions.append("type = %s")
            params.append(doc_type)
        
        if agency:
            conditions.append("agency LIKE %s")
            params.append(f"%{agency}%")
        
        if start_date:
            try:
                # Validate date format
                datetime.datetime.strptime(start_date, '%Y-%m-%d')
                conditions.append("publication_date >= %s")
                params.append(start_date)
            except ValueError:
                logger.warning(f"Invalid start_date format: {start_date}")
        
        if end_date:
            try:
                # Validate date format
                datetime.datetime.strptime(end_date, '%Y-%m-%d')
                conditions.append("publication_date <= %s")
                params.append(end_date)
            except ValueError:
                logger.warning(f"Invalid end_date format: {end_date}")
        
        # Build query
        query_str = "SELECT id, document_number, title, type, agency, publication_date, abstract FROM documents"
        
        if conditions:
            query_str += " WHERE " + " AND ".join(conditions)
        
        query_str += " ORDER BY publication_date DESC LIMIT %s"
        params.append(limit)
        
        # Execute query
        async with conn.cursor() as cursor:
            await cursor.execute(query_str, params)
            results = await cursor.fetchall()
            
            # Convert dates to string for JSON serialization
            for row in results:
                if 'publication_date' in row and row['publication_date']:
                    row['publication_date'] = row['publication_date'].isoformat()
            
            return results
    except Exception as e:
        logger.error(f"Error searching documents: {str(e)}")
        return []
    finally:
        conn.close()

async def get_document_details(document_id: str) -> Optional[Dict[str, Any]]:
    """
    Get detailed information about a specific document
    
    Args:
        document_id: Document ID
        
    Returns:
        Document details or None if not found
    """
    conn = await get_connection()
    try:
        async with conn.cursor() as cursor:
            # Get document details
            await cursor.execute(
                """
                SELECT * FROM documents 
                WHERE id = %s OR document_number = %s
                """, 
                (document_id, document_id)
            )
            document = await cursor.fetchone()
            
            if not document:
                return None
            
            # Convert dates to string for JSON serialization
            if 'publication_date' in document and document['publication_date']:
                document['publication_date'] = document['publication_date'].isoformat()
            
            if 'effective_date' in document and document['effective_date']:
                document['effective_date'] = document['effective_date'].isoformat()
            
            if 'created_at' in document and document['created_at']:
                document['created_at'] = document['created_at'].isoformat()
            
            if 'updated_at' in document and document['updated_at']:
                document['updated_at'] = document['updated_at'].isoformat()
            
            # Get document topics
            await cursor.execute(
                """
                SELECT t.name 
                FROM topics t
                JOIN document_topics dt ON t.id = dt.topic_id
                WHERE dt.document_id = %s
                """,
                (document['id'],)
            )
            topics = await cursor.fetchall()
            document['topics'] = [topic['name'] for topic in topics]
            
            return document
    except Exception as e:
        logger.error(f"Error getting document details: {str(e)}")
        return None
    finally:
        conn.close()

async def get_recent_documents(days: int = 7, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get recent documents published within the specified number of days
    
    Args:
        days: Number of days to look back
        limit: Maximum number of results
        
    Returns:
        List of recent documents
    """
    conn = await get_connection()
    try:
        # Calculate date range
        end_date = datetime.datetime.now().date()
        start_date = end_date - datetime.timedelta(days=days)
        
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT id, document_number, title, type, agency, publication_date, abstract
                FROM documents
                WHERE publication_date BETWEEN %s AND %s
                ORDER BY publication_date DESC
                LIMIT %s
                """,
                (start_date, end_date, limit)
            )
            results = await cursor.fetchall()
            
            # Convert dates to string for JSON serialization
            for row in results:
                if 'publication_date' in row and row['publication_date']:
                    row['publication_date'] = row['publication_date'].isoformat()
            
            return results
    except Exception as e:
        logger.error(f"Error getting recent documents: {str(e)}")
        return []
    finally:
        conn.close()

async def get_executive_orders(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get recent executive orders
    
    Args:
        limit: Maximum number of results
        
    Returns:
        List of executive orders
    """
    conn = await get_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT id, document_number, title, publication_date, executive_order_number, abstract
                FROM documents
                WHERE presidential_document_type = 'Executive Order'
                ORDER BY publication_date DESC
                LIMIT %s
                """,
                (limit,)
            )
            results = await cursor.fetchall()
            
            # Convert dates to string for JSON serialization
            for row in results:
                if 'publication_date' in row and row['publication_date']:
                    row['publication_date'] = row['publication_date'].isoformat()
            
            return results
    except Exception as e:
        logger.error(f"Error getting executive orders: {str(e)}")
        return []
    finally:
        conn.close()

async def search_by_topic(topic: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Search documents by topic
    
    Args:
        topic: Topic to search for
        limit: Maximum number of results
        
    Returns:
        List of documents with the specified topic
    """
    conn = await get_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT d.id, d.document_number, d.title, d.type, d.agency, d.publication_date, d.abstract
                FROM documents d
                JOIN document_topics dt ON d.id = dt.document_id
                JOIN topics t ON dt.topic_id = t.id
                WHERE t.name LIKE %s
                ORDER BY d.publication_date DESC
                LIMIT %s
                """,
                (f"%{topic}%", limit)
            )
            results = await cursor.fetchall()
            
            # Convert dates to string for JSON serialization
            for row in results:
                if 'publication_date' in row and row['publication_date']:
                    row['publication_date'] = row['publication_date'].isoformat()
            
            return results
    except Exception as e:
        logger.error(f"Error searching by topic: {str(e)}")
        return []
    finally:
        conn.close()

async def get_document_count_by_type() -> List[Dict[str, Any]]:
    """
    Get the count of documents by type
    
    Returns:
        List of document types with counts
    """
    conn = await get_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT type, COUNT(*) as count
                FROM documents
                GROUP BY type
                ORDER BY count DESC
                """
            )
            results = await cursor.fetchall()
            return results
    except Exception as e:
        logger.error(f"Error getting document count by type: {str(e)}")
        return []
    finally:
        conn.close()

async def get_document_count_by_agency(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get the count of documents by agency
    
    Args:
        limit: Maximum number of results
        
    Returns:
        List of agencies with document counts
    """
    conn = await get_connection()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT agency, COUNT(*) as count
                FROM documents
                WHERE agency IS NOT NULL
                GROUP BY agency
                ORDER BY count DESC
                LIMIT %s
                """,
                (limit,)
            )
            results = await cursor.fetchall()
            return results
    except Exception as e:
        logger.error(f"Error getting document count by agency: {str(e)}")
        return []
    finally:
        conn.close()

# For testing the tools
async def test_tools():
    """Test all database tools"""
    print("Testing search_documents...")
    results = await search_documents("climate", limit=3)
    print(f"Found {len(results)} results for 'climate'")
    
    print("\nTesting get_recent_documents...")
    recent = await get_recent_documents(days=30, limit=3)
    print(f"Found {len(recent)} recent documents")
    
    print("\nTesting get_executive_orders...")
    orders = await get_executive_orders(limit=3)
    print(f"Found {len(orders)} executive orders")
    
    print("\nTesting get_document_count_by_type...")
    type_counts = await get_document_count_by_type()
    print(f"Document counts by type: {type_counts}")

if __name__ == "__main__":
    # Test the tools
    asyncio.run(test_tools())