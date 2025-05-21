# db_test.py
import asyncio
import aiomysql
from config import DB_CONFIG

async def test_connection():
    conn = await aiomysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        db=DB_CONFIG['db']
    )
    
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1 as test")
        result = await cursor.fetchone()
        print(f"Connection successful: {result}")
    
    conn.close()
    print("Connection closed")

if __name__ == "__main__":
    asyncio.run(test_connection())