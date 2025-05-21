# agent.py
import asyncio
import json
import logging
import os
import datetime
from typing import Dict, List, Any, Optional, Tuple
import aiohttp
from db_tools import (
    search_documents, 
    get_document_details, 
    get_recent_documents, 
    get_executive_orders,
    search_by_topic, 
    get_document_count_by_type, 
    get_document_count_by_agency
)
from pipeline_orchestrator import FederalRegistryPipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("agent")

# Configure LLM settings
OLLAMA_API_URL = os.getenv("OLLAMA_API_URL", "http://localhost:11434/api/chat")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b")

# Define our tool schema
TOOL_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "search_documents",
            "description": "Search Federal Registry documents based on various criteria",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Text to search for in title or abstract"
                    },
                    "doc_type": {
                        "type": "string",
                        "description": "Type of document (RULE, PRORULE, NOTICE, PRESDOCU)"
                    },
                    "agency": {
                        "type": "string",
                        "description": "Agency name or partial name"
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return"
                    }
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_document_details",
            "description": "Get detailed information about a specific document",
            "parameters": {
                "type": "object",
                "required": ["document_id"],
                "properties": {
                    "document_id": {
                        "type": "string",
                        "description": "Document ID or document number"
                    }
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_recent_documents",
            "description": "Get recent documents published within the specified number of days",
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Number of days to look back"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return"
                    }
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_executive_orders",
            "description": "Get recent executive orders",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return"
                    }
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_by_topic",
            "description": "Search documents by topic",
            "parameters": {
                "type": "object",
                "required": ["topic"],
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "Topic to search for"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return"
                    }
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_document_count_by_type",
            "description": "Get the count of documents by type",
            "parameters": {
                "type": "object",
                "properties": {}
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_document_count_by_agency",
            "description": "Get the count of documents by agency",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return"
                    }
                }
            }
        }
    }
]

class FederalRegistryAgent:
    """Agent for handling Federal Registry queries"""
    
    def __init__(self):
        self.system_prompt = """
        You are a Federal Registry Assistant, designed to help users find and understand documents 
        from the U.S. Federal Registry. You can search for documents, provide details about specific 
        documents, and offer insights about recent publications, executive orders, and more.
        
        1. Always use the provided tools to look up information. Do not make up information.
        2. If you don't have enough information to answer a query, ask clarifying questions.
        3. When presenting document information, include the title, publication date, and a brief summary.
        4. Be concise and informative in your responses.
        5. If the user asks for something outside your scope, politely explain what you can help with.
        
        Remember, you're a helpful assistant focused on Federal Registry information, which includes
        rules, proposed rules, notices, and presidential documents.
        """
        
        # Initialize the pipeline
        self.pipeline = FederalRegistryPipeline()
        
        # Map tool names to their async functions
        self.tool_map = {
            "search_documents": search_documents,
            "get_document_details": get_document_details,
            "get_recent_documents": get_recent_documents,
            "get_executive_orders": get_executive_orders,
            "search_by_topic": search_by_topic,
            "get_document_count_by_type": get_document_count_by_type,
            "get_document_count_by_agency": get_document_count_by_agency
        }
        
        # Initialize data_initialized flag
        self.data_initialized = False
    
    async def initialize_data(self):
        """Initialize the data pipeline with recent data"""
        if self.data_initialized:
            return
            
        try:
            # Run pipeline for the last 30 days by default
            end_date = datetime.datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
            
            logger.info("Initializing data pipeline...")
            success = await self.pipeline.run_pipeline(start_date, end_date)
            if success:
                logger.info("Data pipeline initialized successfully")
                self.data_initialized = True
            else:
                logger.error("Failed to initialize data pipeline")
        except Exception as e:
            logger.error(f"Error initializing data pipeline: {str(e)}")
    
    async def chat_with_local_model(self, messages: List[Dict[str, str]], tools: List[Dict] = None) -> Dict:
        """Send messages to local LLM and get response"""
        try:
            payload = {
                "model": OLLAMA_MODEL,
                "messages": messages,
                "stream": False
            }
            
            if tools:
                payload["tools"] = tools
            
            async with aiohttp.ClientSession() as session:
                async with session.post(OLLAMA_API_URL, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    else:
                        error_text = await response.text()
                        logger.error(f"Error from LLM API: {response.status} - {error_text}")
                        return {"error": f"API error: {response.status}"}
        except Exception as e:
            logger.error(f"Exception in chat_with_local_model: {str(e)}")
            return {"error": str(e)}
    
    async def execute_tool_call(self, tool_call: Dict) -> Dict:
        """Execute a tool call and return the result"""
        try:
            tool_name = tool_call["function"]["name"]
            arguments = json.loads(tool_call["function"]["arguments"])
            
            if tool_name in self.tool_map:
                tool_func = self.tool_map[tool_name]
                result = await tool_func(**arguments)
                return {
                    "tool_call_id": tool_call["id"],
                    "role": "tool",
                    "name": tool_name,
                    "content": json.dumps(result)
                }
            else:
                logger.error(f"Unknown tool: {tool_name}")
                return {
                    "tool_call_id": tool_call["id"],
                    "role": "tool",
                    "name": tool_name,
                    "content": json.dumps({"error": f"Unknown tool: {tool_name}"})
                }
        except Exception as e:
            logger.error(f"Error executing tool call: {str(e)}")
            return {
                "tool_call_id": tool_call["id"],
                "role": "tool",
                "name": tool_call["function"]["name"],
                "content": json.dumps({"error": str(e)})
            }
    
    async def process_query(self, query: str, history: List[Dict[str, str]] = None) -> Dict:
        """Process a user query and return a response"""
        if not history:
            history = []
        
        # Add system prompt at the beginning if it's not already there
        if not history or history[0].get("role") != "system":
            history = [{"role": "system", "content": self.system_prompt}] + history
        
        # Add the current query to history
        history.append({"role": "user", "content": query})
        
        try:
            # Get response from LLM
            response = await self.chat_with_local_model(history, TOOL_SCHEMA)
            
            if "error" in response:
                return {
                    "answer": f"I encountered an error: {response['error']}",
                    "history": history
                }
            
            # Extract the assistant's message from the response
            assistant_message = response.get("message", {})
            
            if assistant_message:
                # Add assistant's message to history
                history.append(assistant_message)
                
                # Check if the assistant wants to use a tool
                if "tool_calls" in assistant_message:
                    for tool_call in assistant_message["tool_calls"]:
                        function_name = tool_call["function"]["name"]
                        function_args = tool_call["function"]["arguments"]
                        if isinstance(function_args, str):
                            function_args = json.loads(function_args)
                        
                        # Call the appropriate function from tool_map
                        if function_name in self.tool_map:
                            tool_response = await self.tool_map[function_name](**function_args)
                            
                            # Process the tool response
                            if isinstance(tool_response, dict):
                                if "content" in tool_response:
                                    content = tool_response["content"]
                                else:
                                    content = json.dumps(tool_response, indent=2)
                            else:
                                content = str(tool_response) if tool_response else "No results found"
                            
                            # Add the tool response to history
                            history.append({
                                "role": "tool",
                                "name": function_name,
                                "content": content
                            })
                    
                    # Get final answer from LLM after tool execution
                    final_response = await self.chat_with_local_model(history, TOOL_SCHEMA)
                    if "error" not in final_response and "message" in final_response:
                        final_message = final_response["message"]
                        if "content" in final_message:
                            history.append({
                                "role": "assistant",
                                "content": final_message["content"]
                            })
            
            # Extract the final answer
            final_answer = next(
                (msg["content"] for msg in reversed(history)
                 if isinstance(msg, dict) and msg.get("role") == "assistant" and "tool_calls" not in msg),
                "No answer found"
            )
            
            return {
                "answer": final_answer,
                "history": history
            }
            
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            return {
                "answer": f"I encountered an error while processing your query: {str(e)}",
                "history": history
            }

# For testing the agent
async def test_agent():
    """Test the agent with a sample query"""
    agent = FederalRegistryAgent()
    result = await agent.process_query("Show me the 5 most recent executive orders")
    print("Query: Show me the 5 most recent executive orders")
    print("Answer:", result["answer"])

if __name__ == "__main__":
    # Test the agent
    asyncio.run(test_agent())