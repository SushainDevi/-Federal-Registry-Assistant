    # Federal Registry Assistant

A comprehensive system for querying and retrieving information from the U.S. Federal Registry, featuring a modern web interface and real-time chat capabilities.

## Project Overview

The Federal Registry Assistant is a full-stack application that allows users to search, retrieve, and interact with Federal Registry documents through a user-friendly interface. It includes a data pipeline for fetching and processing Federal Registry data, a database for storage, and a web interface for user interaction.

## Features

- Real-time document search and retrieval
- WebSocket-based chat interface
- Executive order tracking
- Document categorization by type and agency
- Topic-based search
- Recent document monitoring
- RESTful API endpoints
- Modern, responsive UI

## Project Structure

```
├── api.py                 # FastAPI application and endpoints
├── agent.py              # Federal Registry Agent implementation
├── config.py             # Configuration settings
├── data/                 # Data storage directory
├── data_fetcher.py       # Data fetching implementation
├── data_processor.py     # Data processing logic
├── db_tools.py           # Database query tools
├── db_uploader.py        # Database upload functionality
├── db_utils.py           # Database utility functions
├── pipeline_config.py    # Pipeline configuration
├── pipeline_orchestrator.py # Pipeline orchestration
├── sql.sql              # Database schema
├── static/              # Frontend static files
│   ├── index.html      # Main UI
│   ├── styles.css      # UI styles
│   └── script.js       # Frontend logic
└── venv/               # Python virtual environment
```

## Components

### 1. Data Pipeline
- **Data Fetcher**: Retrieves Federal Registry documents
- **Data Processor**: Processes and categorizes documents
- **Database Uploader**: Stores processed data in MySQL database

### 2. Backend
- **FastAPI Server**: Handles HTTP and WebSocket requests
- **Federal Registry Agent**: Processes user queries and manages responses
- **Database Tools**: Provides database query functionality

### 3. Frontend
- Modern chat interface
- Real-time updates via WebSocket
- Responsive design

## API Endpoints

- `GET /`: API information
- `POST /query`: Process a query
- `GET /health`: Health check
- `WS /ws/{client_id}`: WebSocket endpoint for real-time chat
- `GET /ui`: Web interface

## Database Schema

The system uses a MySQL database with the following main tables:
- `documents`: Stores Federal Registry documents
- `topics`: Document topics
- `document_topics`: Document-topic relationships

## Setup and Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd federal-registry-assistant
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up the database:
```bash
mysql -u [username] -p < sql.sql
```

5. Configure the application:
- Update `config.py` with your database credentials
- Modify `pipeline_config.py` for pipeline settings

6. Run the application:
```bash
python api.py
```

7. Access the web interface:
```
http://localhost:8000/ui
```

## Usage

1. Open the web interface at `http://localhost:8000/ui`
2. Type your query in the chat interface
3. Examples of queries:
   - "Show me the 5 most recent executive orders"
   - "Search for documents about climate change"
   - "What are the latest regulations from the EPA?"

