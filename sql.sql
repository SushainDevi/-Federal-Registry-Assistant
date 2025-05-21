-- Create database
CREATE DATABASE federal_registry;
USE federal_registry;

-- Create documents table
CREATE TABLE documents (
    id VARCHAR(255) PRIMARY KEY,
    document_number VARCHAR(100) NOT NULL,
    title TEXT NOT NULL,
    type VARCHAR(100) NOT NULL,
    agency VARCHAR(255),
    publication_date DATE NOT NULL,
    effective_date DATE,
    action VARCHAR(255),
    presidential_document_type VARCHAR(255),
    executive_order_number VARCHAR(50),
    html_url TEXT,
    pdf_url TEXT,
    abstract TEXT,
    full_text LONGTEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_publication_date (publication_date),
    INDEX idx_document_type (type),
    INDEX idx_agency (agency),
    INDEX idx_presidential_document_type (presidential_document_type)
);

-- Create agencies table
CREATE TABLE agencies (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    short_name VARCHAR(100),
    parent_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_parent_id (parent_id)
);

-- Create document_topics table
CREATE TABLE topics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create document_topics table (junction table)
CREATE TABLE document_topics (
    document_id VARCHAR(255),
    topic_id INT,
    PRIMARY KEY (document_id, topic_id),
    FOREIGN KEY (document_id) REFERENCES documents(id) ON DELETE CASCADE,
    FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE
);

-- Create pipeline_logs table to track data updates
CREATE TABLE pipeline_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    run_date DATE NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    records_processed INT DEFAULT 0,
    new_records INT DEFAULT 0,
    updated_records INT DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_run_date (run_date),
    INDEX idx_status (status)
);