
-- psql -U username -d postgres -f create_huffman_db.sql

-- This command is to connect to the default database to create a new database
\connect postgres

-- Create the database 'huffman'
CREATE DATABASE huffman;

-- Connect to the newly created database
\connect huffman

-- Create table 'LLM_data'
CREATE TABLE LLM_data (
    title VARCHAR(255),         -- Title of the article
    url VARCHAR(255),           -- URL to the article
    content TEXT,               -- Content of the article, unlimited size
    author VARCHAR(255),        -- Author of the article
    date DATE,                  -- Publication date of the article
    postexcerpt TEXT            -- Excerpt from the article
);