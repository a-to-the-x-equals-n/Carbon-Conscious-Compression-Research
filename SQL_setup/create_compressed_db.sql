-- This command is to connect to the default database to create a new database
\connect postgres

-- Create a new database named 'compressed_huffman'
CREATE DATABASE compressed_huffman;

-- Connect to the new database (you would run this line in your SQL client or script)
\connect compressed_huffman

-- Create a table to store compressed data
CREATE TABLE compressed_articles (
    title BYTEA,
    url BYTEA,
    content BYTEA,
    author BYTEA,
    date BYTEA,  
    postexcerpt BYTEA
);
