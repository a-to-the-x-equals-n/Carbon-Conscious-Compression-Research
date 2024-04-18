import psycopg2
from psycopg2 import sql
import util
from huffman import HuffmanCoding

# Connection parameters
db = "huffman"
comp_db = "compressed_huffman"
host = "localhost"  
user, password = util.load_vars("OWNER", "SQLPASS")


# Establish the connection
conn = psycopg2.connect(dbname=db, user=user, password=password, host=host)


def fetch_data():
    """Fetch data from the original database."""
    
    cur = conn.cursor()
    cur.execute("SELECT id, title, url, content, author, date, postexcerpt FROM LLM_data")
    articles = cur.fetchall()
    cur.close()
    conn.close()
    return articles

def insert_compressed_data(articles):
    """Compress and insert data into the new compressed_huffman database."""
    
    cur = conn.cursor()
    
    for article in articles:
        # Compress data
        # TODO: integrate compression 
        compressed_title = (article[0])
        compressed_url = (article[1])
        compressed_content = (article[2])
        compressed_author = (article[3])
        compressed_date = (article[4])
        compressed_postexcerpt = (article[5])
        
        # Insert compressed data
        cur.execute(
            "INSERT INTO compressed_articles (title, url, content, author, date, postexcerpt) VALUES (%s, %s, %s, %s, %s, %s)",
            (compressed_title, compressed_url, compressed_content, compressed_author, compressed_date, compressed_postexcerpt)
        )
    
    conn.commit()
    cur.close()
    conn.close()
