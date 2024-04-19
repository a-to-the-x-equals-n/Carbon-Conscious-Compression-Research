from huffman import HuffmanCoding
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import LargeBinary, Text
from pathlib import Path
import os
import gc
import json
import subprocess
import util



class HuffmanSql(HuffmanCoding):
    """
    A class derived from HuffmanCoding that adds functionality to interact with SQL databases.
    This class can be used to store and retrieve Huffman-encoded data from a database.
    """
    def __init__(self, path, db_URL, huff_URL):
        """
        Initialize the Huffsql class by initializing the superclass HuffmanCoding with a path,
        and setting up database engines for operations.
        """
        super().__init__(path) # Initialize the base class with the path
        self.engine = create_engine(db_URL)  # Engine for the database where raw data might be stored
        self.huff_engine = create_engine(huff_URL)  # Engine for the database to store compressed data
        self.table = ''
        self.huff_table = ''



    def __del__(self):
        """
        Explicitly handle the cleanup of resources by dropping tables from their respective databases.
        """
        try:
            # Drop the main table from the main database
            with self.engine.connect() as conn:
                conn.execute(f"DROP TABLE IF EXISTS {self.table}")
            print(f"Table {self.table} has been removed.")

        except Exception:
            print(f"No table named {self.table} currently exists.")

        try:
            # Drop the Huffman compressed table from the Huffman database
            with self.huff_engine.connect() as conn:
                conn.execute(f"DROP TABLE IF EXISTS {self.huff_table}")
            print(f"Table {self.huff_table} has been removed.")

        except Exception:
            print(f"No table named {self.huff_table} currently exists.")
    



    ''' Creating SQL tables '''


    def csv_to_table(self, chunk = 5000):
        """
        Reads a CSV file in chunks if it's large and uploads each chunk to the specified SQL table.

        Args:
            chunk (int): Number of rows per chunk.
        """
        # Calculate the file size in megabytes
        file_size_mb = os.path.getsize(self.path) / (1024 * 1024)

        # Get table name
        self.table = Path(self.path).stem # The stem property returns the file name without the suffix
        
        try:
            # Determine if the file should be read in chunks
            if file_size_mb > 20:
                print(f"File size is {file_size_mb:.2f} MB, reading and uploading in chunks.")

                for chunk in pd.read_csv(self.path, chunksize = chunk):
                    chunk.to_sql(name=self.table, con=self.engine, if_exists='append', index=False)

                    print("Uploaded a chunk to the database.")
            else:
                print(f"File size is {file_size_mb:.2f} MB, reading the entire file and uploading.")

                data = pd.read_csv(self.path)
                data.to_sql(name=self.table, con=self.engine, if_exists='replace', index=False)

                print("Uploaded the entire data to the database.")

        except SQLAlchemyError as e:
            print(f"An error occurred: {e}")



    def compressed_to_table(self, compressed_data):
        """
        Uploads compressed data to a SQL database table.

        Args:
            compressed_data (pd.DataFrame): The DataFrame containing compressed data and metadata.

        Raises:
            SQLAlchemyError: If there is an issue with database connection or SQL execution.
        """
        # Define the table name where the compressed data will be stored.
        self.huff_table = f'{self.table}_compressed'
        
        # Define column data types dynamically based on DataFrame structure
        # Assuming all columns except the last two are byte arrays
        dtype_dict = {col: LargeBinary for col in compressed_data.columns[:-2]}

        # Add the data types for the last two columns which are metadata JSON strings
        dtype_dict.update({
            compressed_data.columns[-2]: Text,  # 'codes' column
            compressed_data.columns[-1]: Text   # 'reverse_mapping' column
        })

        try:
            # Upload the DataFrame to the SQL database, replacing any existing data in the table.
            compressed_data.to_sql(
                name=self.huff_table,
                con=self.huff_engine,
                if_exists='replace',
                index=False,
                dtype=dtype_dict  # specify column data types here
            )
            print("Uploaded the entire data to the database.")

        except SQLAlchemyError as e:
            # Handle potential errors during the upload process and print an error message.
            print(f"An error occurred: {e}")




    ''' Fetching SQL tables '''


    def fetch_all(self):
        """ Fetches all data from the specified table and returns it as a DataFrame. """

        query = f"SELECT * FROM {self.table}"

        try:
            data = pd.read_sql(query, self.engine)
            print("Data fetched successfully.")
            return data
        
        except SQLAlchemyError as e:
            print(f"An error occurred while fetching data: {e}")


    def fetch_all_compressed(self):
        """ Fetches all data from the specified table and returns it as a DataFrame. """

        query = f"SELECT * FROM {self.huff_table}"

        try:
            data = pd.read_sql(query, self.huff_engine)
            print("Data fetched successfully.")
            return data
        
        except SQLAlchemyError as e:
            print(f"An error occurred while fetching data: {e}")

        


    ''' Huffman compression on SQL tables '''


    def compress(self, data):
        """
        Compresses each column of the DataFrame, stores the compressed data back into the DataFrame,
        and saves the Huffman encoding metadata as JSON.

        Args:
            data (pd.DataFrame): The DataFrame containing the data to be compressed.
        """
        # Initialize columns for storing JSON metadata about Huffman codes and reverse mappings.
        # This is necessary to decompress the data later.
        data['codes'] = pd.Series(dtype=object)
        data['reverse_mapping'] = pd.Series(dtype=object)

        # Create a temporary DataFrame to store binary data
        binary_data = pd.DataFrame(index=data.index)

        # Iterate over columns to compress data, skipping the last two which are reserved for metadata.
        for column in data.columns[:-2]:  # Ensure metadata columns are not included in the compression process.

            # Compress each column using Huffman encoding and replace the original data.
            compressed_bytes = self.compress_helper(data[column])

            # Store compressed data into the binary_data DataFrame
            binary_data[column] = compressed_bytes.apply(lambda x: x if isinstance(x, bytes) else None)

            # Serialize and store Huffman codes and reverse mappings for each column.
            data.at[0, 'codes'] = json.dumps({f"{column}_codes": self.codes})
            data.at[0, 'reverse_mapping'] = json.dumps({f"{column}_reverse_mapping": self.reverse_mapping})

            # Clear Huffman tree and mappings after processing each column to prevent data leaking between columns.
            self.reset_attrs()

        # Replace original DataFrame columns with binary data
        data.update(binary_data)

        # Return resources from this larger df
        del binary_data
        gc.collect() # Force garbage collector

        return data



    def compress_helper(self, column):
        """
        Compress a single column of data using Huffman encoding and return the compressed data
        along with the necessary Huffman encoding metadata (codes and reverse mappings).

        Args:
            column (pd.Series): A pandas Series representing a single column of data to be compressed.

        Returns:
            tuple: A tuple containing the compressed data (bytes), Huffman codes, and reverse mappings.
        """

        # Convert the entire column to a single string to facilitate Huffman compression.
        text = ''.join(column.astype(str)).rstrip() # Strip any trailing whitespace characters from the end of the text.
        frequency = self.make_frequency_dict(text) # Build the frequency dictionary from the text.

        self.make_heap(frequency) # Create a priority queue (min-heap) from the frequency dictionary.
        self.merge_nodes() # Merge nodes in the heap until only one remains; this is the root of the Huffman tree.
        self.make_codes() # Generate Huffman codes from the Huffman tree.
        
        encoded_text = self.get_encoded_text(text) # Encode the text using the generated Huffman codes.
        padded_encoded_text = self.pad_encoded_text(encoded_text) # Pad the encoded text to ensure it's a multiple of 8 bits, suitable for byte conversion.
        
        b = self.get_byte_array(padded_encoded_text) # Convert the padded, encoded text into a byte array suitable for storage or transmission.
        
        return b



    def reset_attrs(self):
        """Reset the Huffman tree and mappings."""
        self.heap = []
        self.codes = {}
        self.reverse_mapping = {}
    



    ''' Huffman decompression on SQL tables '''
        

    def decompress(self, compressed_data):
        """
        Decompresses all data in the DataFrame using stored Huffman encoding metadata.

        Args:
            compressed_df (pd.DataFrame): DataFrame containing the compressed data and metadata.

        Returns:
            pd.DataFrame: DataFrame with decompressed data.
        """
        # Initialize a DataFrame to store decompressed data
        decompressed_data = pd.DataFrame(index=compressed_data.index)

        # Iterate over each column in the compressed DataFrame (skip metadata columns)
        for column in compressed_data.columns[:-2]:  # Assuming the last two columns are 'codes' and 'reverse_mapping'
            decompressed_data[column] = compressed_data[column].apply(
                lambda x: self.decompress_helper(x, json.loads(compressed_data.at[0, 'reverse_mapping'][f'{column}_reverse_mapping']))
            )

        return decompressed_data



    def decompress_helper(self, compressed_col, reverse_map):
        """
        Decompresses binary data for a single column using the reverse mapping metadata.

        Args:
            compressed_col (bytes): The compressed column as a byte array.
            reverse_map (dict): Huffman reverse mappings used for decoding.

        Returns:
            str: The decompressed text.
        """
        bit_string = ''
        # Convert bytes to bit string
        for byte in compressed_col:
            bits = bin(byte)[2:].rjust(8, '0')  # Convert byte to a binary string, padding to 8 bits
            bit_string += bits

        # Remove padding from the bit string to get the pure encoded text
        encoded_text = self.remove_padding(bit_string)

        # Set up Huffman tree or use reverse_mapping to decode the text
        self.reverse_mapping = reverse_map  # Assuming the parent class can use this directly
        decompressed_text = self.decode_text(encoded_text)

        return decompressed_text




    ''' Export database tables '''


    def export_as_sql(self, db, table, user,  host = 'localhost', port = 5432):
        """
        Exports a specific table to an SQL file using pg_dump.

        Args:
            table_name (str): The name of the table to export.
            output_path (str): The file path where the SQL dump should be saved.
        """
        output_path = f'datasets/{table}.sql'

        # Command to run pg_dump for a specific table
        command = [
            "pg_dump",
            "--table", table,
            "--file", output_path,
            "--username", user,
            "--host", host,
            "--port", port,
            db
        ]

        # Run the command using subprocess
        try:
            subprocess.run(command, check=True)
            print(f"Table {table} has been successfully exported to {output_path}.")

        except subprocess.CalledProcessError as e:
            print(f"An error occurred while exporting the table: {e}")



    def export_as_csv(self, table, engine, output_path):
        """
        Exports a table from the SQL database to a CSV file.

        Args:
            table_name (str): The name of the table to export.
            output_path (str): The file path where the CSV should be saved.
            engine (URL): the database you wish to connect to.
        """
        # Connect to the database and download the table content into a DataFrame
        with engine.connect() as connection:
            df = pd.read_sql_table(table, con=connection)

        # Save the DataFrame to a CSV file
        df.to_csv(output_path, index=False)
        print(f"Table {table} has been successfully exported to {output_path}.")



if __name__ == "__main__":
    """
    This block is the starting point of the script when it's run as the main program.
    It initializes the HuffmanSql compression process for a specified file.
    It exports the inital csv file as an sql file, then compresses the data.
    The compressed data is then exported as an sql file.

    With the compressed and non compressed data in sql file format, overall size can be compared.
    """
    # Define the path to the text file that will be compressed.
    path = "datasets/LLM_data.csv"

    # Load environment variables
    user, pwd = util.load_vars("OWNER", "SQLPASS")
    db = "huffman"
    huff_db = "compressed_huffman"
    host = 'localhost'
    port = 5432

    url = f'postgresql://{user}:{pwd}@{host}/{db}'
    huff_url = f'postgresql://{user}:{pwd}@{host}/{huff_db}'
    
    # Create an instance of HuffmanSql
    h = HuffmanSql(path, url, huff_url)

    h.csv_to_table() # Load source file and store as db table
    h.export_as_sql(db, h.table, user, host, port) # Export table as SQL file
    data = h.fetch_all() # Fetch new db table
    binary_data = h.compress(data) # Compress new table
    h.compressed_to_table(binary_data) # Store compressed table in db
    h.export_as_sql(huff_db, h.huff_table, user, host, port) # Export compressed table as an sql file

    # clean up
    del h
    gc.collect()
