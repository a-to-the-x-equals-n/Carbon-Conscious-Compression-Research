import heapq
import os



class HuffmanCoding:
    """
    A class that implements Huffman Coding for file compression and decompression.
    It handles building the Huffman tree, encoding, and decoding processes.
    """
    def __init__(self, path):
        """
        Initialize the HuffmanCoding class with a path for the file to be compressed or decompressed.
        """
        self.path = path
        self.heap = []  # Min-heap for building the Huffman tree.
        self.codes = {}  # Dictionary to store the character-to-code mapping.
        self.reverse_mapping = {}  # Dictionary to store the code-to-character mapping for decoding.


    class HeapNode:
        """
        A nested class within the HuffmanCoding class that represents a node in the Huffman tree.
        
        The Huffman tree is built using these nodes, each representing a character and its frequency in the text.
        The nodes are arranged in the tree based on their frequency, with less frequent characters
        generally placed deeper in the tree, which results in longer codes for less common characters.
        """
        def __init__(self, char, freq):
            """
            Initialize a new node with a character and its frequency.

            Args:
                char (str): The character or None (for internal nodes that are not leaves).
                freq (int): The frequency of occurrence of the character.
            """
            self.char = char  # The character of the node.
            self.freq = freq  # Frequency of the character.
            self.left = None  # Left child in the Huffman tree.
            self.right = None  # Right child in the Huffman tree.


        def __lt__(self, other):
            """
            Comparator method to prioritize nodes with lower frequencies. This is used by the heapq
            to maintain the heap property.

            Args:
                other (HeapNode): Another node to compare against.

            Returns:
                bool: True if this node's frequency is less than the other node's frequency.
            """
            return self.freq < other.freq


        def __eq__(self, other):
            """
            Check equality of this node with another node, based on frequency and type.

            This method is useful for heap operations and ensures that the heap can correctly
            identify nodes with the same frequency.

            Args:
                other (HeapNode): Another node to compare against.

            Returns:
                bool: True if both nodes are of the same type and have the same frequency.
            """
            if other is None:
                return False
            if not isinstance(other, type(self)):
                return False
            return self.freq == other.freq



    def make_frequency_dict(self, text):
        """
        Create a frequency dictionary mapping each character to its frequency of appearance in the text.
        
        This dictionary is essential for building the Huffman tree, where the frequency of each
        character is used to determine its position in the tree, with less frequent characters
        generally being deeper in the tree and having longer codes.

        Args:
            text (str): The input text from which to calculate character frequencies.

        Returns:
            dict: A dictionary where keys are characters and values are their corresponding frequencies.
        """
        frequency = {} # Initialize an empty dictionary to hold character frequencies.
        for character in text:

            if character not in frequency:
                frequency[character] = 0 # Initialize the frequency count for new characters.
            frequency[character] += 1 # Increment the frequency count for each character occurrence.

        return frequency # Return the complete frequency dictionary.


    def make_heap(self, frequency):
        """
        Build a min-heap from the frequency dictionary using the HeapNode. This heap is used to construct
        the Huffman tree, which is central to the Huffman encoding and decoding process.

        A min-heap is a complete binary tree where the node with the smallest value is always at the root.
        This property is crucial for the optimal property of Huffman coding where the least frequent characters
        should have the longest codes and be deepest in the tree.

        Args:
            frequency (dict): A dictionary mapping each character to its frequency of occurrence.
        """
        for key in frequency:
            # Create a HeapNode for each character with its frequency from the dictionary.
            node = self.HeapNode(key, frequency[key])

            # Insert the newly created node into the heap. 
            # The heap structure will automatically be maintained by the heapq library.
            # This ensures the node with the lowest frequency is always at the front.
            heapq.heappush(self.heap, node)


    # Merge nodes until one node remains; this node will be the root of the Huffman tree.
    def merge_nodes(self):
        """
        Merge nodes to construct the Huffman tree. This function repeatedly removes the two nodes
        with the lowest frequency from the heap, merges them into a new node, and inserts the new node
        back into the heap. This process continues until only one node remains in the heap, which will
        serve as the root of the Huffman tree.

        The root of the Huffman tree created by this method will have branches that represent the
        paths to each character, with the path lengths inversely proportional to the character frequencies.
        """
        while len(self.heap) > 1:
            # Remove the two nodes with the smallest frequencies from the heap.
            node1 = heapq.heappop(self.heap) # Node with the smallest frequency.
            node2 = heapq.heappop(self.heap) # Node with the second smallest frequency.

            # Create a new merged node with no character (internal node), where the frequency is the sum of the two smallest frequencies.
            # This new node will be the parent of the two nodes popped from the heap.
            merged = self.HeapNode(None, node1.freq + node2.freq)
            merged.left = node1  # Assign the first extracted node as the left child.
            merged.right = node2  # Assign the second extracted node as the right child.

            # Push the merged node back into the heap. 
            # The heap property will automatically be maintained, ensuring that the node with the next lowest frequency is always at the front.
            heapq.heappush(self.heap, merged)


    def make_codes_helper(self, root, current_code):
        """
        Recursively traverse the Huffman tree to generate codes for each character. 
        This function assigns a binary code to each character by traversing from the root to the leaf nodes.
        '0' is added to the code when moving to the left child and '1' when moving to the right child.

        Args:
            root (HeapNode): The current node in the Huffman tree.
            current_code (str): The current Huffman code being constructed as we traverse the tree.
        """
        if root is None:
            return  # Base case: If the node is None, return as there is nothing to process.
        
        if root.char is not None:
            # If the node is a leaf node (contains a character), store the code in the dictionary.
            self.codes[root.char] = current_code
            self.reverse_mapping[current_code] = root.char

        # Recursively generate codes for the left and right children.  
        self.make_codes_helper(root.left, current_code + "0") # Append '0' for the left child.
        self.make_codes_helper(root.right, current_code + "1") # Append '1' for the right child.


    def make_codes(self):
        """
        Start the Huffman coding process by extracting the root of the tree from the heap and
        initiating the recursive traversal to generate codes.

        This function begins the code generation process that will populate the `codes` and
        `reverse_mapping` dictionaries with the Huffman codes for each character.
        """
        root = heapq.heappop(self.heap) # Remove the root of the Huffman tree from the heap.
        self.make_codes_helper(root, "") # Start the recursive code generation process with an empty code string.



    def get_encoded_text(self, text):
        """
        Converts the input text into its encoded form using the generated Huffman codes.
        This function takes the input text and transforms it into a string of binary codes
        according to the Huffman encoding scheme previously established.

        Args:
            text (str): The input text to be encoded using Huffman codes.

        Returns:
            str: A string containing the binary Huffman codes corresponding to each character
                in the input text.
        """
        encoded_text = "" # Initialize an empty string to hold the encoded text.
        for character in text:

            # Append the Huffman code for each character in the input text to the encoded_text string.
            # This mapping must have been previously created in the make_codes function.
            encoded_text += self.codes[character]

        return encoded_text # Return the fully encoded text as a single string of binary digits.


    def pad_encoded_text(self, encoded_text):
        """
        Adds padding to the encoded text to ensure its length is a multiple of 8 bits.
        This step is crucial for the encoded text to be properly handled as bytes,
        as byte-oriented systems process data in byte (8 bits) increments.

        Args:
            encoded_text (str): The Huffman-encoded string consisting of a sequence of binary codes.

        Returns:
            str: The padded encoded string where the length is a multiple of 8,
                with additional information appended at the beginning to indicate
                the amount of padding added.
        """
        # Calculate the required padding to make the encoded text length a multiple of 8.
        extra_padding = 8 - len(encoded_text) % 8

        # Append zeros to the end of the encoded text to add the necessary padding.
        for i in range(extra_padding):
            encoded_text += "0"

        # Prepare a binary string that represents the padding length, padded to 8 bits.
        # This string is used to know how many zeros were added when decoding the text.
        padded_info = "{0:08b}".format(extra_padding)

        # Prepend the padding information to the start of the encoded text.
        # This is important for the decoding process, so it knows how many bits to ignore at the end.
        encoded_text = padded_info + encoded_text

        return encoded_text  # Return the final padded encoded text.


    def get_byte_array(self, padded_encoded_text):
        """
        Converts padded encoded text into a byte array. This array is suitable for storage or transmission
        as it transforms the string of bits into a series of bytes, which are a standard way of handling
        data in most systems.

        Args:
            padded_encoded_text (str): The Huffman encoded text with padding to ensure its length
                                    is a multiple of 8 bits.

        Returns:
            bytearray: A bytearray which represents the encoded data as bytes, suitable for writing
                    to a binary file or transmitting over a network.
        """
        # Ensure that the length of the padded encoded text is indeed a multiple of 8.
        # If not, there is an error in the padding process.
        if len(padded_encoded_text) % 8 != 0:
            print("Encoded text not padded properly")
            exit(0)  # Exit the program indicating an error condition.

        b = bytearray()  # Initialize an empty bytearray to store the bytes.
        # Iterate over the padded encoded text 8 bits (1 byte) at a time.
        for i in range(0, len(padded_encoded_text), 8):
            # Extract a slice of 8 bits.
            byte = padded_encoded_text[i:i+8]
            # Convert the string of bits into an integer and then to a byte, and append to the bytearray.
            b.append(int(byte, 2))

        return b  # Return the complete bytearray containing the encoded data.



    def compress(self):
        """
        Compress the text file specified by the 'path' attribute using Huffman coding, 
        and write the compressed data to a binary file. This function performs several steps:
        reading the file, building the Huffman tree, encoding the text, padding the encoded text,
        and finally writing the compressed data to a binary file.

        Returns:
            str: The path to the compressed binary file.
        """
        # Split the file path into a base filename and extension.
        filename, file_extension = os.path.splitext(self.path)
        # Define the output path for the compressed file.
        output_path = filename + ".bin"

        # Open the original text file and a new binary file for writing the compressed data.
        with open(self.path, 'r+') as file, open(output_path, 'wb') as output:
            # Read the entire content of the file.
            text = file.read()
            # Strip any trailing whitespace characters from the end of the text.
            text = text.rstrip()

            # Generate the frequency dictionary for the characters in the text.
            frequency = self.make_frequency_dict(text)
            # Create a min-heap from the frequency dictionary using the Huffman tree nodes.
            self.make_heap(frequency)
            # Merge nodes until one node remains; this node will be the root of the Huffman tree.
            self.merge_nodes()
            # Generate Huffman codes based on the structure of the Huffman tree.
            self.make_codes()

            # Encode the original text using the generated Huffman codes.
            encoded_text = self.get_encoded_text(text)
            # Add padding to the encoded text to ensure its length is a multiple of 8 bits.
            padded_encoded_text = self.pad_encoded_text(encoded_text)

            # Convert the padded encoded text into a byte array.
            b = self.get_byte_array(padded_encoded_text)
            # Write the byte array to the binary output file.
            output.write(bytes(b))

        # Output a message to the console indicating that the file has been compressed.
        print("Compressed")
        # Return the path to the compressed file.
        return output_path



    ''' Decompression methods below: '''



    def remove_padding(self, padded_encoded_text):
        """
        Remove padding from the encoded text to restore the pure encoded form that represents
        the actual data. This function extracts the padding information and uses it to determine
        how much of the end of the encoded text to discard, ensuring the decoded text is exactly
        what was originally encoded without any additional padding bits.

        Args:
            padded_encoded_text (str): The encoded text with padding at the end to make its
                                    length a multiple of 8 bits.

        Returns:
            str: The encoded text with the padding removed, ready for decoding.
        """
        # Extract the first 8 bits which contain the information about the amount of padding added.
        padded_info = padded_encoded_text[:8]
        # Convert the binary string to an integer to get the number of padding bits.
        extra_padding = int(padded_info, 2)

        # Remove the padding information from the start of the text.
        padded_encoded_text = padded_encoded_text[8:]
        # Remove the padding bits from the end of the encoded text using the padding amount.
        encoded_text = padded_encoded_text[:-1 * extra_padding]

        return encoded_text  # Return the encoded text with padding removed.



    def decode_text(self, encoded_text):
        """
        Decode the Huffman-encoded text back into the original text using the reverse mapping
        of Huffman codes to characters. This function iterates through each bit in the encoded text,
        building Huffman codes incrementally and translating them back to characters whenever a complete
        code is formed.

        Args:
            encoded_text (str): The Huffman-encoded text, a string of binary digits ('0's and '1's).

        Returns:
            str: The original text obtained by decoding the Huffman-encoded text.
        """
        current_code = ""  # Initialize an empty string to build Huffman codes.
        decoded_text = ""  # Initialize an empty string to accumulate the decoded characters.

        # Iterate through each bit in the encoded text.
        for bit in encoded_text:
            current_code += bit  # Add the bit to the current Huffman code being built.

            # Check if the current code is a valid Huffman code according to the reverse mapping.
            if current_code in self.reverse_mapping:
                # Retrieve the corresponding character from the reverse mapping dictionary.
                character = self.reverse_mapping[current_code]
                decoded_text += character  # Append the character to the decoded text.
                current_code = ""  # Reset the current code to start building a new Huffman code.

        return decoded_text  # Return the fully decoded text.



    def decompress(self, input_path):
        """
        Decompresses the binary file specified by the input path using Huffman coding, 
        converting it back into its original text format and writing the result to a new text file.
        
        Args:
            input_path (str): The path to the binary file that contains Huffman encoded data.
        
        Returns:
            str: The path to the decompressed text file.
        """
        # Extract the base filename and file extension from the original path to form the output filename.
        filename, file_extension = os.path.splitext(self.path)
        # Define the path for the decompressed output file.
        output_path = filename + "_decompressed" + ".txt"

        # Open the encoded binary file for reading and the output file for writing.
        with open(input_path, 'rb') as file, open(output_path, 'w') as output:
            bit_string = ""  # Initialize an empty string to hold all the bits read from the file.

            # Read the first byte from the file.
            byte = file.read(1)
            while len(byte) > 0:
                # Convert the byte to an integer and then to a binary string, padding it to 8 bits.
                byte = ord(byte)
                bits = bin(byte)[2:].rjust(8, '0')  # Ensure each byte string has exactly 8 bits.
                bit_string += bits  # Append the bits to the bit string.
                byte = file.read(1)  # Read the next byte.

            # Remove padding from the bit string to get the pure encoded text.
            encoded_text = self.remove_padding(bit_string)
            # Decode the encoded text using the Huffman coding reverse mappings.
            decompressed_text = self.decode_text(encoded_text)

            # Write the decompressed text to the output file.
            output.write(decompressed_text)

        # Inform the user that the file has been decompressed.
        print("Decompressed")
        return output_path  # Return the path to the decompressed text file.




if __name__ == "__main__":
    """
    This block is the starting point of the script when it's run as the main program.
    It initializes the Huffman coding process for a specified file, compresses and then decompresses it,
    and prints the paths to the compressed and decompressed files.
    """
    
    # Define the path to the text file that will be compressed.
    path = "alice.txt"

    # Create an instance of HuffmanCoding with the specified file path.
    h = HuffmanCoding(path)

    # Compress the file specified by the path and store the path to the compressed file.
    output_path = h.compress()
    # Output the path to the compressed file.
    print("Compressed file path: " + output_path)

    # Decompress the previously compressed file and store the path to the decompressed file.
    decom_path = h.decompress(output_path)
    # Output the path to the decompressed file.
    print("Decompressed file path: " + decom_path)

