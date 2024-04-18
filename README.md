# Carbon-Conscious-Compression-Research

## Quick Start Guide
1. Clone the repo
    ```bash
    git clone https://github.com/a-to-the-x-equals-n/Carbon-Conscious-Compression-Research.git
    ```
2. The repo includes a sample file in the `datasets` folder: `alice.txt` (*the Alice in Wonderland story*).
3. `huffman.py` has a **main guard** so you can test the class directly.

    ```python
    # Main Idiom
    if __name__ == "__main__":
            
        path = "datasets/alice.txt"

        h = HuffmanCoding(path)

        output_path = h.compress()
        print("Compressed file path: " + output_path)

        decom_path = h.decompress(output_path)
        print("Decompressed file path: " + decom_path)
    ```
4. Navigate to the directory of your new repository, then in terminal enter:
    ```bash
    python3 huffman.py
    ```

5. The above command will demonstrate a compression and decompression on the `alice.txt` file.
6. After execution, the program will create an `<filename>.bin` file and `<filename>_decompressed.txt` file in the `datasets` folder.
7. You can edit the `path` variable in the **main guard** to sample other files:
    ```python
    path = "datasets/alice.txt"
    ```

## Contributors
### Source Code
Bhrigu Srivastava

https://github.com/bhrigu123/huffman-coding

https://youtu.be/JCOph23TQTY

https://bhrigu.me/blog/2017/01/17/huffman-coding-python-implementation/
