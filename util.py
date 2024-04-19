import os
from dotenv import load_dotenv
from pathlib import Path


def load_vars(*args):
    """
    Load specified environment variables from a .env file located in the current directory.

    Args:
        *args (str): Variable number of string arguments that represent the names of the environment
                     variables to be loaded.

    Returns:
        list: A list containing the values of the environment variables specified in *args.
              Each value corresponds to the environment variable name provided in the same order.
    """
    try:
        # Determine the current directory of the script and locate the .env file
        envars = current_dir() / ".env"

        # Load the environment variables from the .env file
        load_dotenv(envars)

    except Exception as e:
        # If an error occurs during loading, print the exception and return None to indicate failure
        print(f"Error loading .env file: {e}")
        return None
    
    # Retrieve and return the values of the environment variables requested
    return [os.getenv(arg) for arg in args]



def absolute_path():
    """
    Get the absolute path of the directory where the current script is located.

    Returns:
        Path: A Path object representing the absolute path of the directory containing the current script.
    """
    return Path(os.path.dirname(os.path.abspath(__file__)))



def current_dir():
    """
    Determine the current directory where the script is running. This is particularly useful for cases
    where __file__ might not be defined, such as within an interactive Python shell or notebook.

    Returns:
        Path: A Path object representing the current directory. If __file__ is defined, it returns
              the directory containing the script, otherwise, it returns the current working directory.
    """
    return Path(__file__).resolve().parent if "__file__" in locals() else Path.cwd()





if __name__ == "__main__":
    pass