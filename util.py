import os
from dotenv import load_dotenv
from pathlib import Path


def load_vars(*args):
    # Load the Environment variables
    try:
        # Determine the current directory of the script and locate the .env file
        envars = current_dir() / ".env"

        # Load the environment variables from the .env file
        load_dotenv(envars)

    # If any exception occurs during the process, raise an EnvFileError
    except Exception as e:
        return print(f'{e}')
    
    # Return a list of values for the given environment variable names
    return [os.getenv(arg) for arg in args]


def absolute_path():
    return Path(os.path.dirname(os.path.abspath(__file__)))

def current_dir():
    return Path(__file__).resolve().parent if "__file__" in locals() else Path.cwd()





if __name__ == "__main__":
    # Small scale testing
    pass