import mysql.connector
import os
from dotenv import load_dotenv

# Load environment variables from .env file in the project root
# Assumes .env file is in the parent directory of 'src'
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

# Database configuration fetched from environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", 3306)
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def connect():
    """
    Establishes a connection to the MySQL database using credentials
    from environment variables.

    Returns:
        mysql.connector.connection.MySQLConnection: The database connection object,
                                                    or None if connection fails.
    """
    conn = None
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        print("Error: Database configuration is incomplete in environment variables.")
        return None
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=int(DB_PORT),
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        print("Database connection successful.")
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return None


def close(conn):
    """
    Closes the database connection if it's open.

    Args:
        conn (mysql.connector.connection.MySQLConnection): The connection object to close.
    """
    if conn and conn.is_connected():
        conn.close()
        print("Database connection closed.")
