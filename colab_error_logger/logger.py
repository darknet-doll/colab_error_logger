# colab_error_logger/logger.py
import sqlite3
import sys
import traceback
from datetime import datetime

class ErrorLogger:
    def __init__(self, session_name: str, db_path: str = "error_logs.db"):
        """
        Initialize the error logger.

        Args:
            session_name (str): Name of the session (manual).
            db_path (str): SQLite database file path.
        """
        self.session_name = session_name
        self.db_path = db_path
        self._setup_db()
        self._install_hook()

    def _setup_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_name TEXT,
                error_type TEXT,
                timestamp TEXT,
                traceback TEXT
            )
        ''')
        conn.commit()
        conn.close()

    def _install_hook(self):
        def custom_excepthook(exc_type, exc_value, exc_tb):
            tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            self.log_error(exc_type.__name__, tb_str)
            sys.__excepthook__(exc_type, exc_value, exc_tb)
        
        sys.excepthook = custom_excepthook

    def log_error(self, error_type: str, tb: str):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            INSERT INTO errors (session_name, error_type, timestamp, traceback)
            VALUES (?, ?, ?, ?)
        ''', (self.session_name, error_type, datetime.utcnow().isoformat(), tb))
        conn.commit()
        conn.close()


def start_logger(session_name: str, db_path: str = "error_logs.db"):
    """
    Start error logging for the current session.
    """
    return ErrorLogger(session_name, db_path)
