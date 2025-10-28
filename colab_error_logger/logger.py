# colab_error_logger/logger.py
import sqlite3
import traceback
from datetime import datetime

try:
    from IPython.core.interactiveshell import InteractiveShell
except ImportError:
    InteractiveShell = None


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
        """Create the errors table if it doesn’t exist."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_type TEXT,
                session_name TEXT,
                error_type TEXT,
                date TEXT
            )
        ''')
        conn.commit()
        conn.close()

    def _install_hook(self):
        """Attach a custom IPython exception handler."""
        if InteractiveShell is not None:
            shell = InteractiveShell.instance()

            def custom_exc(shell, exc_type, exc_value, exc_tb, tb_offset=None):
                tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
                self.log_error(exc_type.__name__, tb_str)
                # Display error normally in notebook
                shell.showtraceback((exc_type, exc_value, exc_tb), tb_offset=tb_offset)

            shell.set_custom_exc((Exception,), custom_exc)

    def log_error(self, error_type: str, tb: str):
        """Insert a new error record into the database."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            INSERT INTO errors ("techie", session_name, error_type, date)
            VALUES (?, ?, ?, ?)
        ''', (self.session_name, error_type, datetime.utcnow().date().isoformat()))
        conn.commit()
        conn.close()


def start_logger(session_name: str, db_path: str = "error_logs.db"):
    """
    Start error logging for the current session.
    """
    return ErrorLogger(session_name, db_path)
