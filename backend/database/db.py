import sqlite3
from datetime import datetime
import json
from typing import List, Dict, Optional

class Database:
    """SQLite database for search history"""
    
    def __init__(self, db_path: str = "search_history.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize database tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS search_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                search_id TEXT UNIQUE NOT NULL,
                timestamp TEXT NOT NULL,
                bank_statement_file TEXT NOT NULL,
                bridge_file TEXT NOT NULL,
                transaction_ids_file TEXT NOT NULL,
                total_searched INTEGER,
                total_found INTEGER,
                success_count INTEGER,
                failed_count INTEGER,
                not_in_bridge INTEGER,
                not_in_statement INTEGER,
                total_success_amount REAL,
                total_failed_amount REAL,
                processing_time REAL,
                csv_output_path TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transaction_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                search_id TEXT NOT NULL,
                transaction_id TEXT NOT NULL,
                bank_id TEXT,
                date TEXT,
                debit_amount REAL,
                credit_amount REAL,
                status TEXT,
                customer_name TEXT,
                branch TEXT,
                reference_no TEXT,
                description TEXT,
                error_type TEXT,
                FOREIGN KEY (search_id) REFERENCES search_history(search_id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_status (
                task_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                progress INTEGER DEFAULT 0,
                message TEXT,
                result TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def save_search_history(self, search_data: Dict):
        """Save search history"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO search_history (
                search_id, timestamp, bank_statement_file, bridge_file,
                transaction_ids_file, total_searched, total_found,
                success_count, failed_count, not_in_bridge, not_in_statement,
                total_success_amount, total_failed_amount, processing_time,
                csv_output_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            search_data['search_id'],
            search_data['timestamp'],
            search_data['bank_statement_file'],
            search_data['bridge_file'],
            search_data['transaction_ids_file'],
            search_data['total_searched'],
            search_data['total_found'],
            search_data['success_count'],
            search_data['failed_count'],
            search_data['not_in_bridge'],
            search_data['not_in_statement'],
            search_data['total_success_amount'],
            search_data['total_failed_amount'],
            search_data['processing_time'],
            search_data['csv_output_path']
        ))
        
        conn.commit()
        conn.close()
    
    def save_transaction_details(self, search_id: str, transactions: List[Dict]):
        """Save transaction details"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Use executemany for much faster batch inserts
        transaction_records = [(
            search_id,
            txn.get('transaction_id'),
            txn.get('bank_id'),
            txn.get('date'),
            txn.get('debit_amount'),
            txn.get('credit_amount'),
            txn.get('status'),
            txn.get('customer_name'),
            txn.get('branch'),
            txn.get('reference_no'),
            txn.get('description'),
            txn.get('error_type')
        ) for txn in transactions]
        
        cursor.executemany('''
            INSERT INTO transaction_details (
                search_id, transaction_id, bank_id, date,
                debit_amount, credit_amount, status, customer_name,
                branch, reference_no, description, error_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', transaction_records)
        
        conn.commit()
        conn.close()
    
    def get_all_searches(self) -> List[Dict]:
        """Get all search history"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM search_history ORDER BY timestamp DESC')
        rows = cursor.fetchall()
        
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_search_by_id(self, search_id: str) -> Optional[Dict]:
        """Get specific search by ID"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM search_history WHERE search_id = ?', (search_id,))
        row = cursor.fetchone()
        
        conn.close()
        
        return dict(row) if row else None
    
    def get_transactions_by_search_id(self, search_id: str) -> List[Dict]:
        """Get all transactions for a search"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM transaction_details WHERE search_id = ?', (search_id,))
        rows = cursor.fetchall()
        
        conn.close()
        
        return [dict(row) for row in rows]
    
    def search_transaction_by_id(self, transaction_id: str) -> List[Dict]:
        """Search for a transaction across all searches"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT td.*, sh.timestamp, sh.bank_statement_file
            FROM transaction_details td
            JOIN search_history sh ON td.search_id = sh.search_id
            WHERE td.transaction_id = ?
            ORDER BY sh.timestamp DESC
        ''', (transaction_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]

    def create_task(self, task_id: str):
        """Create a new background task record"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        cursor.execute('''
            INSERT INTO task_status (task_id, status, progress, message, created_at, updated_at)
            VALUES (?, 'processing', 0, 'Starting reconciliation...', ?, ?)
        ''', (task_id, now, now))
        conn.commit()
        conn.close()

    def update_task_progress(self, task_id: str, progress: int, message: str):
        """Update task progress and message"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        cursor.execute('''
            UPDATE task_status 
            SET progress = ?, message = ?, updated_at = ?
            WHERE task_id = ?
        ''', (progress, message, now, task_id))
        conn.commit()
        conn.close()

    def complete_task(self, task_id: str, result: Dict):
        """Mark task as completed"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        cursor.execute('''
            UPDATE task_status 
            SET status = 'completed', progress = 100, message = 'Complete', result = ?, updated_at = ?
            WHERE task_id = ?
        ''', (json.dumps(result), now, task_id))
        conn.commit()
        conn.close()

    def fail_task(self, task_id: str, error_message: str):
        """Mark task as failed"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        cursor.execute('''
            UPDATE task_status 
            SET status = 'failed', message = ?, updated_at = ?
            WHERE task_id = ?
        ''', (error_message, now, task_id))
        conn.commit()
        conn.close()

    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """Get the current status of a background task"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM task_status WHERE task_id = ?', (task_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            data = dict(row)
            if data['result']:
                data['result'] = json.loads(data['result'])
            return data
        return None