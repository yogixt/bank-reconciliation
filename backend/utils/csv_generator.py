import pandas as pd
from datetime import datetime
import os

class CSVGenerator:
    """Generate detailed CSV reports"""
    
    def generate_reconciliation_csv(
        self, 
        results: list,
        not_in_bridge: list,
        not_in_statement: list,
        search_id: str,
        metadata: dict
    ) -> str:
        """Generate detailed reconciliation CSV"""
        
        os.makedirs('outputs', exist_ok=True)
        
        # Combine all data
        all_data = []
        
        # Add successful/failed results
        for r in results:
            all_data.append({
                'Transaction ID': r['transaction_id'],
                'Bank ID': r['bank_id'],
                'Date': r['date'],
                'Debit Amount': r['debit_amount'],
                'Credit Amount': r['credit_amount'],
                'Status': r['status'],
                'Customer Name': r['customer_name'],
                'Branch': r['branch'],
                'Reference No': r['reference_no'],
                'Description': r['description'],
                'Error': ''
            })
        
        # Add not in bridge errors
        for r in not_in_bridge:
            all_data.append({
                'Transaction ID': r['transaction_id'],
                'Bank ID': 'N/A',
                'Date': 'N/A',
                'Debit Amount': 0,
                'Credit Amount': 0,
                'Status': 'ERROR',
                'Customer Name': 'N/A',
                'Branch': 'N/A',
                'Reference No': 'N/A',
                'Description': 'N/A',
                'Error': r['error_type']
            })
        
        # Add not in statement errors
        for r in not_in_statement:
            all_data.append({
                'Transaction ID': r['transaction_id'],
                'Bank ID': r['bank_id'],
                'Date': 'N/A',
                'Debit Amount': 0,
                'Credit Amount': 0,
                'Status': 'ERROR',
                'Customer Name': 'N/A',
                'Branch': 'N/A',
                'Reference No': 'N/A',
                'Description': 'N/A',
                'Error': r['error_type']
            })
        
        # Create DataFrame
        df = pd.DataFrame(all_data)
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"outputs/reconciliation_{search_id}_{timestamp}.csv"
        
        # Save CSV
        df.to_csv(filename, index=False)
        
        print(f"  ✓ CSV saved: {filename}")
        
        return filename