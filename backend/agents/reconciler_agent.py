import pandas as pd
from typing import List, Dict, Tuple
import time

class ReconcilerAgent:
    """
    Deterministic reconciliation agent
    Handles Q1: Returns ALL matches (Option A)
    """
    
    def reconcile(
        self, 
        bank_df: pd.DataFrame,
        bridge_map: Dict[str, str],
        txn_search_list: List[str]
    ) -> Dict:
        """
        Reconcile transactions
        Returns ALL matches for each bank_id (Q1: Option A)
        """
        
        start_time = time.time()
        
        results = []
        not_in_bridge = []
        not_in_statement = []
        
        print(f"  Processing {len(txn_search_list)} transaction IDs...")
        
        for txn_id in txn_search_list:
            # Check if in bridge
            if txn_id not in bridge_map:
                not_in_bridge.append({
                    'transaction_id': txn_id,
                    'bank_id': None,
                    'status': 'NOT_IN_BRIDGE',
                    'error_type': 'Bridge file does not contain this transaction ID'
                })
                continue
            
            # Get bank_id from bridge
            bank_id = bridge_map[txn_id]
            
            # Find ALL occurrences in bank statement (Q1: Option A)
            matches = bank_df[bank_df['bank_id'] == bank_id]
            
            if matches.empty:
                not_in_statement.append({
                    'transaction_id': txn_id,
                    'bank_id': bank_id,
                    'status': 'NOT_IN_STATEMENT',
                    'error_type': 'Bank ID found in bridge but not in bank statement'
                })
                continue
            
            # Process ALL matches
            for _, row in matches.iterrows():
                debit = float(str(row['DEBITS']).replace(',', '')) if row['DEBITS'] else 0.0
                credit = float(str(row['CREDITS']).replace(',', '')) if row['CREDITS'] else 0.0
                
                # Determine status
                if debit > 0 and credit == 0:
                    status = 'SUCCESS'
                elif credit > 0 and debit == 0:
                    status = 'FAILED'
                else:
                    status = 'UNKNOWN'
                
                # Extract customer name from description
                desc = str(row['DESCRIPTION'])
                parts = desc.split(' - ')
                customer_name = parts[-1].strip() if len(parts) > 5 else 'N/A'
                
                results.append({
                    'transaction_id': txn_id,
                    'bank_id': bank_id,
                    'date': str(row['DATE']),
                    'debit_amount': debit,
                    'credit_amount': credit,
                    'status': status,
                    'customer_name': customer_name,
                    'branch': str(row['BRANCH']),
                    'reference_no': str(row['REFERENCE NO']),
                    'description': desc,
                    'error_type': None
                })
        
        # Calculate statistics
        success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
        failed_count = sum(1 for r in results if r['status'] == 'FAILED')
        total_success_amount = sum(r['debit_amount'] for r in results if r['status'] == 'SUCCESS')
        total_failed_amount = sum(r['credit_amount'] for r in results if r['status'] == 'FAILED')
        
        processing_time = time.time() - start_time
        
        print(f"  ✓ Reconciliation complete in {processing_time:.2f}s")
        print(f"    Found: {len(results)}")
        print(f"    Success: {success_count}, Failed: {failed_count}")
        print(f"    Not in bridge: {len(not_in_bridge)}")
        print(f"    Not in statement: {len(not_in_statement)}")
        
        return {
            'results': results,
            'not_in_bridge': not_in_bridge,
            'not_in_statement': not_in_statement,
            'statistics': {
                'total_searched': len(txn_search_list),
                'total_found': len(results),
                'success_count': success_count,
                'failed_count': failed_count,
                'not_in_bridge': len(not_in_bridge),
                'not_in_statement': len(not_in_statement),
                'total_success_amount': total_success_amount,
                'total_failed_amount': total_failed_amount,
                'processing_time': processing_time
            }
        }