import pandas as pd
from typing import List, Dict
import time

class ReconcilerAgent:
    """
    Deterministic reconciliation agent
    Handles Q1: Returns ALL matches (Option A)
    Optimized with vectorized pandas operations.
    """

    def reconcile(
        self,
        bank_df: pd.DataFrame,
        bridge_map: Dict[str, str],
        txn_search_list: List[str]
    ) -> Dict:
        """
        Reconcile transactions.
        Returns ALL matches for each bank_id (Q1: Option A).
        """

        start_time = time.time()

        print(f"  Processing {len(txn_search_list)} transaction IDs...")

        # ── Pre-process bank_df columns once (vectorized) ─────
        bank_df = bank_df.copy()
        bank_df['parsed_debit'] = (
            bank_df['DEBITS'].astype(str)
            .str.replace(',', '', regex=False)
        )
        bank_df['parsed_debit'] = pd.to_numeric(bank_df['parsed_debit'], errors='coerce').fillna(0.0)

        bank_df['parsed_credit'] = (
            bank_df['CREDITS'].astype(str)
            .str.replace(',', '', regex=False)
        )
        bank_df['parsed_credit'] = pd.to_numeric(bank_df['parsed_credit'], errors='coerce').fillna(0.0)

        bank_df['parsed_desc'] = bank_df['DESCRIPTION'].astype(str)
        parts_series = bank_df['parsed_desc'].str.split(' - ')
        bank_df['parsed_customer'] = parts_series.apply(
            lambda p: p[-1].strip() if len(p) > 5 else 'N/A'
        )

        bank_df['parsed_status'] = 'UNKNOWN'
        bank_df.loc[(bank_df['parsed_debit'] > 0) & (bank_df['parsed_credit'] == 0), 'parsed_status'] = 'SUCCESS'
        bank_df.loc[(bank_df['parsed_credit'] > 0) & (bank_df['parsed_debit'] == 0), 'parsed_status'] = 'FAILED'

        bank_df['parsed_date'] = bank_df['DATE'].astype(str)
        bank_df['parsed_branch'] = bank_df['BRANCH'].astype(str)
        bank_df['parsed_ref'] = bank_df['REFERENCE NO'].astype(str) if 'REFERENCE NO' in bank_df.columns else ''

        # ── Group bank rows by bank_id for O(1) lookup ────────
        grouped = dict(tuple(bank_df.groupby('bank_id')))

        # ── Process transactions ───────────────────────────────
        results = []
        not_in_bridge = []
        not_in_statement = []

        for txn_id in txn_search_list:
            if txn_id not in bridge_map:
                not_in_bridge.append({
                    'transaction_id': txn_id,
                    'bank_id': None,
                    'status': 'NOT_IN_BRIDGE',
                    'error_type': 'Bridge file does not contain this transaction ID'
                })
                continue

            bank_id = bridge_map[txn_id]
            matches = grouped.get(bank_id)

            if matches is None or matches.empty:
                not_in_statement.append({
                    'transaction_id': txn_id,
                    'bank_id': bank_id,
                    'status': 'NOT_IN_STATEMENT',
                    'error_type': 'Bank ID found in bridge but not in bank statement'
                })
                continue

            # itertuples is ~100x faster than iterrows
            for row in matches.itertuples(index=False):
                results.append({
                    'transaction_id': txn_id,
                    'bank_id': bank_id,
                    'date': row.parsed_date,
                    'debit_amount': row.parsed_debit,
                    'credit_amount': row.parsed_credit,
                    'status': row.parsed_status,
                    'customer_name': row.parsed_customer,
                    'branch': row.parsed_branch,
                    'reference_no': row.parsed_ref,
                    'description': row.parsed_desc,
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
