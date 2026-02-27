import os
import io
import uuid
import pandas as pd
from datetime import datetime
from typing import Dict, List
import asyncio

async def process_reconciliation_task(
    task_id: str,
    search_id: str,
    bank_path: str,
    bridge_path: str,
    txn_path: str,
    bank_filename: str,
    bridge_filename: str,
    txn_filename: str,
    db_instance,
    reconciler_agent_instance,
    csv_generator_instance
):
    """
    Background task to process the heavy reconciliation logic
    """
    start_time = datetime.now()
    
    try:
        # Step 1: Read Bank Statement
        db_instance.update_task_progress(task_id, 10, "Reading Bank Statement...")
        
        # Read once, find header in memory (avoids slow double-read)
        df_raw = pd.read_excel(bank_path, header=None)
        header_row = None
        for i, row in df_raw.iterrows():
            if 'DATE' in str(row.values):
                header_row = i
                break

        if header_row is None:
            raise ValueError("Could not find header row in bank statement")

        # Reuse the already-loaded data instead of reading the file again
        bank_df = df_raw.iloc[header_row + 1:].copy()
        bank_df.columns = df_raw.iloc[header_row].values
        bank_df = bank_df.reset_index(drop=True)
        
        # Step 2: Extract Bank IDs
        db_instance.update_task_progress(task_id, 20, "Extracting Bank IDs...")
        
        def extract_bank_id(description):
            try:
                parts = str(description).split(' - ')
                if len(parts) > 3:
                    return parts[3].strip().upper()
                return None
            except:
                return None
                
        bank_df['bank_id'] = bank_df['DESCRIPTION'].apply(extract_bank_id)
        bank_df = bank_df.dropna(subset=['bank_id'])
        
        # Step 3: Parse Bridge File
        db_instance.update_task_progress(task_id, 30, "Parsing Bridge File...")
        
        with open(bridge_path, 'r', encoding='utf-8') as f:
            bridge_text = f.read()
            
        bridge_lines = [line.strip() for line in bridge_text.split('\n') if line.strip()]
        
        bridge_map = {}
        for i in range(0, len(bridge_lines), 2):
            if i + 1 < len(bridge_lines):
                txn_id = bridge_lines[i].strip().upper()
                bank_id = bridge_lines[i + 1].strip().upper()
                bridge_map[txn_id] = bank_id
                
        # Step 4: Parse Transaction IDs
        db_instance.update_task_progress(task_id, 40, "Reading Transaction IDs...")
        
        with open(txn_path, 'r', encoding='utf-8') as f:
            txn_text = f.read()
            
        if ',' in txn_text or '\t' in txn_text:
            txn_df = pd.read_csv(txn_path)
            txn_search_list = txn_df.iloc[:, 0].astype(str).str.strip().str.upper().tolist()
        else:
            txn_search_list = [line.strip().upper() for line in txn_text.split('\n') if line.strip()]
            
        txn_search_list = list(set(txn_search_list))
        
        # Step 5: AI Reconciliation
        db_instance.update_task_progress(task_id, 50, "Performing AI Reconciliation...")
        
        reconciliation_result = reconciler_agent_instance.reconcile(
            bank_df,
            bridge_map,
            txn_search_list
        )
        
        # Step 6: Generate CSV
        db_instance.update_task_progress(task_id, 80, "Generating CSV Report...")
        
        csv_path = csv_generator_instance.generate_reconciliation_csv(
            reconciliation_result['results'],
            reconciliation_result['not_in_bridge'],
            reconciliation_result['not_in_statement'],
            search_id,
            {
                'bank_statement': bank_filename,
                'bridge_file': bridge_filename,
                'transaction_ids': txn_filename
            }
        )
        
        # Step 7: Save to Database
        db_instance.update_task_progress(task_id, 90, "Saving to Database...")
        total_time = (datetime.now() - start_time).total_seconds()
        
        db_instance.save_search_history({
            'search_id': search_id,
            'timestamp': start_time.isoformat(),
            'bank_statement_file': bank_filename,
            'bridge_file': bridge_filename,
            'transaction_ids_file': txn_filename,
            'total_searched': reconciliation_result['statistics']['total_searched'],
            'total_found': reconciliation_result['statistics']['total_found'],
            'success_count': reconciliation_result['statistics']['success_count'],
            'failed_count': reconciliation_result['statistics']['failed_count'],
            'not_in_bridge': reconciliation_result['statistics']['not_in_bridge'],
            'not_in_statement': reconciliation_result['statistics']['not_in_statement'],
            'total_success_amount': reconciliation_result['statistics']['total_success_amount'],
            'total_failed_amount': reconciliation_result['statistics']['total_failed_amount'],
            'processing_time': total_time,
            'csv_output_path': csv_path
        })
        
        all_transactions = (
            reconciliation_result['results'] +
            reconciliation_result['not_in_bridge'] +
            reconciliation_result['not_in_statement']
        )
        db_instance.save_transaction_details(search_id, all_transactions)
        
        # Step 8: Done!
        final_result = {
            "status": "success",
            "search_id": search_id,
            "summary": reconciliation_result['statistics'],
            "csv_download_url": f"/api/download-csv/{search_id}",
            "timestamp": start_time.isoformat(),
            "processing_time": total_time
        }
        
        db_instance.complete_task(task_id, final_result)
        
    except Exception as e:
        db_instance.fail_task(task_id, str(e))
    finally:
        # Cleanup temporary files
        try:
            if os.path.exists(bank_path): os.remove(bank_path)
            if os.path.exists(bridge_path): os.remove(bridge_path)
            if os.path.exists(txn_path): os.remove(txn_path)
        except Exception as cleanup_error:
            print(f"Failed to cleanup temp files: {cleanup_error}")
