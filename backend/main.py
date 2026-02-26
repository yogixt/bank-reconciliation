from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import pandas as pd
import io
import os
import uuid
from datetime import datetime
from typing import List, Optional
from dotenv import load_dotenv
import aiofiles

from database.db import Database
from agents.gemini_agent import GeminiAgent
from agents.reconciler_agent import ReconcilerAgent
from utils.csv_generator import CSVGenerator

load_dotenv()

app = FastAPI(
    title="unotag Bank Reconciliation API - Production",
    description="Production-ready reconciliation system with AI",
    version="3.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize
db = Database()
gemini_agent = GeminiAgent()
reconciler_agent = ReconcilerAgent()
csv_generator = CSVGenerator()

# Create directories
os.makedirs('temp', exist_ok=True)
os.makedirs('outputs', exist_ok=True)

@app.get("/")
async def root():
    return {
        "service": "unotag Bank Reconciliation API",
        "status": "operational",
        "version": "3.0.0",
        "features": [
            "Smart bank ID extraction",
            "Bridge file parsing (N, N+1 format)",
            "Debit/Credit status detection",
            "ALL matches returned (Q1: Option A)",
            "Detailed error messages (Q2: Option A)",
            "Historical search (Q3: Option B)",
            "Cloud CSV storage (Q4: Option C)"
        ]
    }

@app.get("/api/health")
async def health_check():
    return {"status": "ok"}

@app.post("/api/reconcile")
async def reconcile_files(
    bank_statement: UploadFile = File(...),
    bridge_file: UploadFile = File(...),
    transaction_ids: UploadFile = File(...)
):
    """
    Main reconciliation endpoint
    Processes files and returns detailed results
    """
    
    search_id = str(uuid.uuid4())
    start_time = datetime.now()
    
    try:
        print(f"\n{'='*80}")
        print(f"🚀 SEARCH [{search_id}] - Bank Reconciliation")
        print(f"{'='*80}\n")
        
        # ============================================
        # STEP 1: READ BANK STATEMENT
        # ============================================
        print("📊 STEP 1: Reading Bank Statement")
        
        bank_content = await bank_statement.read()
        
        # Find header row
        df_raw = pd.read_excel(io.BytesIO(bank_content), header=None)
        header_row = None
        for i, row in df_raw.iterrows():
            if 'DATE' in str(row.values):
                header_row = i
                break
        
        if header_row is None:
            raise ValueError("Could not find header row in bank statement")
        
        # Read with correct header
        bank_df = pd.read_excel(io.BytesIO(bank_content), skiprows=header_row, header=0)
        
        print(f"  ✓ Bank statement loaded: {len(bank_df)} transactions")
        
        # ============================================
        # STEP 2: EXTRACT BANK IDs
        # ============================================
        print("\n🧠 STEP 2: Extracting Bank IDs from Descriptions")
        
        def extract_bank_id(description):
            """Extract bank ID from IMPS description (Part 3)"""
            try:
                parts = str(description).split(' - ')
                if len(parts) > 3:
                    return parts[3].strip().upper()
                return None
            except:
                return None
        
        bank_df['bank_id'] = bank_df['DESCRIPTION'].apply(extract_bank_id)
        bank_df = bank_df.dropna(subset=['bank_id'])
        
        print(f"  ✓ Extracted {len(bank_df)} bank IDs")
        
        # ============================================
        # STEP 3: PARSE BRIDGE FILE (N, N+1 format)
        # ============================================
        print("\n🔗 STEP 3: Parsing Bridge File")
        
        bridge_content = await bridge_file.read()
        bridge_text = bridge_content.decode('utf-8')
        bridge_lines = [line.strip() for line in bridge_text.split('\n') if line.strip()]
        
        bridge_map = {}
        for i in range(0, len(bridge_lines), 2):
            if i + 1 < len(bridge_lines):
                txn_id = bridge_lines[i].strip().upper()
                bank_id = bridge_lines[i + 1].strip().upper()
                bridge_map[txn_id] = bank_id
        
        print(f"  ✓ Bridge file parsed: {len(bridge_map)} mappings")
        
        # ============================================
        # STEP 4: READ TRANSACTION IDs TO SEARCH
        # ============================================
        print("\n📝 STEP 4: Reading Transaction IDs")
        
        txn_content = await transaction_ids.read()
        txn_text = txn_content.decode('utf-8')
        
        if ',' in txn_text or '\t' in txn_text:
            txn_df = pd.read_csv(io.StringIO(txn_text))
            txn_search_list = txn_df.iloc[:, 0].astype(str).str.strip().str.upper().tolist()
        else:
            txn_search_list = [line.strip().upper() for line in txn_text.split('\n') if line.strip()]
        
        txn_search_list = list(set(txn_search_list))
        
        print(f"  ✓ Transaction IDs to search: {len(txn_search_list)}")
        
        # ============================================
        # STEP 5: RECONCILIATION
        # ============================================
        print("\n⚡ STEP 5: Performing Reconciliation")
        
        reconciliation_result = reconciler_agent.reconcile(
            bank_df,
            bridge_map,
            txn_search_list
        )
        
        # ============================================
        # STEP 6: GENERATE CSV
        # ============================================
        print("\n📄 STEP 6: Generating CSV Report")
        
        csv_path = csv_generator.generate_reconciliation_csv(
            reconciliation_result['results'],
            reconciliation_result['not_in_bridge'],
            reconciliation_result['not_in_statement'],
            search_id,
            {
                'bank_statement': bank_statement.filename,
                'bridge_file': bridge_file.filename,
                'transaction_ids': transaction_ids.filename
            }
        )
        
        # ============================================
        # STEP 7: SAVE TO DATABASE
        # ============================================
        print("\n💾 STEP 7: Saving to Database")
        
        total_time = (datetime.now() - start_time).total_seconds()
        
        # Save search history
        db.save_search_history({
            'search_id': search_id,
            'timestamp': start_time.isoformat(),
            'bank_statement_file': bank_statement.filename,
            'bridge_file': bridge_file.filename,
            'transaction_ids_file': transaction_ids.filename,
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
        
        # Save transaction details
        all_transactions = (
            reconciliation_result['results'] +
            reconciliation_result['not_in_bridge'] +
            reconciliation_result['not_in_statement']
        )
        db.save_transaction_details(search_id, all_transactions)
        
        print(f"\n{'='*80}")
        print(f"✅ RECONCILIATION COMPLETE in {total_time:.2f}s")
        print(f"{'='*80}\n")
        
        return {
            "status": "success",
            "search_id": search_id,
            "summary": reconciliation_result['statistics'],
            "csv_download_url": f"/api/download-csv/{search_id}",
            "timestamp": start_time.isoformat()
        }
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}\n")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/download-csv/{search_id}")
async def download_csv(search_id: str):
    """Download CSV report"""
    search = db.get_search_by_id(search_id)
    
    if not search:
        raise HTTPException(status_code=404, detail="Search not found")
    
    csv_path = search['csv_output_path']
    
    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="CSV file not found")
    
    return FileResponse(
        csv_path,
        media_type="text/csv",
        filename=f"reconciliation_{search_id}.csv"
    )

@app.get("/api/history")
async def get_history():
    """Get all search history"""
    history = db.get_all_searches()
    return {"history": history}

@app.get("/api/history/{search_id}")
async def get_search_details(search_id: str):
    """Get details of a specific search"""
    search = db.get_search_by_id(search_id)
    
    if not search:
        raise HTTPException(status_code=404, detail="Search not found")
    
    transactions = db.get_transactions_by_search_id(search_id)
    
    return {
        "search": search,
        "transactions": transactions
    }

@app.post("/api/chat")
async def chat(message: str = Query(...)):
    """
    Chat with Gemini about transaction data
    Searches ALL historical data (Q3: Option B)
    """
    
    try:
        # Get all search history
        search_history = db.get_all_searches()
        
        # Get all transactions from recent searches
        all_transactions = []
        for search in search_history[:10]:  # Last 10 searches
            transactions = db.get_transactions_by_search_id(search['search_id'])
            all_transactions.extend(transactions)
        
        # Get Gemini response
        response = gemini_agent.chat(message, search_history, all_transactions)
        
        return {
            "status": "success",
            "response": response
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/search-transaction/{transaction_id}")
async def search_transaction(transaction_id: str):
    """
    Search for a transaction across ALL history
    Q3: Option B - searches all historical data
    """
    results = db.search_transaction_by_id(transaction_id.upper())
    
    if not results:
        return {
            "found": False,
            "message": f"Transaction ID {transaction_id} not found in any search history"
        }
    
    return {
        "found": True,
        "results": results
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)