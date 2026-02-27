from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException, Query
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
from tasks.reconciliation_task import process_reconciliation_task

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
    background_tasks: BackgroundTasks,
    bank_statement: UploadFile = File(...),
    bridge_file: UploadFile = File(...),
    transaction_ids: UploadFile = File(...)
):
    """
    Main reconciliation endpoint (ASYNC)
    Accepts files, creates a background task, and returns immediately
    """
    
    task_id = str(uuid.uuid4())
    search_id = task_id # Use the same ID for both for tracking simplicity
    
    try:
        # Save files to temp directory to avoid OOM killer on Render
        temp_bank_path = f"temp/{search_id}_{bank_statement.filename}"
        temp_bridge_path = f"temp/{search_id}_{bridge_file.filename}"
        temp_txn_path = f"temp/{search_id}_{transaction_ids.filename}"
        
        with open(temp_bank_path, "wb") as buffer:
            buffer.write(await bank_statement.read())
            
        with open(temp_bridge_path, "wb") as buffer:
            buffer.write(await bridge_file.read())
            
        with open(temp_txn_path, "wb") as buffer:
            buffer.write(await transaction_ids.read())
        
        # Create task in database
        db.create_task(task_id)
        
        # Dispatch background task immediately
        background_tasks.add_task(
            process_reconciliation_task,
            task_id=task_id,
            search_id=search_id,
            bank_path=temp_bank_path,
            bridge_path=temp_bridge_path,
            txn_path=temp_txn_path,
            bank_filename=bank_statement.filename,
            bridge_filename=bridge_file.filename,
            txn_filename=transaction_ids.filename,
            db_instance=db,
            reconciler_agent_instance=reconciler_agent,
            csv_generator_instance=csv_generator
        )
        
        return {
            "status": "accepted",
            "task_id": task_id,
            "message": "Reconciliation task started in the background."
        }
        
    except Exception as e:
        print(f"\n❌ STARTUP ERROR: {str(e)}\n")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/reconcile/status/{task_id}")
async def get_reconciliation_status(task_id: str):
    """
    Polling endpoint for frontend to check the progress of the task
    """
    status_data = db.get_task_status(task_id)
    if not status_data:
        raise HTTPException(status_code=404, detail="Task not found")
        
    return status_data

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