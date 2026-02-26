import google.generativeai as genai
import os
import json
from typing import Dict, Any, List

class GeminiAgent:
    """Gemini AI agent for intelligent file processing"""
    
    def __init__(self):
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")
        
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
        
    def execute_skill(self, skill_prompt: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a skill with context"""
        
        full_prompt = f"""
{skill_prompt}

Context:
{json.dumps(context, indent=2, default=str)}

CRITICAL: Respond ONLY with valid JSON. No markdown, no explanation.
"""
        
        try:
            response = self.model.generate_content(
                full_prompt,
                generation_config={
                    'temperature': 0.1,
                    'top_p': 0.8,
                    'max_output_tokens': 4096,
                }
            )
            
            response_text = response.text.strip()
            
            # Clean markdown
            if response_text.startswith('```json'):
                response_text = response_text.split('```json')[1].split('```')[0].strip()
            elif response_text.startswith('```'):
                response_text = response_text.split('```')[1].split('```')[0].strip()
            
            return json.loads(response_text)
            
        except Exception as e:
            print(f"Gemini API Error: {str(e)}")
            raise
    
    def chat(self, user_message: str, search_history: List[Dict], transaction_data: List[Dict]) -> str:
        """Chat interface for querying transaction data"""
        
        chat_prompt = f"""
You are an AI assistant helping with bank reconciliation queries.

User Question: {user_message}

Available Data:
- Recent Searches: {len(search_history)} reconciliations
- Transaction Records: {len(transaction_data)} transactions

Search History Summary:
{json.dumps(search_history[:5], indent=2, default=str)}

Transaction Data Sample:
{json.dumps(transaction_data[:10], indent=2, default=str)}

Analyze the user's question and provide a helpful, accurate response.
If they ask about a specific transaction ID, search for it and provide details.
If they ask for summaries or statistics, calculate from the data.

Respond in a professional, helpful tone. Include relevant details.
"""
        
        try:
            response = self.model.generate_content(
                chat_prompt,
                generation_config={
                    'temperature': 0.3,
                    'max_output_tokens': 2048,
                }
            )
            
            return response.text.strip()
            
        except Exception as e:
            return f"Error processing your question: {str(e)}"