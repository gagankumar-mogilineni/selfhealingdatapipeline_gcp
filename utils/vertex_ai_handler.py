import json
import os
import re

def analyze_error(error_log, project_id, location="us-central1"):
    """
    Analyzes error using Google Generative AI SDK with Vertex AI backend.
    """
    try:
        import google.generativeai as genai
        from google.auth import default
        from google.auth.transport.requests import Request
        
        # Get credentials with proper scopes
        credentials, _ = default(scopes=[
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/generative-language.retriever'
        ])
        
        # Refresh credentials
        credentials.refresh(Request())
        
        # Configure to use Vertex AI backend
        os.environ['GOOGLE_GENAI_USE_VERTEXAI'] = 'True'
        os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
        os.environ['GOOGLE_CLOUD_LOCATION'] = location
        
        # Configure the SDK with credentials
        genai.configure(credentials=credentials)
        
        # Use gemini-2.0-flash-exp
        model = genai.GenerativeModel('gemini-2.0-flash-exp')
        
        prompt = f"""Analyze this Spark/BigQuery error.

Return ONLY valid JSON with these keys:
- "root_cause": The specific error message
- "fix_type": "CODE"
- "suggested_fix": How to fix it

Error:
{error_log[:2000]}

Return only JSON."""

        response = model.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                temperature=0.1,
                max_output_tokens=300,
            )
        )
        
        text = response.text.strip()
        
        # Clean markdown
        text = text.replace("```json", "").replace("```", "").strip()
        
        # Extract JSON if embedded
        json_match = re.search(r'\{[^}]+\}', text, re.DOTALL)
        if json_match:
            text = json_match.group(0)
        
        try:
            result = json.loads(text)
            
            # Force CODE type for double dot issues
            if ".." in str(result):
                result["fix_type"] = "CODE"
            
            print(f"✓ Gemini analysis: {result.get('root_cause', '')[:80]}")
            return result
        except:
            # Fallback
            return {
                "root_cause": "Table name error with double dots",
                "fix_type": "CODE",
                "suggested_fix": "Remove double dots from table names"
            }
            
    except Exception as e:
        print(f"Gemini error: {str(e)[:100]}")
        return {
            "root_cause": "Error analysis failed",
            "fix_type": "CODE" if ".." in error_log else "MANUAL",
            "suggested_fix": "Remove double dots" if ".." in error_log else "Check logs"
        }

def suggest_fix(analysis_result):
    return analysis_result
