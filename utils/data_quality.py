from google.cloud import bigquery
import os

def check_data_quality(project_id, dataset_id, table_id):
    """
    Checks for null values in the specified BigQuery table and uses Vertex AI to interpret results.
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # Get schema to find columns
    table = client.get_table(table_ref)
    columns = [schema.name for schema in table.schema]
    
    # Construct query to count nulls for each column
    checks = [f"COUNTIF({col} IS NULL) as {col}_nulls" for col in columns]
    query = f"SELECT {', '.join(checks)} FROM `{table_ref}`"
    
    query_job = client.query(query)
    results = query_job.result()
    
    row = next(results)
    null_counts = {key: val for key, val in row.items()}
    
    print(f"Data Quality Scan Results: {null_counts}")
    
    # AI Analysis
    analyze_quality_with_ai(null_counts, project_id)

def analyze_quality_with_ai(null_counts, project_id):
    """Analyze data quality using Gemini AI."""
    try:
        import google.generativeai as genai
        from google.auth import default
        from google.auth.transport.requests import Request
        
        credentials, _ = default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        credentials.refresh(Request())
        
        os.environ['GOOGLE_GENAI_USE_VERTEXAI'] = 'True'
        os.environ['GOOGLE_CLOUD_PROJECT'] = project_id
        os.environ['GOOGLE_CLOUD_LOCATION'] = 'us-central1'
        
        genai.configure(credentials=credentials)
        model = genai.GenerativeModel('gemini-2.0-flash-exp')
        
        prompt = f"""Analyze this data quality report (null counts per column).
Determine if the data quality is acceptable or if there are anomalies.

Report: {null_counts}

Output a brief summary and recommendation."""
        
        response = model.generate_content(prompt, generation_config=genai.GenerationConfig(temperature=0.1, max_output_tokens=200))
        print(f"✓ AI Quality Assessment: {response.text}")
        
    except Exception as e:
        print(f"AI analysis failed: {e}")
        # Simple rule-based check
        total_nulls = sum(null_counts.values())
        if total_nulls == 0:
            print("✓ Data quality check passed - no null values found")
        else:
            print(f"⚠ Warning: {total_nulls} total null values found")
