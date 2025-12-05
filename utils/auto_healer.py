from google.cloud import storage
import re

def apply_fix(fix_suggestion, dag_file_path, script_path):
    """
    Applies the fix suggested by Vertex AI.
    Can handle both local and GCS paths.
    
    Args:
        fix_suggestion (dict): The JSON output from analyze_error.
        dag_file_path (str): Path to the Airflow DAG file (to update config).
        script_path (str): Path to the PySpark script (GCS or local).
    """
    fix_type = fix_suggestion.get("fix_type")
    suggestion = fix_suggestion.get("suggested_fix", "")
    root_cause = fix_suggestion.get("root_cause", "")
    
    print(f"\n{'='*60}")
    print(f"APPLYING AUTO-FIX: {fix_type}")
    print(f"{'='*60}")
    print(f"Root Cause: {root_cause}")
    print(f"Suggested Fix: {suggestion}")
    print(f"{'='*60}\n")

    if fix_type == "CODE" or (fix_type == "CONFIG" and ".." in root_cause):
        # Auto-fix code issues (including table name config issues)
        try:
            # Check if it's a GCS path
            if script_path.startswith("gs://"):
                fix_gcs_file(script_path, root_cause, suggestion)
            else:
                fix_local_file(script_path, root_cause, suggestion)
            
            print(f"✓ Successfully applied code fix to {script_path}")
        except Exception as e:
            print(f"✗ Failed to apply auto-fix: {e}")
            print(f"Manual intervention required: {suggestion}")

    elif fix_type == "CONFIG":
        print(f"CONFIG fix needed in {dag_file_path}")
        print(f"Suggestion: {suggestion}")
        print("Manual configuration update required.")

    else:
        print("Manual intervention required.")
        print(f"Suggestion: {suggestion}")

def fix_gcs_file(gcs_path, root_cause, suggestion):
    """
    Fix a file in GCS by downloading, fixing, and re-uploading.
    """
    # Parse GCS path: gs://bucket/path/to/file
    gcs_path_clean = gcs_path.replace('gs://', '')
    parts = gcs_path_clean.split('/', 1)
    bucket_name = parts[0]
    blob_path = parts[1] if len(parts) > 1 else ''
    
    # Download file from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    content = blob.download_as_text()
    print(f"Downloaded {len(content)} bytes from {gcs_path}")
    
    # Apply fix based on root cause
    fixed_content = apply_code_fix(content, root_cause, suggestion)
    
    if fixed_content != content:
        # Upload fixed file back to GCS
        blob.upload_from_string(fixed_content, content_type='text/x-python')
        print(f"Uploaded fixed file to {gcs_path}")
    else:
        print("No changes made - pattern not found or already fixed")

def fix_local_file(file_path, root_cause, suggestion):
    """
    Fix a local file.
    """
    with open(file_path, 'r') as f:
        content = f.read()
    
    fixed_content = apply_code_fix(content, root_cause, suggestion)
    
    if fixed_content != content:
        with open(file_path, 'w') as f:
            f.write(fixed_content)
        print(f"Fixed local file: {file_path}")
    else:
        print("No changes made - pattern not found or already fixed")

def apply_code_fix(content, root_cause, suggestion):
    """
    Apply code fixes based on common patterns.
    """
    # Pattern 1: Double dot in table name
    if "double dot" in suggestion.lower() or ".." in root_cause:
        # Fix: selfhealing..table -> selfhealing.table
        fixed = re.sub(r'\.\.+', '.', content)
        if fixed != content:
            print("Applied fix: Removed double dots from table references")
            return fixed
    
    # Pattern 2: Missing dataset
    if "dataset" in suggestion.lower() and "not found" in root_cause.lower():
        print("Dataset creation required - cannot auto-fix")
        return content
    
    # Pattern 3: Permission errors
    if "permission" in root_cause.lower():
        print("Permission issue - cannot auto-fix code")
        return content
    
    # Pattern 4: Syntax errors (generic)
    if "syntax" in suggestion.lower():
        print("Syntax error detected - manual review recommended")
        return content
    
    # If no specific pattern matched, return original
    return content
