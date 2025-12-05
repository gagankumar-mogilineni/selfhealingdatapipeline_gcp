from google.cloud import storage
import re

def apply_fix(fix_suggestion, dag_file_path, script_path):
    """Applies AI-suggested fixes to code files."""
    fix_type = fix_suggestion.get("fix_type")
    suggestion = fix_suggestion.get("suggested_fix", "")
    root_cause = fix_suggestion.get("root_cause", "")
    
    print(f"\n{'='*60}")
    print(f"AUTO-FIX: {fix_type}")
    print(f"Root Cause: {root_cause}")
    print(f"Fix: {suggestion}")
    print(f"{'='*60}\n")

    if fix_type == "CODE" or (fix_type == "CONFIG" and ".." in root_cause):
        try:
            if script_path.startswith("gs://"):
                fix_gcs_file(script_path, root_cause)
            else:
                fix_local_file(script_path, root_cause)
            print(f"✓ Fixed: {script_path}")
        except Exception as e:
            print(f"✗ Auto-fix failed: {e}")
    else:
        print(f"Manual fix required: {suggestion}")

def fix_gcs_file(gcs_path, root_cause):
    """Fix file in GCS."""
    gcs_path_clean = gcs_path.replace('gs://', '')
    bucket_name, blob_path = gcs_path_clean.split('/', 1)
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    content = blob.download_as_text()
    fixed_content = apply_code_fix(content, root_cause)
    
    if fixed_content != content:
        blob.upload_from_string(fixed_content, content_type='text/x-python')
        print(f"Uploaded fixed file to GCS")

def fix_local_file(file_path, root_cause):
    """Fix local file."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    fixed_content = apply_code_fix(content, root_cause)
    
    if fixed_content != content:
        with open(file_path, 'w') as f:
            f.write(fixed_content)

def apply_code_fix(content, root_cause):
    """Apply pattern-based fixes."""
    if "double dot" in root_cause.lower() or ".." in root_cause:
        fixed = re.sub(r'\.\.+', '.', content)
        if fixed != content:
            print("Applied: Removed double dots")
            return fixed
    return content
