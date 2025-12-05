from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
import sys
import os

# Add current directory to path to import utils
sys.path.append(os.path.dirname(__file__))

from utils.vertex_ai_handler import analyze_error, suggest_fix
from utils.auto_healer import apply_fix
from utils.data_quality import check_data_quality

PROJECT_ID = "beaming-force-480014-v3"
REGION = "us-central1"
CLUSTER_NAME = "self-healing-cluster"
BUCKET_NAME = "gagansamplebucket"

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0
}

def self_healing_callback(context):
    """
    Callback triggered on task failure.
    Fetches actual Dataproc Spark logs for AI analysis.
    """
    from google.cloud import dataproc_v1
    from google.cloud import storage
    
    exception = context.get('exception')
    task_instance = context.get('task_instance')
    
    print(f"Task failed! Exception: {exception}")
    
    error_log = ""
    
    try:
        # Extract job_id from the exception message or XCom
        exception_str = str(exception)
        
        # Try to find job ID in exception message
        import re
        job_id_match = re.search(r'job_id["\s:=]+([a-f0-9\-]+)', exception_str, re.IGNORECASE)
        
        if job_id_match:
            job_id = job_id_match.group(1)
            print(f"Found Dataproc job ID: {job_id}")
            
            # Fetch Dataproc job details
            job_client = dataproc_v1.JobControllerClient(
                client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
            )
            
            job = job_client.get_job(
                project_id=PROJECT_ID,
                region=REGION,
                job_id=job_id
            )
            
            print(f"Job status: {job.status.state.name}")
            
            # Get driver output URI (contains actual Spark logs)
            driver_output_uri = job.driver_output_resource_uri
            
            if driver_output_uri:
                print(f"Fetching Spark logs from: {driver_output_uri}")
                
                # Parse GCS URI: gs://bucket/path/to/file
                gcs_path = driver_output_uri.replace('gs://', '')
                parts = gcs_path.split('/', 1)
                bucket_name = parts[0]
                base_path = parts[1] if len(parts) > 1 else ''
                
                # Try multiple log file patterns
                log_files_to_try = [
                    base_path,  # driveroutput
                    base_path + '.000000000',  # driveroutput.000000000
                    base_path.replace('driveroutput', 'driveroutput.000000000'),
                ]
                
                storage_client = storage.Client(project=PROJECT_ID)
                bucket = storage_client.bucket(bucket_name)
                
                log_content = None
                for log_file in log_files_to_try:
                    try:
                        blob = bucket.blob(log_file)
                        log_content = blob.download_as_text()
                        print(f"Successfully fetched log from: {log_file}")
                        break
                    except Exception as e:
                        print(f"Could not fetch {log_file}: {str(e)[:100]}")
                        continue
                
                if log_content:
                    # Extract the Traceback section for better error analysis
                    traceback_start = log_content.find("Traceback (most recent call last):")
                    
                    if traceback_start != -1:
                        # Found Traceback - extract from there to end or next 4000 chars
                        error_section = log_content[traceback_start:traceback_start + 4000]
                        error_log = error_section
                        print(f"Retrieved {len(log_content)} bytes of Spark logs")
                        print(f"Extracted Traceback section ({len(error_section)} chars)")
                        print(f"Traceback preview: {error_section[:500]}")
                    else:
                        # No Traceback found, send both start and end
                        error_log = f"=== START ===\n{log_content[:2000]}\n\n=== END ===\n{log_content[-2000:]}"
                        print(f"Retrieved {len(log_content)} bytes of Spark logs (no Traceback found)")
                        print(f"Last 500 chars: {log_content[-500:]}")
                else:
                    # Fallback to job status
                    error_log = f"Job Status: {job.status.state.name}\nDetails: {job.status.details}\nException: {exception_str}"
                    print("Could not fetch any log files, using job status")
            else:
                # Use job status details
                error_log = f"Job Status: {job.status.state.name}\nDetails: {job.status.details}\nException: {exception_str}"
                print("No driver output URI found, using job status")
        else:
            print("Could not extract job_id from exception")
            error_log = exception_str
    
    except Exception as fetch_error:
        print(f"Error fetching Dataproc logs: {fetch_error}")
        error_log = f"Airflow Exception: {str(exception)}\nLog Fetch Error: {str(fetch_error)}"
    
    print("\n=== Initiating Self-Healing Protocol ===")
    analysis = analyze_error(error_log, PROJECT_ID)
    fix = suggest_fix(analysis)
    
    print(f"\nRoot Cause: {fix.get('root_cause')}")
    print(f"Fix Type: {fix.get('fix_type')}")
    print(f"Suggested Fix: {fix.get('suggested_fix')}")
    
    # Apply auto-fix
    dag_path = __file__
    script_path = f"gs://{BUCKET_NAME}/scripts/transform_script.py"
    
    apply_fix(fix, dag_path, script_path)

def run_data_quality_check(**kwargs):
    # Extract params from kwargs
    params = kwargs.get('params', {})
    input_table = params.get('input_table', 'employee_data')
    output_table = params.get('output_table', 'employee_data_output')
    
    check_data_quality(PROJECT_ID, "output", output_table)

with DAG(
    'self_healing_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    params={
        "input_table": "employee_data",
        "output_table": "employee_data_output"
    }
) as dag:

    # Cluster Configuration
    CLUSTER_CONFIG = {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
        }
    }

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    # PySpark Job Definition
    pyspark_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/transform_script.py",
            "args": [
                "--project_id", PROJECT_ID,
                "--input_table", "{{ params.input_table }}",
                "--output_table", "{{ params.output_table }}"
            ],
        },
    }

    submit_spark_job = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        job=pyspark_job,
        region=REGION,
        project_id=PROJECT_ID,
        on_failure_callback=self_healing_callback
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_data_quality_check,
        provide_context=True
    )

    # Define dependencies
    create_cluster >> submit_spark_job >> delete_cluster
    submit_spark_job >> quality_check
