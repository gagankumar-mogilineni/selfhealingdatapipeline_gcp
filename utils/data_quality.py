from google.cloud import bigquery

def check_data_quality(project_id, dataset_id, table_id):
    """Check data quality by counting nulls."""
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT 
            COUNTIF(column_name IS NULL) as null_count
        FROM `{project_id}.{dataset_id}.{table_id}`
    """
    
    try:
        results = client.query(query).result()
        for row in results:
            print(f"Null count: {row.null_count}")
            if row.null_count > 0:
                print(f"⚠ Warning: {row.null_count} null values found")
            else:
                print("✓ Data quality check passed")
    except Exception as e:
        print(f"Quality check failed: {e}")

def run_data_quality_check(project_id, output_table):
    """Run quality check on output table."""
    print(f"\n{'='*60}")
    print("DATA QUALITY CHECK")
    print(f"{'='*60}")
    check_data_quality(project_id, "output", output_table)
    print(f"{'='*60}\n")
