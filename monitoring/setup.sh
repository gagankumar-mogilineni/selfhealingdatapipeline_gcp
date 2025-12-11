#!/bin/bash

# Financial Monitoring System - Quick Setup Script
# This script sets up the complete monitoring system

set -e

echo "=========================================="
echo "Financial Monitoring System Setup"
echo "=========================================="

# Configuration
PROJECT_ID="beaming-force-480014-v3"
COMPOSER_BUCKET="us-central1-selfhealingpipe-a8f6a28e-bucket"
DATASET_ID="financial_monitoring"

# Step 1: Create BigQuery dataset
echo ""
echo "Step 1: Creating BigQuery dataset..."
bq mk --dataset --location=us-central1 $PROJECT_ID:$DATASET_ID || echo "Dataset already exists"

# Step 2: Create tables
echo ""
echo "Step 2: Creating BigQuery tables..."
bq query --use_legacy_sql=false < monitoring/setup_bigquery.sql

# Step 3: Upload monitoring module
echo ""
echo "Step 3: Uploading monitoring module to Composer..."
gsutil -m cp -r monitoring gs://$COMPOSER_BUCKET/dags/

# Step 4: Upload DAG
echo ""
echo "Step 4: Uploading financial monitoring DAG..."
gsutil cp dags/financial_monitoring_dag.py gs://$COMPOSER_BUCKET/dags/

echo ""
echo "=========================================="
echo "✓ Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Configure Slack webhook in dags/financial_monitoring_dag.py"
echo "2. Customize EXPECTED_FEEDS list"
echo "3. Wait 2-3 minutes for Airflow to detect the DAG"
echo "4. Trigger manually or wait for scheduled run (5 PM daily)"
echo ""
echo "To trigger manually:"
echo "gcloud composer environments run selfhealingpipeline \\"
echo "    --location us-central1 \\"
echo "    dags trigger financial_monitoring"
echo ""
