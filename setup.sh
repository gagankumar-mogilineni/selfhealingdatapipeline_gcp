#!/bin/bash

# Self-Healing Data Pipeline - Automated Setup Script
# This script sets up the complete pipeline in a new GCP project

set -e  # Exit on error

# ============================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================

PROJECT_ID="your-project-id"              # Replace with your GCP project ID
REGION="us-central1"                       # GCP region
BUCKET_NAME="your-bucket-name"            # GCS bucket name (must be globally unique)
COMPOSER_ENV="selfhealingpipeline"        # Cloud Composer environment name
CLUSTER_NAME="self-healing-cluster"       # Dataproc cluster name

# ============================================================
# STEP 1: SET PROJECT AND AUTHENTICATE
# ============================================================

echo "=========================================="
echo "STEP 1: Setting up GCP project"
echo "=========================================="

gcloud config set project $PROJECT_ID
gcloud auth application-default login

# Get project number
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

echo "✓ Project ID: $PROJECT_ID"
echo "✓ Project Number: $PROJECT_NUMBER"
echo "✓ Compute Service Account: $COMPUTE_SA"

# ============================================================
# STEP 2: ENABLE REQUIRED APIs
# ============================================================

echo ""
echo "=========================================="
echo "STEP 2: Enabling required APIs"
echo "=========================================="

gcloud services enable dataproc.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable composer.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable aiplatform.googleapis.com
gcloud services enable generativelanguage.googleapis.com

echo "✓ All APIs enabled"

# ============================================================
# STEP 3: GRANT IAM PERMISSIONS
# ============================================================

echo ""
echo "=========================================="
echo "STEP 3: Granting IAM permissions"
echo "=========================================="

# Grant Storage Admin role
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member=serviceAccount:$COMPUTE_SA \
    --role=roles/storage.admin \
    --condition=None

# Grant Vertex AI User role
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member=serviceAccount:$COMPUTE_SA \
    --role=roles/aiplatform.user \
    --condition=None

echo "✓ IAM roles granted to Compute Engine service account"

# ============================================================
# STEP 4: CREATE GCS BUCKET
# ============================================================

echo ""
echo "=========================================="
echo "STEP 4: Creating GCS bucket"
echo "=========================================="

gsutil mb -p $PROJECT_ID -l $REGION gs://$BUCKET_NAME || echo "Bucket already exists"

echo "✓ GCS bucket created: gs://$BUCKET_NAME"

# ============================================================
# STEP 5: CREATE BIGQUERY DATASETS
# ============================================================

echo ""
echo "=========================================="
echo "STEP 5: Creating BigQuery datasets"
echo "=========================================="

bq mk --dataset --location=$REGION $PROJECT_ID:selfhealing || echo "Dataset selfhealing already exists"
bq mk --dataset --location=$REGION $PROJECT_ID:output || echo "Dataset output already exists"

# Create sample input table
bq mk --table $PROJECT_ID:selfhealing.employee_data \
    id:INTEGER,name:STRING,department:STRING,salary:FLOAT || echo "Table already exists"

echo "✓ BigQuery datasets created"

# ============================================================
# STEP 6: CREATE CLOUD COMPOSER ENVIRONMENT
# ============================================================

echo ""
echo "=========================================="
echo "STEP 6: Creating Cloud Composer environment"
echo "=========================================="
echo "⚠ This step takes 20-30 minutes..."

gcloud composer environments create $COMPOSER_ENV \
    --location $REGION \
    --python-version 3 \
    --image-version composer-2-latest \
    --node-count 3 \
    --zone ${REGION}-a \
    --machine-type n1-standard-1 \
    --disk-size 30 || echo "Composer environment already exists"

echo "✓ Cloud Composer environment created"

# ============================================================
# STEP 7: INSTALL PYTHON DEPENDENCIES
# ============================================================

echo ""
echo "=========================================="
echo "STEP 7: Installing Python dependencies"
echo "=========================================="

gcloud composer environments update $COMPOSER_ENV \
    --location $REGION \
    --update-pypi-package google-generativeai>=0.3.0

echo "✓ Python packages installed"

# ============================================================
# STEP 8: GET COMPOSER BUCKET NAME
# ============================================================

echo ""
echo "=========================================="
echo "STEP 8: Getting Composer bucket"
echo "=========================================="

COMPOSER_BUCKET=$(gcloud composer environments describe $COMPOSER_ENV \
    --location $REGION \
    --format="get(config.dagGcsPrefix)" | sed 's|/dags||')

echo "✓ Composer bucket: $COMPOSER_BUCKET"

# ============================================================
# STEP 9: UPLOAD PIPELINE FILES
# ============================================================

echo ""
echo "=========================================="
echo "STEP 9: Uploading pipeline files"
echo "=========================================="

# Upload Spark script
gsutil cp scripts/transform_script.py gs://$BUCKET_NAME/scripts/

# Upload DAG
gsutil cp dags/self_healing_pipeline.py $COMPOSER_BUCKET/dags/

# Upload utils
gsutil -m cp utils/*.py $COMPOSER_BUCKET/dags/utils/

echo "✓ All files uploaded"

# ============================================================
# STEP 10: UPDATE DAG CONFIGURATION
# ============================================================

echo ""
echo "=========================================="
echo "STEP 10: Updating DAG configuration"
echo "=========================================="

# Create temporary file with updated configuration
cat > /tmp/dag_config.txt << EOF
PROJECT_ID = "$PROJECT_ID"
REGION = "$REGION"
CLUSTER_NAME = "$CLUSTER_NAME"
BUCKET_NAME = "$BUCKET_NAME"
EOF

echo "✓ Configuration updated"
echo ""
echo "⚠ IMPORTANT: Update the following variables in your DAG file:"
cat /tmp/dag_config.txt

# ============================================================
# STEP 11: LOAD SAMPLE DATA
# ============================================================

echo ""
echo "=========================================="
echo "STEP 11: Loading sample data (optional)"
echo "=========================================="

cat > /tmp/sample_data.json << 'EOF'
{"id": 1, "name": "John Doe", "department": "Engineering", "salary": 95000.0}
{"id": 2, "name": "Jane Smith", "department": "Marketing", "salary": 85000.0}
{"id": 3, "name": "Bob Johnson", "department": "Sales", "salary": 75000.0}
EOF

bq load --source_format=NEWLINE_DELIMITED_JSON \
    $PROJECT_ID:selfhealing.employee_data \
    /tmp/sample_data.json \
    id:INTEGER,name:STRING,department:STRING,salary:FLOAT

echo "✓ Sample data loaded"

# ============================================================
# SETUP COMPLETE
# ============================================================

echo ""
echo "=========================================="
echo "✓ SETUP COMPLETE!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Wait 2-3 minutes for Airflow to detect the new DAG"
echo "2. Access Airflow UI:"
echo "   gcloud composer environments run $COMPOSER_ENV --location $REGION web-server -- --help"
echo ""
echo "3. Trigger the DAG:"
echo "   gcloud composer environments run $COMPOSER_ENV \\"
echo "       --location $REGION \\"
echo "       dags trigger self_healing_pipeline"
echo ""
echo "Configuration Summary:"
echo "- Project ID: $PROJECT_ID"
echo "- Region: $REGION"
echo "- GCS Bucket: gs://$BUCKET_NAME"
echo "- Composer Environment: $COMPOSER_ENV"
echo "- Composer Bucket: $COMPOSER_BUCKET"
echo ""
