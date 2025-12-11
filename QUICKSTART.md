# Self-Healing Pipeline - Quick Start Guide

## Prerequisites

1. **GCP Account** with billing enabled
2. **gcloud CLI** installed and configured
3. **Project Owner** or **Editor** role
4. **Local files** from this repository

## One-Command Setup

### Step 1: Update Configuration

Edit `setup.sh` and update these variables:

```bash
PROJECT_ID="your-project-id"              # Your GCP project ID
BUCKET_NAME="your-unique-bucket-name"     # Must be globally unique
```

### Step 2: Run Setup Script

```bash
# Make script executable
chmod +x setup.sh

# Run setup (takes ~30 minutes due to Composer creation)
./setup.sh
```

### Step 3: Update DAG Configuration

After setup completes, update the DAG file with your project details:

```bash
# Edit the DAG file
nano dags/self_healing_pipeline.py

# Update these lines:
PROJECT_ID = "your-project-id"
BUCKET_NAME = "your-bucket-name"
```

Then re-upload:

```bash
# Get Composer bucket name from setup output
gsutil cp dags/self_healing_pipeline.py gs://YOUR_COMPOSER_BUCKET/dags/
```

### Step 4: Trigger DAG

```bash
gcloud composer environments run selfhealingpipeline \
    --location us-central1 \
    dags trigger self_healing_pipeline
```

---

## What the Setup Script Does

1. ✅ Enables 6 required APIs
2. ✅ Grants IAM roles to service accounts
3. ✅ Creates GCS bucket
4. ✅ Creates BigQuery datasets
5. ✅ Creates Cloud Composer environment (20-30 min)
6. ✅ Installs Python dependencies
7. ✅ Uploads all pipeline files
8. ✅ Loads sample data

---

## Manual Setup (Alternative)

If you prefer manual setup or the script fails, follow these steps:

### 1. Enable APIs

```bash
gcloud services enable dataproc.googleapis.com bigquery.googleapis.com \
    composer.googleapis.com storage.googleapis.com aiplatform.googleapis.com \
    generativelanguage.googleapis.com --project=YOUR_PROJECT_ID
```

### 2. Grant IAM Roles

```bash
# Get project number
PROJECT_NUMBER=$(gcloud projects describe YOUR_PROJECT_ID --format="value(projectNumber)")

# Grant roles
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member=serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com \
    --role=roles/storage.admin

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member=serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com \
    --role=roles/aiplatform.user
```

### 3. Create Resources

```bash
# GCS bucket
gsutil mb -l us-central1 gs://YOUR_BUCKET_NAME

# BigQuery datasets
bq mk --dataset YOUR_PROJECT_ID:selfhealing
bq mk --dataset YOUR_PROJECT_ID:output

# Composer environment (via GCP Console is easier)
```

### 4. Upload Files

```bash
# Upload scripts
gsutil cp scripts/transform_script.py gs://YOUR_BUCKET_NAME/scripts/

# Upload DAG and utils (get Composer bucket from Console)
gsutil cp dags/self_healing_pipeline.py gs://COMPOSER_BUCKET/dags/
gsutil -m cp utils/*.py gs://COMPOSER_BUCKET/dags/utils/
```

---

## Verification

### Check Setup Status

```bash
# Check APIs
gcloud services list --enabled | grep -E "dataproc|bigquery|composer|aiplatform"

# Check IAM
gcloud projects get-iam-policy YOUR_PROJECT_ID \
    --flatten="bindings[].members" \
    --filter="bindings.members:compute@developer.gserviceaccount.com"

# Check Composer
gcloud composer environments list --locations=us-central1

# Check files
gsutil ls gs://YOUR_BUCKET_NAME/scripts/
```

### Access Airflow UI

```bash
gcloud composer environments describe selfhealingpipeline \
    --location us-central1 \
    --format="get(config.airflowUri)"
```

---

## Troubleshooting

### Setup Script Fails

**Error: "Permission denied"**
- Ensure you have Owner or Editor role
- Run: `gcloud auth login` and `gcloud auth application-default login`

**Error: "Quota exceeded"**
- Check project quotas in GCP Console
- Request quota increase if needed

**Error: "Composer creation failed"**
- Create manually via GCP Console: Composer → Create Environment
- Use default settings, Python 3, Composer 2

### DAG Not Appearing

- Wait 2-3 minutes for Airflow to detect new DAG
- Check DAG file syntax: No errors in Airflow UI
- Verify files uploaded to correct Composer bucket

### DAG Fails

- Check Airflow logs in UI
- Verify BigQuery datasets exist
- Ensure sample data is loaded
- Check service account permissions

---

## Cost Estimate

**Monthly costs for occasional use:**
- Cloud Composer: ~$300/month (always running)
- Dataproc: ~$0.50/hour (only when DAG runs)
- BigQuery: Minimal (storage + queries)
- Vertex AI: ~$0.0005 per 1K characters

**Cost optimization:**
- Delete Composer when not in use
- Use smaller Composer nodes (n1-standard-1)
- Use preemptible Dataproc workers

---

## Next Steps

1. ✅ Run setup script
2. ✅ Verify all resources created
3. ✅ Trigger DAG
4. ✅ Monitor in Airflow UI
5. ✅ Test self-healing by introducing errors

For detailed configuration, see `GCP_CONFIGURATION.md`
