# BigQuery to ERD - gcloud CLI Setup Guide

## Using gcloud CLI Authentication (No Service Account Required)

This guide shows you how to use the BigQuery to ERD tool with gcloud CLI authentication instead of a service account.

## Prerequisites

1. **Install Google Cloud CLI**:
   ```bash
   # macOS
   brew install google-cloud-sdk
   
   # Or download from: https://cloud.google.com/sdk/docs/install
   ```

2. **Verify installation**:
   ```bash
   gcloud --version
   ```

## Setup Steps

### 1. Authenticate with Google Cloud

```bash
# Login to your Google account
gcloud auth login

# Set up Application Default Credentials (ADC)
gcloud auth application-default login

# Set your default project
gcloud config set project YOUR_PROJECT_ID
```

### 2. Configure the Tool

1. **Edit your `.env` file**:
   ```bash
   # Comment out or remove the service account line
   # GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
   
   # Set your project and dataset
   PROJECT_ID=your-actual-project-id
   DATASET_ID=your-actual-dataset-name
   ```

2. **Verify your setup**:
   ```bash
   # Test BigQuery access
   bq ls
   
   # Test the tool
   python test_installation.py
   ```

### 3. Run the Tool

```bash
# Basic usage
bigquery-to-erd

# With specific dataset
bigquery-to-erd --dataset-id your_dataset

# With custom output
bigquery-to-erd --dataset-id your_dataset --output-file my_erd.drawio --format drawio
```

## Authentication Methods Comparison

| Method | Pros | Cons | Best For |
|--------|------|------|----------|
| **gcloud CLI** | Easy setup, no key management | Requires user login | Development, personal use |
| **Service Account** | No user interaction, secure | Key management required | Production, CI/CD |

## Troubleshooting

### Common Issues

1. **"No credentials found"**:
   ```bash
   gcloud auth application-default login
   ```

2. **"Project not found"**:
   ```bash
   gcloud config set project YOUR_PROJECT_ID
   ```

3. **"Permission denied"**:
   - Ensure your account has BigQuery access
   - Check project permissions
   - Verify dataset exists

### Verify Authentication

```bash
# Check current account
gcloud auth list

# Check application default credentials
gcloud auth application-default print-access-token

# Test BigQuery access
bq query "SELECT 1"
```

## Security Notes

- **Application Default Credentials** are stored locally on your machine
- They're automatically used by Google Cloud client libraries
- No need to manage service account keys
- Credentials expire and need periodic refresh

## Next Steps

Once authenticated, you can:

1. **Generate your first ERD**:
   ```bash
   bigquery-to-erd --dataset-id your_dataset
   ```

2. **Try different formats**:
   ```bash
   # Mermaid format
   bigquery-to-erd --format mermaid --output-file schema.mmd
   
   # PlantUML format
   bigquery-to-erd --format plantuml --output-file schema.puml
   ```

3. **Include views and external tables**:
   ```bash
   bigquery-to-erd --include-views --include-external-tables
   ```

4. **Use custom relationship rules**:
   ```bash
   bigquery-to-erd --custom-rules examples/relationship_rules.json
   ```

## Need Help?

- Check the main [USAGE.md](docs/USAGE.md) for detailed usage instructions
- Run `bigquery-to-erd --help` for all available options
- Use `--verbose` flag for detailed logging
