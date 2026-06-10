# Contributing

Thank you for your interest in contributing to gdrive_data_ingestion_framework.

## Code of Conduct

By participating in this project, you agree to be respectful and constructive in all interactions.

## Before You Start

1. Read the project documentation in `gdrive_to_udl_documentation.md`.
2. Check existing issues and pull requests to avoid duplicate work.
3. Open an issue for major changes before implementation.

## Local Testing Setup (gdrive_to_udl.py)

Use this setup to run and validate changes from your laptop or local VM.

### Prerequisites

- Python 3.8+
- Access to Google Drive OAuth credentials (client_id, client_secret, refresh_token, token_uri)
- Optional: AWS access to assume the target role for S3 testing

### 1) Create and activate a virtual environment

```bash
cd gdrive_to_dbx_data_ingestion/gdrive_framework
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

### 2) Install dependencies

```bash
pip install google-api-python-client oauth2client boto3 pandas httplib2
```

### 3) Set OAuth credentials JSON

Create a JSON value with this minimum contract:

```json
{
   "client_id": "...",
   "client_secret": "...",
   "refresh_token": "...",
   "token_uri": "https://oauth2.googleapis.com/token"
}
```

Export as an environment variable:

```bash
export GDRIVE_CREDS_JSON='{"client_id":"...","client_secret":"...","refresh_token":"...","token_uri":"https://oauth2.googleapis.com/token"}'
```

### 4) Smoke test metadata listing

```bash
python gdrive_to_udl.py list \
   --credentials_json "$GDRIVE_CREDS_JSON"
```

Folder-scoped recursive listing:

```bash
python gdrive_to_udl.py list \
   --credentials_json "$GDRIVE_CREDS_JSON" \
   --ids <folder_id>
```

### 5) Smoke test download to local path

```bash
python gdrive_to_udl.py download \
   --credentials_json "$GDRIVE_CREDS_JSON" \
   --ids <file_or_folder_id> \
   --volume_path ./tmp_downloads

# Optional: force an export format for Google-native files
# --file_type csv
```

### 6) Optional smoke test download to S3

```bash
python gdrive_to_udl.py download \
   --credentials_json "$GDRIVE_CREDS_JSON" \
   --ids <file_or_folder_id> \
   --s3_bucket <bucket_name> \
   --s3_prefix <prefix> \
   --s3_role_arn arn:aws:iam::<account_id>:role/<role_name>

# Optional: force an export format for Google-native files
# --file_type csv
```

## Databricks Testing Setup (gdrive_to_udl.py)

Use this setup when validating framework behavior in Databricks jobs or notebooks.

### 1) Cluster/runtime prerequisites

- DBR with Python 3.8+
- Access to a Unity Catalog Volume for output testing
- Secret scope configured for OAuth JSON and (optionally) AWS role ARN

### 2) Install required libraries on cluster

In a notebook cell:

```python
%pip install google-api-python-client oauth2client boto3 pandas httplib2
```

Restart Python after install if prompted.

### 3) Load credentials from Databricks secrets

```python
credentials_json = dbutils.secrets.get(scope="<scope>", key="gdrive-creds-json")
```

### 4) Import and run framework functions

```python
import sys
sys.path.append("/Workspace/Repos/<user-or-org>/<repo>/gdrive_to_dbx_data_ingestion/gdrive_framework")

from gdrive_to_udl import create_drive_service, list_metadata

service = create_drive_service(credentials_json)
df = list_metadata(service, folder_id="<folder_id>")
display(df.head())
```

### 5) Validate download path behavior

- Volume/local output: pass a valid `/Volumes/...` path and verify file creation.
- S3 output: provide a valid assume-role ARN and verify object creation in target bucket/prefix.

### 6) Recommended Databricks validation checklist

- Metadata listing works for both file and folder IDs.
- Shared Drive access works (`supportsAllDrives=True` paths).
- Google-native exports use expected output formats.
- Shortcut targets resolve correctly.
- Errors are logged clearly and processing continues where designed.

## Credentials and Security

- Never commit OAuth JSON, tokens, or AWS secrets.
- Use environment variables locally and secret scopes in Databricks.
- Use role assumption for S3 access; avoid static long-lived AWS credentials.

## How to Contribute

1. Fork the repository and create a feature branch.
2. Make focused changes with clear commit messages.
3. Add or update tests and documentation where relevant.
4. Ensure code quality checks pass locally.
5. Submit a pull request with a clear summary of:
   - What changed
   - Why it changed
   - How it was tested

## Pull Request Guidelines

- Keep pull requests small and reviewable.
- Reference related issue IDs in the PR description.
- Include sample commands or output when behavior changes.
- Update docs for any user-facing or configuration changes.

## Reporting Bugs

When filing a bug, please include:

- Environment details (Databricks runtime, Python version, cloud target)
- Reproduction steps
- Expected vs actual behavior
- Relevant logs or stack traces (without secrets)

## Security Reporting

Do not open public issues for potential security vulnerabilities.
Report security concerns privately to the maintainers through your internal security process.

## License

By submitting a contribution, you agree that your contributions are licensed under the project license described in LICENSE.md
