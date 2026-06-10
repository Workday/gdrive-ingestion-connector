# `gdrive_to_udl.py` — Technical Reference

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python&logoColor=white)
![Google Drive API](https://img.shields.io/badge/Google%20Drive%20API-v3-4285F4?logo=googledrive&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-STS%20Assume%20Role-FF9900?logo=amazonaws&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Unity%20Catalog-FF3621?logo=databricks&logoColor=white)
![boto3](https://img.shields.io/badge/boto3-latest-FF9900?logo=amazonaws&logoColor=white)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)

**Author:** Shubhani Patil
**Module path:** `gdrive_to_dbx_data_ingestion/gdrive_framework/gdrive_to_udl.py`
**Last updated:** May 2026

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture & Design Decisions](#2-architecture--design-decisions)
3. [Dependencies](#3-dependencies)
4. [Configuration Constants](#4-configuration-constants)
5. [Google Drive API Usage](#5-google-drive-api-usage)
6. [Authentication](#6-authentication)
7. [Public API — Functions & Classes](#7-public-api--functions--classes)
8. [Internal (Private) Functions](#8-internal-private-functions)
9. [CLI Interface](#9-cli-interface)
10. [Supported Google-Native Export Formats](#10-supported-google-native-export-formats)
11. [Consumed Drive API Fields](#11-consumed-drive-api-fields)
12. [File Naming & Sanitization](#12-file-naming--sanitization)
13. [Error Handling Strategy](#13-error-handling-strategy)
14. [Pros](#14-pros)
15. [Security Considerations](#15-security-considerations)
16. [Usage Examples](#16-usage-examples)

---

## 1. Overview

`gdrive_to_udl.py` is a Python utility for extracting files from Google Drive (including Shared Drives) and writing them to either:

- An **AWS S3 bucket** (via STS assume-role), or
- A **Databricks Unity Catalog Volume** or any local filesystem path, or
- **Both simultaneously**.

It supports:
- Listing metadata of all accessible files and folders.
- Downloading individual files or entire folder trees (flat or hierarchy-preserving).
- Exporting Google-native formats (Sheets, Docs, Slides, Drawings, Forms, Scripts, Jamboard, Sites) to open formats.
- Programmatic use as a Python library (imported by `lcca_gdrive_to_udl.ipynb`).
- CLI use as a standalone script.

---

## 2. Architecture & Design Decisions

```
Caller (Notebook / Airflow / CLI)
           │
           ▼
  create_drive_service()     ──── OAuth2 / token refresh ──► Google Drive API v3
           │
           ├── list_metadata()                              → DataFrame (file metadata)
           ├── get_file_metadata()                          → DataFrame (single file)
           │
           └── download_and_write_output()
                    │
                    ├── _resolve_download_target()          → resolve shortcuts
                    ├── _download_google_native()           → files().export()
                    │       └── _resolve_export()           → format resolution
                    ├── _download_binary()                  → files().get_media()
                    │
                    ├── S3 write (s3_client.put_object)     → AWS S3
                    └── Volume write (open + write)         → Databricks / local
```

**Key design principles:**

| Principle | Implementation |
|---|---|
| No local temp files for S3 path | Bytes streamed in-memory via `io.BytesIO` |
| No static AWS keys | STS assume-role is mandatory |
| No fallback export for unknown types | Logs error and continues — no silent corruption |
| Shortcut transparency | Shortcuts resolved to real targets before any download |
| Paged API calls | All `files().list()` responses are paginated via `_page_all()` |

---

## 3. Dependencies

| Package | Purpose |
|---|---|
| `google-api-python-client` | Google Drive REST API client (`googleapiclient`) |
| `google-auth` / `oauth2client` | OAuth2 credential management + token refresh |
| `httplib2` | HTTP transport layer for credential refresh |
| `boto3` | AWS SDK for S3 uploads and STS assume-role |
| `pandas` | DataFrame construction for metadata results |
| `argparse` | CLI argument parsing |
| `io`, `os`, `json`, `sys`, `logging` | Python standard library |

**Install:**
```bash
pip install google-api-python-client oauth2client boto3 pandas httplib2
```

---

## 4. Configuration Constants

These module-level constants control API behaviour and are not user-configurable at runtime.

| Constant | Value | Purpose |
|---|---|---|
| `API_NAME` | `"drive"` | Google API name |
| `API_VERSION` | `"v3"` | Drive API version |
| `SCOPES` | `["https://www.googleapis.com/auth/drive.readonly"]` | Read-only OAuth scope |
| `JOB_NAME` | `"Gdrive_to_UDL"` | Prefix in all log messages |
| `MIME_TYPE_FOLDER` | `application/vnd.google-apps.folder` | Used to detect folders in API responses |
| `MIME_TYPE_SHORTCUT` | `application/vnd.google-apps.shortcut` | Used to detect and resolve shortcuts |
| `MIME_TYPE_PDF` | `application/pdf` | Reused in export maps |
| `FILE_METADATA_FIELDS` | `"id, name, mimeType, size"` | Fields fetched in single-file metadata calls |
| `DRIVE_LIST_FIELDS` | `"nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime, parents, owners)"` | Fields fetched in folder/list calls |
| `GOOGLE_NATIVE_EXPORT_TYPES` | Dict of dicts | Export MIME map per Google-native type |
| `DEFAULT_EXPORT_FORMAT` | Dict | Default export extension per Google-native type |

---

## 5. Google Drive API Usage

### 5.1 API Calls Made

| Method | Purpose | Key Parameters |
|---|---|---|
| `files().get(fileId, fields, supportsAllDrives)` | Fetch metadata for a single file or folder | `supportsAllDrives=True` required for Shared Drives |
| `files().list(q, pageSize, fields, supportsAllDrives, includeItemsFromAllDrives, corpora)` | List files in a folder or all accessible files | `corpora="allDrives"` covers Shared Drives |
| `files().export(fileId, mimeType)` | Export Google-native files (Docs, Sheets, Slides, etc.) | `mimeType` = export target MIME |
| `files().get_media(fileId)` | Download binary (non-native) files as-is | No `mimeType` needed |

### 5.2 Pagination

All `files().list()` calls are wrapped in `_page_all()`:
- Iterates `nextPageToken` until exhausted.
- Returns a flat list of all matching file objects.
- No hard cap — use `max_files` argument in recursive functions if needed.

### 5.3 Shared Drive Support

Every API call sets:
- `supportsAllDrives=True` — allows access to items in Shared Drives.
- `includeItemsFromAllDrives=True` — includes files from all drives accessible by the credential.
- `corpora="allDrives"` — searches across My Drive and all Shared Drives.

### 5.4 Query Syntax

All folder-scoped listing uses:
```
'{folder_id}' in parents and trashed=false
```
This returns only direct children of the folder, not deep descendants. Recursion is handled in Python.

---

## 6. Authentication

### 6.1 OAuth2 Credential Contract

Two credential JSON formats are supported:

**Preferred — Minimal Contract:**
```json
{
  "client_id":     "...",
  "client_secret": "...",
  "refresh_token": "...",
  "token_uri":     "https://oauth2.googleapis.com/token",
  "access_token":  "..."   // optional
}
```

**Backward-compatible — Full oauth2client serialized JSON** (includes `_class`, `_module` keys).

### 6.2 Token Refresh

`create_drive_service()` calls `creds.refresh(Http())` when:
- `access_token_expired` is `True`, or
- `access_token` is absent.

Token refresh is in-memory only — no credential file is written to disk.

### 6.3 `create_drive_service(credentials_json: str) → service`

| Argument | Type | Description |
|---|---|---|
| `credentials_json` | `str` | Serialized OAuth JSON (see §6.1) |

**Returns:** Google Drive `Resource` object (authenticated service).

**Exits with `sys.exit(1)` on:**
- Unparseable JSON
- Missing required fields (`client_id`, `client_secret`, `refresh_token`, `token_uri`)
- Connection failure

---

## 7. Public API — Functions & Classes

### 7.1 `create_drive_service(credentials_json)`

See §6.3.

---

### 7.2 `list_metadata(service, folder_id=None) → pd.DataFrame`

Lists metadata for accessible files.

| Argument | Type | Default | Description |
|---|---|---|---|
| `service` | Resource | — | Authenticated Drive service |
| `folder_id` | `str` | `None` | If provided, recursively lists all files under this folder |

**Returns:** DataFrame with columns:

| Column | Source |
|---|---|
| `file_id` | `id` |
| `name` | `name` |
| `mime_type` | `mimeType` |
| `size_bytes` | `size` (or `"N/A"` for Google-native) |
| `created_time` | `createdTime` |
| `modified_time` | `modifiedTime` |
| `parent_id` | `parents[0]` (or `"N/A"`) |
| `owners` | `owners[].emailAddress` comma-joined |

**Behaviour:**
- Without `folder_id`: lists ALL files accessible by the credential (`trashed=false`).
- With `folder_id`: uses `_list_all_files_recursive()` — breadth-first traversal.

**Limitation:** When called without `folder_id`, this returns every file the credential can see. For a service account with broad Shared Drive access this may be thousands of rows.

---

### 7.3 `get_file_metadata(service, file_id) → pd.DataFrame`

Returns a single-row DataFrame for one file ID.

| Argument | Type | Description |
|---|---|---|
| `service` | Resource | Authenticated Drive service |
| `file_id` | `str` | Google Drive file ID |

**Returns:** Same column structure as `list_metadata`. Exits on API error.

---

### 7.4 `download_and_write_output(service, file_id, file_name, file_type, mime_type, s3_client, bucket, s3_prefix, volume_path, return_details) → bool | dict`

Core download function. Downloads one file and writes to S3 and/or volume path.

| Argument | Type | Default | Description |
|---|---|---|---|
| `service` | Resource | — | Authenticated Drive service |
| `file_id` | `str` | — | Drive file ID |
| `file_name` | `str` | — | Display name (used in logging and output path) |
| `file_type` | `str` | `None` | Override export extension (e.g. `"csv"`, `"pdf"`) |
| `mime_type` | `str` | `""` | MIME type from Drive metadata |
| `s3_client` | boto3 S3 client | `None` | Required if writing to S3 |
| `bucket` | `str` | `None` | S3 bucket name |
| `s3_prefix` | `str` | `None` | S3 key prefix |
| `volume_path` | `str` | `None` | Databricks volume or local directory path |
| `return_details` | `bool` | `False` | Return dict instead of bool |

**Return when `return_details=False`:** `True` (success) / `False` (failure).

**Return when `return_details=True`:**
```python
{
    "success":           bool,
    "final_file_name":   str,    # actual filename written (may differ from file_name)
    "chosen_file_format": str,   # extension used (e.g. "xlsx", "pdf")
    "error_code":        int,    # HTTP status code (or None)
    "error_message":     str     # human-readable error
}
```

**Internal flow:**
1. Resolve shortcuts → `_resolve_download_target()`
2. If Google-native MIME → `_download_google_native()` (uses `files().export()`)
3. Otherwise → `_download_binary()` (uses `files().get_media()`)
4. Write to S3 if `s3_client` + `bucket` + `s3_prefix` all provided
5. Write to volume if `volume_path` provided
6. Both S3 and volume can be written in one call

**Export size fallback:** If a Google-native file exceeds Drive's export size limit (`exportSizeLimitExceeded`), the function automatically retries as PDF when the original format was not PDF. If PDF also fails or wasn't an option, the file is marked as failed.

---

### 7.5 `create_s3_client(args) → boto3 S3 client`

Creates a temporary S3 client using STS assume-role.

| `args` attribute | Description |
|---|---|
| `args.s3_role_arn` | **Required.** IAM Role ARN. No static keys supported. |
| `args.s3_role_session_name` | Session name (default: `"GDriveToS3Session"`) |
| `args.s3_role_duration_seconds` | Session TTL in seconds (default: 3600) |

**Exits with `sys.exit(1)` if `s3_role_arn` is absent or STS call fails.**

---

## 8. Internal (Private) Functions

These are not part of the public API but are documented here for maintainability.

| Function | Purpose |
|---|---|
| `_log(msg, error)` | Thin wrapper over `logging.info` / `logging.error` with `JOB_NAME` prefix |
| `_build_credentials_from_minimal_json(data)` | Builds `OAuth2Credentials` from minimal JSON contract |
| `_load_session_credentials(credentials_json)` | Parses and validates credential JSON; tries minimal path first, falls back to full oauth2client JSON |
| `_identify_id_type(service, id_value)` | Calls `files().get(fields="mimeType")` to determine if an ID is a file or folder |
| `_page_all(list_fn, **kwargs)` | Generic paginator; iterates `nextPageToken` until exhausted |
| `_process_folder_items(items, queue, all_files, max_files)` | Classifies items into subfolders (queue) vs files; respects `max_files` cap |
| `_list_all_files_recursive(service, folder_id, max_files)` | Breadth-first recursive listing (no path tracking) |
| `_list_all_files_recursive_with_paths(service, folder_id, current_path, max_files)` | Depth-first recursive listing; attaches `relative_folder_path` to each file dict |
| `_resolve_export(mime_type, file_type)` | Returns `(extension, export_mime)` for Google-native type; returns `(None, None)` if unknown |
| `_is_export_size_limit_error(exc)` | Detects `exportSizeLimitExceeded` error string |
| `_extract_error_details(exc)` | Extracts `(error_code, error_message)` from `HttpError` or generic exceptions |
| `_sanitize_output_filename(file_name)` | Replaces `/`, `\`, and `<>:"\|?*` with `_`; prevents empty filenames |
| `_resolve_shortcut_target(service, shortcut_id, shortcut_name)` | Resolves Drive shortcut to target file metadata (handles `resourceKey`) |
| `_make_result(success, ...)` | Builds `bool` or `dict` result depending on `return_details` |
| `_download_request_to_bytes(request, file_name)` | Executes media/export request and streams payload into `io.BytesIO`; logs progress per chunk |
| `_resolve_download_target(service, file_id, file_name, mime_type)` | Normalises file vs shortcut; returns `(file_id, file_name, mime_type, resource_key)` or `None` |
| `_download_google_native(service, file_id, file_name, mime_type, file_type, resource_key)` | Exports Google-native file; handles size-limit fallback to PDF |
| `_download_binary(service, file_id, file_name, mime_type, resource_key)` | Downloads regular binary file with `get_media()` |
| `_resolve_requested_ids(args, require_at_least_one)` | Validates `--ids` argument; exits if required but missing |
| `_validate_download_destinations(args)` | Validates S3 and/or volume args; exits if neither provided |
| `_collect_files_to_process(service, file_id, id_type)` | Returns list of file dicts for a single file or recursive folder |
| `_resolve_top_level_files(service, current_id)` | Fetches top-level metadata; delegates to recursive list if folder |
| `_append_download_result(result, ...)` | Appends result row to `success_records` or `failure_records` |
| `_resolve_hierarchy_top_level_files(service, current_id)` | Same as `_resolve_top_level_files` but uses `_list_all_files_recursive_with_paths()` |
| `_append_hierarchy_download_result(result, ...)` | Like `_append_download_result` but includes `relative_folder_path` |
| `_process_hierarchy_id(service, s3, args, current_id, ...)` | Orchestrates one ID in hierarchy mode; builds sub-directory paths under `volume_path` |

---

## 9. CLI Interface

### 9.1 Sub-commands

| Sub-command | Function | Description |
|---|---|---|
| `list` | `action_list()` | List metadata for all or specified IDs |
| `download` | `action_download()` | Download files flat (no folder hierarchy) |
| `download-hierarchy` | `action_download_with_hierarchy()` | Download preserving Drive folder structure |

### 9.2 Shared Arguments

| Argument | Required | Description |
|---|---|---|
| `--credentials_json` | Yes | Serialized OAuth2 JSON string |
| `--ids` | No (list), Yes (download) | One or more Drive file or folder IDs |

### 9.3 Download / Download-Hierarchy Arguments

| Argument | Required | Default | Description |
|---|---|---|---|
| `--s3_bucket` | If S3 | — | S3 bucket name |
| `--s3_prefix` | If S3 | — | S3 key prefix |
| `--s3_role_arn` | If S3 | — | IAM Role ARN (mandatory for S3) |
| `--volume_path` | No | `None` | Databricks Volume or local path |
| `--s3_role_session_name` | No | `GDriveToS3Session` | STS session name |
| `--s3_role_duration_seconds` | No | `3600` | STS session TTL |
| `--file_type` | No | `None` | Override export format for Google-native files |

### 9.4 Download vs Download-Hierarchy Comparison

| Aspect | `download` | `download-hierarchy` |
|---|---|---|
| Folder structure preserved | No — all files land flat in `volume_path` | Yes — subfolders mirrored |
| Relative path in result | Not included | `relative_folder_path` column included |
| S3 prefix expansion | No | No (prefix unchanged; only volume path expanded) |
| Single file IDs | Written to `volume_path` directly | Same |
| Internal listing function | `_list_all_files_recursive` | `_list_all_files_recursive_with_paths` |

---

## 10. Supported Google-Native Export Formats

| Google Type | Drive MIME | Default Format | All Supported Formats |
|---|---|---|---|
| Google Sheets | `application/vnd.google-apps.spreadsheet` | `xlsx` | xlsx, csv, pdf, ods, tsv, html |
| Google Docs | `application/vnd.google-apps.document` | `docx` | docx, pdf, txt, odt, rtf, html, epub, zip |
| Google Slides | `application/vnd.google-apps.presentation` | `pptx` | pptx, pdf, odp, txt |
| Google Drawings | `application/vnd.google-apps.drawing` | `png` | png, svg, pdf, jpg |
| Google Forms | `application/vnd.google-apps.form` | `zip` | zip |
| Google Apps Script | `application/vnd.google-apps.script` | `json` | json |
| Google Jamboard | `application/vnd.google-apps.jam` | `pdf` | pdf |
| Google Sites | `application/vnd.google-apps.site` | `txt` | txt |

**If a Google-native file has an unrecognised MIME type**, the module logs an error and skips the file — no silent fallback.

**Export size limit (`exportSizeLimitExceeded`):** Automatically retried as `pdf` if the original format was not `pdf`. If PDF also fails or is not available, the file is recorded in `failure_records` with `error_code=403`.

---

## 11. Consumed Drive API Fields

| API Field | Mapped to | Notes |
|---|---|---|
| `id` | `file_id` | Stable unique identifier |
| `name` | `name` | Display name |
| `mimeType` | `mime_type` | Used to route download strategy |
| `size` | `size_bytes` | Absent for Google-native files (shown as `"N/A"`) |
| `createdTime` | `created_time` | ISO 8601 UTC |
| `modifiedTime` | `modified_time` | ISO 8601 UTC — used as watermark in ingestion notebooks |
| `parents[0]` | `parent_id` | First parent only; Drive supports multiple parents |
| `owners[].emailAddress` | `owners` | Comma-joined string |
| `shortcutDetails.targetId` | — | Used in shortcut resolution |
| `shortcutDetails.targetResourceKey` | — | Required for some cross-drive shortcuts |

---

## 12. File Naming & Sanitization

All output filenames pass through `_sanitize_output_filename(file_name)` before any write:

| Rule | Reason |
|---|---|
| Replace `/` and `\` with `_` | Prevent unintended path nesting |
| Replace `<>:"\|?*` with `_` | Invalid filesystem characters on Windows/Linux |
| Strip leading/trailing whitespace and `.` | Prevent hidden file creation |
| Return `"unnamed_file"` if result is empty | Hard guard against empty filename |

Google-native files get the export extension appended: e.g. `My Report.docx`.

---

## 13. Error Handling Strategy

| Scenario | Behaviour |
|---|---|
| Bad credentials JSON | `sys.exit(1)` |
| Drive API auth failure | `sys.exit(1)` |
| Single file download failure | Logged + added to `failure_records`; loop continues |
| Unknown Google-native MIME | Logged as error; file skipped; loop continues |
| Export size limit exceeded | Automatic PDF fallback; failure if fallback also fails |
| Shortcut target not resolvable | Logged + file skipped; loop continues |
| S3 ARN not provided | `sys.exit(1)` |
| No S3 or volume destination provided | `sys.exit(1)` |
| Top-level ID metadata failure (action_download_with_hierarchy) | Full ID recorded as failure; loop continues |

**Logging format:**
```
2026-05-18 12:34:56 INFO: Gdrive_to_UDL Job ===> <message>
2026-05-18 12:34:56 ERROR: Gdrive_to_UDL Job ===> <message>
```

---

## 14. Pros

### Pros

| Area | Detail |
|---|---|
| Zero temp files on S3 path | Bytes are streamed entirely in memory (`io.BytesIO`); no disk I/O for S3 uploads |
| Dual destination support | S3 and volume can be written in a single `download_and_write_output()` call |
| Shared Drive support | All API calls include `supportsAllDrives=True` and `corpora="allDrives"` |
| Shortcut transparency | Shortcuts are silently resolved to their real targets — callers see the real file |
| Export size fallback | Automatic PDF retry reduces silent failures for large Google-native files |
| Read-only scope | `drive.readonly` scope minimises blast radius of credential compromise |
| Programmatic + CLI | Same functions usable from notebooks (imported) and from shell (CLI) |
| Per-file error isolation | One failed download does not abort the batch |
| No static AWS credentials | STS assume-role is enforced; static keys are explicitly rejected |

---

## 15. Security Considerations

| Control | Implementation |
|---|---|
| Least-privilege OAuth scope | `drive.readonly` — no write access to Drive |
| No credential persistence | Token refresh is in-memory; no file written to disk |
| STS assume-role only | Static AWS access keys are explicitly rejected with `sys.exit(1)` |
| Filename sanitization | Prevents path-traversal attacks via crafted Drive file names |
| No SQL in this module | All data operations are file I/O only |
| `cache_discovery=False` | Drive API discovery cache disabled — avoids stale service definitions |

**Recommendation for production:** Store `credentials_json` in a secrets manager (e.g. Databricks Secrets, AWS Secrets Manager) and inject at runtime. Never log or print the credential JSON.

---

## 16. Usage Examples

### 16.1 As a Python Library (Notebook / Airflow)

```python
import importlib.util, sys, os

# Load module from volume path
spec = importlib.util.spec_from_file_location(
    "gdrive_to_udl",
    "/Volumes/udl_raw_dev/file_extracts/.../gdrive_to_udl.py"
)
gdrive_framework = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gdrive_framework)

# Authenticate
credentials_json = dbutils.secrets.get(scope="gdrive", key="oauth_credentials")
service = gdrive_framework.create_drive_service(credentials_json)

# List metadata for a Shared Drive folder
df = gdrive_framework.list_metadata(service, folder_id="<folder_id>")

# Download one file to a Databricks Volume (with result details)
result = gdrive_framework.download_and_write_output(
    service=service,
    file_id="<file_id>",
    file_name="Q4 Report",
    mime_type="application/vnd.google-apps.spreadsheet",
    file_type="xlsx",           # optional override
    volume_path="/Volumes/udl_raw_dev/.../gdrive_lcca",
    return_details=True,
)
# result = {"success": True, "final_file_name": "Q4 Report.xlsx", "chosen_file_format": "xlsx", ...}
```

### 16.2 CLI — List All Files

```bash
python gdrive_to_udl.py list \
  --credentials_json '{"client_id":"...","client_secret":"...","refresh_token":"...","token_uri":"https://oauth2.googleapis.com/token"}'
```

### 16.3 CLI — Download Folder Flat to Volume

```bash
python gdrive_to_udl.py download \
  --credentials_json '<credentials_json>' \
  --ids <folder_id> \
  --volume_path /Volumes/udl_raw_dev/file_extracts/inbound/gdrive_lcca \
  --file_type pdf
```

### 16.4 CLI — Download Folder Preserving Hierarchy

```bash
python gdrive_to_udl.py download-hierarchy \
  --credentials_json '<credentials_json>' \
  --ids <folder_id> \
  --volume_path /Volumes/udl_raw_dev/file_extracts/inbound/gdrive_lcca
```

### 16.5 CLI — Download to Both S3 and Volume

```bash
python gdrive_to_udl.py download \
  --credentials_json '<credentials_json>' \
  --ids <folder_id_1> <folder_id_2> \
  --s3_bucket my-raw-bucket \
  --s3_prefix raw/gdrive/lcca \
  --s3_role_arn arn:aws:iam::<account_number>:role/GDriveIngestRole \
  --volume_path /Volumes/udl_raw_dev/file_extracts/inbound/gdrive_lcca
```

### 16.6 Incremental Ingestion Pattern (used in `lcca_gdrive_to_udl.ipynb`)

```python
# 1. List all files with paths for a Shared Drive folder
files = gdrive_framework._list_all_files_recursive_with_paths(
    service, folder_id="<folder_id>", current_path="Drive Name"
)
# Each entry has: id, name, mimeType, modifiedTime, relative_folder_path

# 2. Filter to files modified after watermark
incremental = [f for f in files if f["modifiedTime"] > last_watermark]

# 3. Download each incremental file
for f in incremental:
    result = gdrive_framework.download_and_write_output(
        service=service,
        file_id=f["id"],
        file_name=f["name"],
        mime_type=f["mimeType"],
        volume_path=os.path.join(raw_base, f["relative_folder_path"]),
        return_details=True,
    )
```

---

*Generated from source: `gdrive_to_dbx_data_ingestion/gdrive_framework/gdrive_to_udl.py` — May 2026*
