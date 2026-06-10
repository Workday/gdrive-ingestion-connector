#!/usr/bin/env python3
"""
gdrive_to_volume_or_s3.py

Utility to:
  1. List metadata of files/folders accessible by a Google Drive user.
        2. Download specific file(s) or all files in folder(s) (using --ids)
         and write to S3 and/or volume/local path (no local temp files unless
         volume/local output is selected).

Key Features:
    - Direct download from Google Drive → S3 and/or volume/local path
  - STS assume-role for S3 access (role ARN required; static keys not supported)
  - Recursive folder traversal (lists all files in subfolders)
    - Console logging for progress and errors
  - No fallback export formats for unknown Google-native types (log error, continue)

Usage examples
--------------
    Airflow variable / runtime credentials:
        CREDENTIALS_JSON: minimal JSON (recommended)
            {
                "client_id": "...",
                "client_secret": "...",
                "refresh_token": "...",
                "token_uri": "https://oauth2.googleapis.com/token",
                "access_token": "..."   # optional
            }
            (full oauth2client serialized JSON is also accepted)

  List all accessible files/folders:
    python gdrive_to_volume_or_s3.py list \\
            --credentials_json '<serialized_oauth_credentials_json>'

  List files in a folder (recursive, auto-detects ID type):
    python gdrive_to_volume_or_s3.py list \\
            --credentials_json '<serialized_oauth_credentials_json>' \
    --ids <folder_id>

  Download a file or folder by id → S3 (auto-detects file vs folder, recursive):
    python gdrive_to_volume_or_s3.py download \\
            --credentials_json '<serialized_oauth_credentials_json>' \
    --ids <file_or_folder_id> \\
      --s3_bucket my-raw-bucket --s3_prefix raw/gdrive \\
      --s3_role_arn arn:aws:iam::<acct-id>:role/<role-name> \\
      --file_type csv

    Download multiple files by IDs → Databricks volume/local path:
        python gdrive_to_volume_or_s3.py download \\
                        --credentials_json '<serialized_oauth_credentials_json>' \
            --ids <file_id_1> <file_id_2> <file_id_3> \\
            --volume_path /Volumes/<catalog>/<schema>/<volume>/gdrive \\
            --file_type csv

    Download a file or folder by id → Databricks volume/local path:
        python gdrive_to_volume_or_s3.py download \\
                        --credentials_json '<serialized_oauth_credentials_json>' \
            --ids <file_or_folder_id> \\
            --volume_path /Volumes/<catalog>/<schema>/<volume>/gdrive \\
            --file_type csv

S3 Path Structure (automatic):
    Files:       s3://{bucket}/{prefix}/{files}

Supported Google-native types and their default export formats
--------------------------------------------------------------
  Google Sheets      → xlsx  (also: csv, pdf, ods, tsv, html)
  Google Docs        → docx  (also: pdf, txt, odt, rtf, html, epub, zip)
  Google Slides      → pptx  (also: pdf, odp, txt)
  Google Drawings    → png   (also: svg, pdf, jpg)
  Google Forms       → zip
  Google Apps Script → json
  Google Jamboard    → pdf
  Google Sites       → txt
  Regular files      → downloaded as-is (pdf, png, mp4, csv, zip, etc.)
"""

__author__ = "Shubhani Patil"

import argparse
import io
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import boto3
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from httplib2 import Http
from oauth2client import client

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

JOB_NAME = "gdrive_to_volume_or_s3"

# Google Drive API constants
API_NAME = "drive"
API_VERSION = "v3"
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# MIME type constants (to reduce duplication)
MIME_TYPE_PDF = "application/pdf"
MIME_TYPE_TEXT_PLAIN = "text/plain"
MIME_TYPE_HTML = "text/html"
MIME_TYPE_FOLDER = "application/vnd.google-apps.folder"
MIME_TYPE_SHORTCUT = "application/vnd.google-apps.shortcut"

# MIME types used when exporting Google-native files.
# Key   = Google-native mimeType from Drive API
# Value = dict of { output_extension: export_mimeType }
GOOGLE_NATIVE_EXPORT_TYPES = {
    # ---- Google Sheets ----
    "application/vnd.google-apps.spreadsheet": {
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "csv":  "text/csv",
        "pdf":  MIME_TYPE_PDF,
        "ods":  "application/x-vnd.oasis.opendocument.spreadsheet",
        "tsv":  "text/tab-separated-values",
        "html": MIME_TYPE_HTML,
    },
    # ---- Google Docs ----
    "application/vnd.google-apps.document": {
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "pdf":  MIME_TYPE_PDF,
        "txt":  MIME_TYPE_TEXT_PLAIN,
        "odt":  "application/vnd.oasis.opendocument.text",
        "rtf":  "application/rtf",
        "html": MIME_TYPE_HTML,
        "epub": "application/epub+zip",
        "zip":  "application/zip",   # HTML zipped
    },
    # ---- Google Slides ----
    "application/vnd.google-apps.presentation": {
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "pdf":  MIME_TYPE_PDF,
        "odp":  "application/vnd.oasis.opendocument.presentation",
        "txt":  MIME_TYPE_TEXT_PLAIN,
    },
    # ---- Google Drawings ----
    "application/vnd.google-apps.drawing": {
        "png":  "image/png",
        "svg":  "image/svg+xml",
        "pdf":  MIME_TYPE_PDF,
        "jpg":  "image/jpeg",
    },
    # ---- Google Forms ----
    "application/vnd.google-apps.form": {
        "zip":  "application/zip",
    },
    # ---- Google Apps Script ----
    "application/vnd.google-apps.script": {
        "json": "application/vnd.google-apps.script+json",
    },
    # ---- Google Jamboard ----
    "application/vnd.google-apps.jam": {
        "pdf":  MIME_TYPE_PDF,
    },
    # ---- Google Sites ----
    "application/vnd.google-apps.site": {
        "txt":  MIME_TYPE_TEXT_PLAIN,
    },
}

# Default export format per Google-native type (used when --file_type is not supplied)
DEFAULT_EXPORT_FORMAT = {
    "application/vnd.google-apps.spreadsheet":  "xlsx",
    "application/vnd.google-apps.document":     "docx",
    "application/vnd.google-apps.presentation": "pptx",
    "application/vnd.google-apps.drawing":      "png",
    "application/vnd.google-apps.form":         "zip",
    "application/vnd.google-apps.script":       "json",
    "application/vnd.google-apps.jam":          "pdf",
    "application/vnd.google-apps.site":         "txt",
}

FILE_METADATA_FIELDS = "id, name, mimeType, size"
DRIVE_LIST_FIELDS = (
    "nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime, parents, owners)"
)
PANDAS_PRINT_OPTIONS_DEFAULT = (
    "display.max_rows", None,
    "display.max_columns", None,
    "display.width", 0,
    "display.max_colwidth", 90,
)
PANDAS_PRINT_OPTIONS_WIDE = (
    "display.max_rows", None,
    "display.max_columns", None,
    "display.width", 0,
    "display.max_colwidth", 120,
)


# ---------------------------------------------------------------------------
# Helper: logging
# ---------------------------------------------------------------------------
def _log(msg, error=False):
    if msg is not None:
        if error:
            logging.error("%s Job ===> %s", JOB_NAME, msg)
        else:
            logging.info("%s Job ===> %s", JOB_NAME, msg)


# ---------------------------------------------------------------------------
# Google Drive authentication
# ---------------------------------------------------------------------------
def _build_credentials_from_minimal_json(credentials_data: dict):
    """Build OAuth2Credentials from a minimal JSON contract."""
    required_fields = ["client_id", "client_secret", "refresh_token", "token_uri"]
    missing = [field for field in required_fields if not credentials_data.get(field)]
    if missing:
        return None

    token_expiry = None
    raw_expiry = credentials_data.get("token_expiry")
    if raw_expiry:
        try:
            token_expiry = datetime.strptime(raw_expiry, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            token_expiry = None

    return client.OAuth2Credentials(
        access_token=credentials_data.get("access_token"),
        client_id=credentials_data["client_id"],
        client_secret=credentials_data["client_secret"],
        refresh_token=credentials_data["refresh_token"],
        token_expiry=token_expiry,
        token_uri=credentials_data["token_uri"],
        user_agent=credentials_data.get("user_agent"),
        revoke_uri=credentials_data.get("revoke_uri"),
        id_token=credentials_data.get("id_token"),
        token_response=credentials_data.get("token_response"),
        scopes=credentials_data.get("scopes"),
        token_info_uri=credentials_data.get("token_info_uri"),
        id_token_jwt=credentials_data.get("id_token_jwt"),
    )


def _load_session_credentials(credentials_json: str):
    """Load OAuth credentials from minimal or full serialized JSON."""
    try:
        credentials_data = json.loads(credentials_json)
    except Exception as exc:
        _log(f"Unable to parse credentials JSON: {exc}", error=True)
        sys.exit(1)

    # Preferred path: minimal credential contract (no _class/_module dependency).
    creds = _build_credentials_from_minimal_json(credentials_data)
    if creds is not None:
        return creds

    # Backward compatibility path: full oauth2client serialized credentials.
    try:
        normalized_json = json.dumps(credentials_data)
        creds = client.Credentials.new_from_json(normalized_json)
    except Exception as exc:
        _log(f"Unable to parse serialized credentials JSON: {exc}", error=True)
        _log(
            "Provide minimal JSON with client_id, client_secret, refresh_token, token_uri "
            "(and optional access_token), or full oauth2client serialized JSON.",
            error=True,
        )
        sys.exit(1)

    if creds is None:
        _log("Credentials JSON did not produce a valid OAuth credential.", error=True)
        _log(
            "Provide minimal JSON with client_id, client_secret, refresh_token, token_uri "
            "(and optional access_token), or full oauth2client serialized JSON.",
            error=True,
        )
        sys.exit(1)

    return creds


def create_drive_service(credentials_json: str):
    """
    Authenticate with Google Drive using serialized OAuth credentials.
    The access token is refreshed in memory for the current session when needed.
    """
    creds = _load_session_credentials(credentials_json)

    try:
        if getattr(creds, "access_token_expired", False) or not getattr(creds, "access_token", None):
            creds.refresh(Http())
        service = build(API_NAME, API_VERSION, http=creds.authorize(Http()), cache_discovery=False)
        _log("Connected to Google Drive Service successfully.")
        return service
    except Exception as exc:
        _log(f"Unable to connect to Google Drive: {exc}", error=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Metadata helpers
# ---------------------------------------------------------------------------
def _identify_id_type(service, id_value: str) -> str:
    """
    Determine if an ID refers to a file or folder by checking its mimeType.
    Returns: 'folder' or 'file'
    """
    try:
        meta = service.files().get(
            fileId=id_value,
            fields="mimeType",
            supportsAllDrives=True,
        ).execute()
        mime = meta.get("mimeType", "")
        return "folder" if mime == MIME_TYPE_FOLDER else "file"
    except Exception as exc:
        _log(f"Unable to identify ID type for '{id_value}': {exc}", error=True)
        sys.exit(1)


def _page_all(list_fn, **kwargs):
    """Iterate through all pages of a Drive files().list() call."""
    results = []
    page_token = None
    while True:
        response = list_fn(**kwargs, pageToken=page_token).execute()
        results.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return results


def _process_folder_items(items: list, queue: list, all_files: list, max_files: Optional[int]) -> bool:
    """
    Process items from a folder: add subfolders to queue, add files to all_files.
    Returns True if max_files limit reached, False otherwise.
    """
    for item in items:
        if item.get("mimeType") == MIME_TYPE_FOLDER:
            queue.append(item["id"])
        else:
            all_files.append(item)
            if max_files and len(all_files) >= max_files:
                return True
    return False


def _list_all_files_recursive(service, folder_id: str, max_files: Optional[int] = None) -> list:
    """
    Recursively list all non-folder files inside folder_id and its subfolders.
    
    Args:
        service: Google Drive service object
        folder_id: Starting folder ID
        max_files: Max files to return (None for unlimited)
    
    Returns:
        List of file dicts with selected metadata fields
    """
    all_files = []
    visited_folders = set()
    queue = [folder_id]
    
    while queue and (max_files is None or len(all_files) < max_files):
        current_folder = queue.pop(0)
        
        if current_folder in visited_folders:
            continue
        visited_folders.add(current_folder)
        
        # Query for items in this folder
        items = _page_all(
            service.files().list,
            q=f"'{current_folder}' in parents and trashed=false",
            pageSize=100,
            fields=DRIVE_LIST_FIELDS,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            corpora="allDrives",
        )
        
        # Process items: add subfolders to queue, files to collection
        if _process_folder_items(items, queue, all_files, max_files):
            break
    
    return all_files


def _list_all_files_recursive_with_paths(service, folder_id: str,
                                         current_path: str = "",
                                         max_files: Optional[int] = None) -> list:
    """
    Recursively list all non-folder files and attach their relative Drive folder path.

    Each returned file dict contains an extra key ``relative_folder_path`` which
    represents the path from the starting folder down to the file's parent folder
    (e.g. "02-Intake & In Progress/GLS").  Joining this with a base volume_path
    recreates the Drive folder hierarchy on disk.

    Unlike the flat variant, this uses depth-first traversal so that the
    relative_folder_path can be built incrementally.
    """
    all_files = []
    items = _page_all(
        service.files().list,
        q=f"'{folder_id}' in parents and trashed=false",
        pageSize=100,
        fields=DRIVE_LIST_FIELDS,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora="allDrives",
    )

    for item in items:
        if max_files and len(all_files) >= max_files:
            break
        if item.get("mimeType") == MIME_TYPE_FOLDER:
            child_path = os.path.join(current_path, item["name"]) if current_path else item["name"]
            nested = _list_all_files_recursive_with_paths(service, item["id"], child_path, max_files)
            all_files.extend(nested)
        else:
            enriched = dict(item)
            enriched["relative_folder_path"] = current_path
            all_files.append(enriched)

    return all_files


def list_metadata(service, folder_id: str = None) -> pd.DataFrame:
    """
    Return a DataFrame with metadata of all accessible files/folders.
    If *folder_id* is given, recursively list all files in that folder and subfolders.
    """
    fields = DRIVE_LIST_FIELDS

    if folder_id:
        _log(f"Listing files inside folder_id={folder_id} (recursive, all subfolders)")
        # Use recursive listing
        items = _list_all_files_recursive(service, folder_id)
    else:
        query = "trashed=false"
        _log("Listing all accessible files/folders (trashed=false)")
        items = _page_all(
            service.files().list,
            q=query,
            pageSize=100,
            fields=fields,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            corpora="allDrives",
        )

    if not items:
        _log("No files found.")
        return pd.DataFrame()

    rows = []
    for f in items:
        owners = ", ".join(o.get("emailAddress", "") for o in f.get("owners", [])) if f.get("owners") else ""
        rows.append(
            {
                "file_id": f.get("id", ""),
                "name": f.get("name", ""),
                "mime_type": f.get("mimeType", ""),
                "size_bytes": f.get("size", "N/A"),
                "created_time": f.get("createdTime", ""),
                "modified_time": f.get("modifiedTime", ""),
                "parent_id": f["parents"][0] if f.get("parents") else "N/A",
                "owners": owners,
            }
        )

    return pd.DataFrame(rows)


def get_file_metadata(service, file_id: str) -> pd.DataFrame:
    """Return a one-row DataFrame with metadata for a single file ID."""
    fields = "id, name, mimeType, size, createdTime, modifiedTime, parents, owners"
    try:
        f = service.files().get(
            fileId=file_id,
            fields=fields,
            supportsAllDrives=True,
        ).execute()
    except Exception as exc:
        _log(f"Could not fetch metadata for file_id={file_id}: {exc}", error=True)
        sys.exit(1)

    owners = ", ".join(o.get("emailAddress", "") for o in f.get("owners", [])) if f.get("owners") else ""
    row = {
        "file_id": f.get("id", ""),
        "name": f.get("name", ""),
        "mime_type": f.get("mimeType", ""),
        "size_bytes": f.get("size", "N/A"),
        "created_time": f.get("createdTime", ""),
        "modified_time": f.get("modifiedTime", ""),
        "parent_id": f["parents"][0] if f.get("parents") else "N/A",
        "owners": owners,
    }
    return pd.DataFrame([row])


# ---------------------------------------------------------------------------
# Download helpers (Direct download to S3)
# ---------------------------------------------------------------------------
def _resolve_export(mime_type: str, file_type: Optional[str]):
    """
    Return (chosen_extension, export_mime_type) for a Google-native file.
    NO fallback: returns None if type is not in the known map.
    """
    if mime_type in GOOGLE_NATIVE_EXPORT_TYPES:
        export_formats = GOOGLE_NATIVE_EXPORT_TYPES[mime_type]
        chosen_format = file_type if (file_type and file_type in export_formats) \
                        else DEFAULT_EXPORT_FORMAT.get(mime_type)
        if not chosen_format:
            chosen_format = next(iter(export_formats))   # first available
        return chosen_format, export_formats[chosen_format]

    return None, None  # Unknown type — no fallback


def _is_export_size_limit_error(exc: Exception) -> bool:
    """Return True when Drive export fails due to export size limits."""
    message = str(exc)
    return "exportSizeLimitExceeded" in message or "too large to be exported" in message.lower()


def _extract_error_details(exc: Exception) -> tuple:
    """Return (error_code, error_message) from known exception types."""
    if isinstance(exc, HttpError):
        error_code = getattr(getattr(exc, "resp", None), "status", None)
        error_message = str(exc)
        try:
            payload = json.loads(exc.content.decode("utf-8"))
            if isinstance(payload, dict):
                error_obj = payload.get("error", {})
                if isinstance(error_obj, dict):
                    error_message = error_obj.get("message", error_message)
        except Exception:
            pass
        return error_code, error_message

    return None, str(exc)


def _sanitize_output_filename(file_name: str) -> str:
    """Return a filesystem-safe file name for local/volume writes."""
    safe_name = (file_name or "").strip()

    # Replace path separators to prevent invalid nested paths.
    # Preserve full name; replace "/" instead of collapsing to basename
    # (which would drop the left side of names like "A / B").
    safe_name = safe_name.replace("/", "_").replace("\\", "_")

    # Remove common invalid filename characters.
    invalid_chars = '<>:"|?*'
    safe_name = "".join("_" if ch in invalid_chars else ch for ch in safe_name)

    # Prevent empty or dot-only filenames.
    safe_name = safe_name.strip().strip(".")
    return safe_name or "unnamed_file"

def _resolve_shortcut_target(service, shortcut_id: str, shortcut_name: str) -> Optional[dict]:
    """Resolve a Google Drive shortcut to its target file metadata."""
    try:
        shortcut_meta = service.files().get(
            fileId=shortcut_id,
            fields="id, name, shortcutDetails(targetId, targetMimeType, targetResourceKey)",
            supportsAllDrives=True,
        ).execute()
    except Exception as exc:
        _log(f"Unable to read shortcut metadata for '{shortcut_name}': {exc}", error=True)
        return None

    shortcut_details = shortcut_meta.get("shortcutDetails", {})
    target_id = shortcut_details.get("targetId")
    target_resource_key = shortcut_details.get("targetResourceKey")
    if not target_id:
        _log(f"Shortcut '{shortcut_name}' has no targetId.", error=True)
        return None

    try:
        target_request = {
            "fileId": target_id,
            "fields": "id, name, mimeType, size, resourceKey",
            "supportsAllDrives": True,
        }
        if target_resource_key:
            target_request["resourceKey"] = target_resource_key

        target_meta = service.files().get(**target_request).execute()
        if target_resource_key and not target_meta.get("resourceKey"):
            target_meta["resourceKey"] = target_resource_key
        _log(f"Resolved shortcut '{shortcut_name}' to target '{target_meta.get('name', target_id)}'.")
        return target_meta
    except Exception as exc:
        _log(f"Unable to resolve target for shortcut '{shortcut_name}': {exc}", error=True)
        return None


def _make_result(success: bool, final_file_name=None, chosen_file_format=None,
                 error_code=None, error_message=None, return_details: bool = False):
    """Return bool by default, or a detailed result dict when requested."""
    if not return_details:
        return success
    return {
        "success": success,
        "final_file_name": final_file_name,
        "chosen_file_format": chosen_file_format,
        "error_code": error_code,
        "error_message": error_message,
    }


def _download_request_to_bytes(request, file_name: str) -> bytes:
    """Execute a media/export request and return payload bytes."""
    bytes_buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(bytes_buffer, request, chunksize=1024 * 1024)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            pct = round(status.progress() * 100, 1)
            _log(f"{file_name} ——> Download progress: {pct}%")
    return bytes_buffer.getvalue()


def _resolve_download_target(service, file_id: str, file_name: str, mime_type: str):
    """Resolve shortcuts and return target (file_id, file_name, mime_type, resource_key) or None."""
    if mime_type != MIME_TYPE_SHORTCUT:
        return file_id, file_name, mime_type, None

    target_meta = _resolve_shortcut_target(service, file_id, file_name)
    if not target_meta:
        return None

    return (
        target_meta["id"],
        target_meta.get("name", file_name),
        target_meta.get("mimeType", ""),
        target_meta.get("resourceKey"),
    )


def _download_google_native(service, file_id: str, file_name: str, mime_type: str,
                            file_type: Optional[str], resource_key: Optional[str] = None):
    """Download a Google-native file, handling export-size fallback when possible."""
    # Sanitize file_name immediately to prevent path issues
    safe_base_name = _sanitize_output_filename(file_name)
    
    chosen_format, export_mime = _resolve_export(mime_type, file_type)
    if not chosen_format:
        msg = f"No export format available for mime_type='{mime_type}' (not in known types)"
        _log(msg, error=True)
        return None, None, None, msg

    s3_file_name = f"{safe_base_name}.{chosen_format}"
    chosen_file_format = chosen_format
    _log(f"Exporting Google-native '{file_name}' as .{chosen_format}")

    try:
        export_request = {"fileId": file_id, "mimeType": export_mime}
        if resource_key:
            export_request["resourceKey"] = resource_key
        request = service.files().export(**export_request)
        payload = _download_request_to_bytes(request, file_name)
        return payload, s3_file_name, chosen_file_format, None
    except HttpError as exc:
        if not _is_export_size_limit_error(exc):
            raise

        export_formats = GOOGLE_NATIVE_EXPORT_TYPES.get(mime_type, {})
        fallback_format = "pdf" if chosen_format != "pdf" and "pdf" in export_formats else None
        if not fallback_format:
            msg = (
                f"Export failed for '{file_name}' as .{chosen_format}: "
                "file is too large to export via Drive API."
            )
            _log(msg, error=True)
            return None, None, None, "exportSizeLimitExceeded"

        _log(
            f"Export size limit hit for '{file_name}' as .{chosen_format}; retrying as .{fallback_format}.",
            error=False,
        )
        export_mime = export_formats[fallback_format]
        s3_file_name = f"{safe_base_name}.{fallback_format}"
        chosen_file_format = fallback_format
        export_request = {"fileId": file_id, "mimeType": export_mime}
        if resource_key:
            export_request["resourceKey"] = resource_key
        request = service.files().export(**export_request)
        payload = _download_request_to_bytes(request, file_name)
        return payload, s3_file_name, chosen_file_format, None


def _download_binary(service, file_id: str, file_name: str, mime_type: str,
                     resource_key: Optional[str] = None):
    """Download a regular non-Google-native file as-is."""
    # Sanitize file_name immediately to prevent path issues
    safe_base_name = _sanitize_output_filename(file_name)
    
    s3_file_name = safe_base_name
    chosen_file_format = os.path.splitext(s3_file_name)[1].lstrip(".") or None
    _log(f"Downloading binary file '{file_name}' (mime={mime_type or 'unknown'})")
    media_request = {"fileId": file_id}
    if resource_key:
        media_request["resourceKey"] = resource_key
    request = service.files().get_media(**media_request)
    payload = _download_request_to_bytes(request, file_name)
    return payload, s3_file_name, chosen_file_format


def download_and_write_output(service, file_id: str, file_name: str,
                              file_type: str = None, mime_type: str = "",
                              s3_client=None, bucket: Optional[str] = None,
                              s3_prefix: Optional[str] = None,
                              volume_path: Optional[str] = None,
                              return_details: bool = False):
    """
    Download a single Drive file and write to S3 and/or volume/local path.

    Returns True/False by default.
    If return_details=True, returns dict with:
      success, final_file_name, chosen_file_format, error_code, error_message
    """
    try:
        resolved_target = _resolve_download_target(service, file_id, file_name, mime_type)
        if not resolved_target:
            return _make_result(
                False,
                error_message=f"Unable to resolve shortcut target for '{file_name}'",
                return_details=return_details,
            )

        file_id, file_name, mime_type, resource_key = resolved_target

        if mime_type.startswith("application/vnd.google-apps."):
            payload, s3_file_name, chosen_file_format, native_error = _download_google_native(
                service, file_id, file_name, mime_type, file_type, resource_key=resource_key
            )
            if native_error:
                return _make_result(
                    False,
                    error_code=403 if native_error == "exportSizeLimitExceeded" else None,
                    error_message=native_error,
                    return_details=return_details,
                )
        else:
            payload, s3_file_name, chosen_file_format = _download_binary(
                service, file_id, file_name, mime_type, resource_key=resource_key
            )

        if s3_client and bucket and s3_prefix:
            s3_key = f"{s3_prefix.rstrip('/')}/{s3_file_name}"
            s3_client.put_object(Bucket=bucket, Key=s3_key, Body=payload)
            _log(f"'{file_name}' streamed to s3://{bucket}/{s3_key}")

        if volume_path:
            os.makedirs(volume_path, exist_ok=True)
            output_path = os.path.join(volume_path, s3_file_name)
            with open(output_path, "wb") as out_file:
                out_file.write(payload)
            _log(f"'{file_name}' written to {output_path}")

        return _make_result(
            True,
            final_file_name=s3_file_name,
            chosen_file_format=chosen_file_format,
            return_details=return_details,
        )

    except Exception as exc:
        _log(f"Error downloading/writing '{file_name}': {exc}", error=True)
        error_code, error_message = _extract_error_details(exc)
        return _make_result(
            False,
            error_code=error_code,
            error_message=error_message,
            return_details=return_details,
        )


def create_s3_client(args):
    """
    Create S3 client using STS assume-role.
    CRITICAL: Role ARN is REQUIRED. No static key fallback.
    """
    if not args.s3_role_arn:
        _log(
            "S3 role ARN is required. Static access keys are not supported. "
            "Provide --s3_role_arn (e.g., arn:aws:iam::<acct>:role/<name>)",
            error=True,
        )
        sys.exit(1)
    
    _log(f"Assuming role for S3: {args.s3_role_arn}")
    try:
        sts = boto3.client("sts")
        assumed_role = sts.assume_role(
            RoleArn=args.s3_role_arn,
            RoleSessionName=args.s3_role_session_name,
            DurationSeconds=args.s3_role_duration_seconds,
        )
        credentials = assumed_role["Credentials"]
        temp_session = boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )
        _log("STS role assumed successfully.")
        return temp_session.client("s3")
    except Exception as exc:
        _log(f"Failed to assume role '{args.s3_role_arn}': {exc}", error=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Core actions
# ---------------------------------------------------------------------------
def action_list(args):
    """List metadata and print to console."""
    service = create_drive_service(args.credentials_json)

    requested_ids = _resolve_requested_ids(args, require_at_least_one=False)

    if not requested_ids:
        df = list_metadata(service)
    else:
        frames = []
        for current_id in requested_ids:
            id_type = _identify_id_type(service, current_id)
            if id_type == "folder":
                _log(f"ID '{current_id}' identified as folder. Listing recursively.")
                current_df = list_metadata(service, folder_id=current_id)
            else:
                _log(f"ID '{current_id}' identified as file. Fetching single-file metadata.")
                current_df = get_file_metadata(service, current_id)

            if not current_df.empty:
                frames.append(current_df)

        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if df.empty:
        print("No files found.")
        return df

    _log(f"Fetched {len(df.columns)} metadata field(s).")

    print("\n" + "=" * 130)
    print(f"  Files accessible by Google Drive service account  |  Total: {len(df)}")
    print("=" * 130)
    # Pretty-print without truncating columns
    with pd.option_context(*PANDAS_PRINT_OPTIONS_DEFAULT):
        print(df.to_string(index=False))
    print("=" * 130)

    return df


def _resolve_requested_ids(args, require_at_least_one: bool) -> Optional[list]:
    """Resolve IDs from --ids and validate required usage."""
    requested_ids = args.ids
    if require_at_least_one and not requested_ids:
        _log("Provide --ids with one or more file/folder IDs.", error=True)
        sys.exit(1)

    return requested_ids


def _validate_download_destinations(args):
    """
    Validate download destinations (S3 and/or volume path).
    Returns tuple (use_s3, use_volume). Exits on validation failure.
    """
    use_s3 = bool(args.s3_bucket or args.s3_prefix or args.s3_role_arn)
    use_volume = bool(args.volume_path)

    if not use_s3 and not use_volume:
        _log(
            "Provide at least one destination: S3 (--s3_bucket, --s3_prefix, --s3_role_arn) "
            "or volume/local path (--volume_path).",
            error=True,
        )
        sys.exit(1)

    if use_s3 and not all([args.s3_bucket, args.s3_prefix, args.s3_role_arn]):
        _log(
            "When using S3 destination, provide all: --s3_bucket, --s3_prefix, --s3_role_arn.",
            error=True,
        )
        sys.exit(1)

    return use_s3, use_volume


def _collect_files_to_process(service, file_id: str, id_type: str) -> list:
    """
    Collect files to process: single file metadata or recursive folder listing.
    Returns list of file dicts. Exits on error if file not found.
    """
    if id_type == "file":
        _log(f"Fetching metadata for file_id={file_id}")
        try:
            meta = service.files().get(
                fileId=file_id,
                fields=FILE_METADATA_FIELDS,
                supportsAllDrives=True,
            ).execute()
            return [meta]
        except Exception as exc:
            _log(f"Could not fetch metadata for file_id={file_id}: {exc}", error=True)
            sys.exit(1)
    
    # Folder: list recursively
    _log(f"Listing files in folder_id={file_id} (recursive)")
    files = _list_all_files_recursive(service, file_id)
    if not files:
        _log(f"No files found inside folder_id={file_id}.")
    else:
        _log(f"Found {len(files)} file(s) to process.")
    return files


def _resolve_top_level_files(service, current_id: str):
    """Resolve one top-level ID to a list of file metadata entries to process."""
    top_meta = service.files().get(
        fileId=current_id,
        fields=FILE_METADATA_FIELDS,
        supportsAllDrives=True,
    ).execute()

    if top_meta.get("mimeType", "") == MIME_TYPE_FOLDER:
        _log(f"Listing files in folder_id={current_id} (recursive)")
        files_to_process = _list_all_files_recursive(service, current_id)
        if not files_to_process:
            _log(f"No files found inside folder_id={current_id}.")
        return files_to_process

    _log(f"Fetching metadata for file_id={current_id}")
    return [top_meta]


def _append_download_result(result: dict, current_id: str, file_id: str, file_name: Optional[str],
                            success_records: list, failure_records: list) -> bool:
    """Append one per-file result row and return True when successful."""
    if result.get("success"):
        success_records.append(
            {
                "source_id": current_id,
                "file_id": file_id,
                "source_file_name": file_name,
                "final_file_name": result.get("final_file_name"),
                "chosen_file_format": result.get("chosen_file_format"),
            }
        )
        return True

    failure_records.append(
        {
            "source_id": current_id,
            "file_id": file_id,
            "source_file_name": file_name,
            "error_code": result.get("error_code"),
            "error_message": result.get("error_message"),
        }
    )
    return False


def _resolve_hierarchy_top_level_files(service, current_id: str) -> list:
    """Resolve one top-level ID to file metadata entries with relative folder paths."""
    top_meta = service.files().get(
        fileId=current_id,
        fields=FILE_METADATA_FIELDS,
        supportsAllDrives=True,
    ).execute()

    if top_meta.get("mimeType", "") != MIME_TYPE_FOLDER:
        _log(f"Single file: {top_meta.get('name', current_id)}")
        top_meta["relative_folder_path"] = ""
        return [top_meta]

    root_name = top_meta.get("name", "")
    _log(f"Listing files in folder '{root_name}' (recursive, hierarchy preserved)")
    files_to_process = _list_all_files_recursive_with_paths(service, current_id, root_name)
    if not files_to_process:
        _log(f"No files found inside folder_id={current_id}.")
        return []

    _log(f"Found {len(files_to_process)} file(s) to process.")
    return files_to_process


def _append_hierarchy_download_result(result: dict, current_id: str, file_id: str,
                                      file_name: str, relative_folder_path: str,
                                      success_records: list, failure_records: list) -> bool:
    """Append one per-file hierarchy-mode result row and return True when successful."""
    base_record = {
        "source_id": current_id,
        "file_id": file_id,
        "source_file_name": file_name,
        "relative_folder_path": relative_folder_path,
    }

    if result.get("success"):
        success_records.append(
            {
                **base_record,
                "final_file_name": result.get("final_file_name"),
                "chosen_file_format": result.get("chosen_file_format"),
            }
        )
        return True

    failure_records.append(
        {
            **base_record,
            "error_code": result.get("error_code"),
            "error_message": result.get("error_message"),
        }
    )
    return False


def _process_hierarchy_id(service, s3, args, current_id: str,
                          success_records: list, failure_records: list) -> tuple:
    """Process one requested ID in hierarchy mode and return (success_count, fail_count)."""
    try:
        files_to_process = _resolve_hierarchy_top_level_files(service, current_id)
    except Exception as exc:
        error_code, error_message = _extract_error_details(exc)
        _log(
            f"Skipping ID '{current_id}' due to top-level metadata failure: {error_message}",
            error=True,
        )
        failure_records.append(
            {
                "source_id": current_id,
                "file_id": current_id,
                "source_file_name": None,
                "relative_folder_path": None,
                "error_code": error_code,
                "error_message": error_message,
            }
        )
        return 0, 1

    if not files_to_process:
        return 0, 0

    success_count = 0
    fail_count = 0

    for file_meta in files_to_process:
        file_id = file_meta["id"]
        file_name = file_meta["name"]
        mime_type = file_meta.get("mimeType", "")
        relative_folder_path = file_meta.get("relative_folder_path", "")
        target_volume_path = args.volume_path

        if target_volume_path and relative_folder_path:
            target_volume_path = os.path.join(target_volume_path, relative_folder_path)

        _log(f"  '{file_name}' -> {target_volume_path or '(no volume path)'}")

        result = download_and_write_output(
            service,
            file_id,
            file_name,
            file_type=args.file_type,
            mime_type=mime_type,
            s3_client=s3,
            bucket=args.s3_bucket,
            s3_prefix=args.s3_prefix,
            volume_path=target_volume_path,
            return_details=True,
        )

        if _append_hierarchy_download_result(
            result,
            current_id,
            file_id,
            file_name,
            relative_folder_path,
            success_records,
            failure_records,
        ):
            success_count += 1
        else:
            fail_count += 1

    return success_count, fail_count


def action_download(args):
    """Download file(s) from Drive and write to S3 and/or volume/local path."""
    file_ids = _resolve_requested_ids(args, require_at_least_one=True)

    use_s3, _ = _validate_download_destinations(args)

    service = create_drive_service(args.credentials_json)
    s3 = create_s3_client(args) if use_s3 else None

    total_success = 0
    total_fail = 0
    success_records = []
    failure_records = []

    for current_id in file_ids:
        _log(f"\n--- Processing ID: {current_id} ---")

        try:
            files_to_process = _resolve_top_level_files(service, current_id)
        except Exception as exc:
            error_code, error_message = _extract_error_details(exc)
            _log(
                f"Skipping ID '{current_id}' due to top-level metadata failure: {error_message}",
                error=True,
            )
            total_fail += 1
            failure_records.append(
                {
                    "source_id": current_id,
                    "file_id": current_id,
                    "source_file_name": None,
                    "error_code": error_code,
                    "error_message": error_message,
                }
            )
            continue

        success_count = 0
        fail_count = 0

        for file_meta in files_to_process:
            file_id = file_meta["id"]
            file_name = file_meta["name"]
            mime_type = file_meta.get("mimeType", "")

            result = download_and_write_output(
                service,
                file_id,
                file_name,
                file_type=args.file_type,
                mime_type=mime_type,
                s3_client=s3,
                bucket=args.s3_bucket,
                s3_prefix=args.s3_prefix,
                volume_path=args.volume_path,
                return_details=True,
            )

            if _append_download_result(result, current_id, file_id, file_name, success_records, failure_records):
                success_count += 1
            else:
                fail_count += 1

        _log(f"ID {current_id}: Success: {success_count}, Failures: {fail_count}")
        total_success += success_count
        total_fail += fail_count

    _log("\n=== Overall Download Summary ===")
    _log(f"Total Success: {total_success}, Total Failures: {total_fail}")

    success_df = pd.DataFrame(success_records)
    failure_df = pd.DataFrame(failure_records)

    if not success_df.empty:
        print("\nSuccess files:")
        with pd.option_context(*PANDAS_PRINT_OPTIONS_WIDE):
            print(success_df.to_string(index=False))

    if not failure_df.empty:
        print("\nFailed files:")
        with pd.option_context(*PANDAS_PRINT_OPTIONS_WIDE):
            print(failure_df.to_string(index=False))

    return success_df, failure_df


def action_download_with_hierarchy(args):
    """
    Download file(s) from Drive to a volume path that mirrors the Drive folder hierarchy.

    For a file located at  RootFolder/02-Intake & In Progress/GLS/report.pdf
    and --volume_path /Volumes/volume_or_s3_raw/workday  the file will be written to
    /Volumes/volume_or_s3_raw/workday/02-Intake & In Progress/GLS/report.pdf

    Single-file IDs are written directly into volume_path (no sub-directory),
    exactly the same as action_download.
    S3 upload behaviour is identical to action_download (prefix is not expanded).
    """
    file_ids = _resolve_requested_ids(args, require_at_least_one=True)

    use_s3, _ = _validate_download_destinations(args)

    service = create_drive_service(args.credentials_json)
    s3 = create_s3_client(args) if use_s3 else None

    total_success = 0
    total_fail = 0
    success_records = []
    failure_records = []

    for current_id in file_ids:
        _log(f"\n--- Processing ID: {current_id} (hierarchy mode) ---")
        success_count, fail_count = _process_hierarchy_id(
            service,
            s3,
            args,
            current_id,
            success_records,
            failure_records,
        )

        _log(f"ID {current_id}: Success: {success_count}, Failures: {fail_count}")
        total_success += success_count
        total_fail += fail_count

    _log("\n=== Overall Download Summary (hierarchy mode) ===")
    _log(f"Total Success: {total_success}, Total Failures: {total_fail}")

    success_df = pd.DataFrame(success_records)
    failure_df = pd.DataFrame(failure_records)

    if not success_df.empty:
        print("\nSuccess files:")
        with pd.option_context(*PANDAS_PRINT_OPTIONS_WIDE):
            print(success_df.to_string(index=False))

    if not failure_df.empty:
        print("\nFailed files:")
        with pd.option_context(*PANDAS_PRINT_OPTIONS_WIDE):
            print(failure_df.to_string(index=False))

    return success_df, failure_df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Google Drive file downloader (S3 and/or volume/local output)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="action", required=True)

    # ---- shared arguments ----
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument("--credentials_json", required=True,
                        help=(
                            "OAuth credentials JSON. Preferred minimal contract: client_id, "
                            "client_secret, refresh_token, token_uri (+ optional access_token). "
                            "Full oauth2client serialized JSON also supported."
                        ))
    shared.add_argument("--ids", nargs='+', default=None,
                        help="One or more Google Drive file/folder IDs. Example: --ids id1 id2 id3")

    # ---- list sub-command ----
    sub.add_parser("list", parents=[shared],
                   help="List metadata for all files, or for --ids (folders are recursive).")

    # ---- download sub-command ----
    p_dl = sub.add_parser("download", parents=[shared],
                           help="Download file(s) from Drive and write to S3 and/or volume path.")
    p_dl.add_argument("--s3_bucket", required=False,
                      help="Target S3 bucket name.")
    p_dl.add_argument("--s3_prefix", required=False,
                      help="Target S3 key prefix (base path for files).")
    p_dl.add_argument("--s3_role_arn", required=False,
                      help="AWS IAM Role ARN for STS assume-role (REQUIRED; no static keys).")
    p_dl.add_argument("--volume_path", default=None,
                      help=(
                          "Databricks volume or local output path for downloaded files, "
                          "for example /Volumes/<catalog>/<schema>/<volume>/gdrive."
                      ))
    p_dl.add_argument("--s3_role_session_name", default="gdriveToS3Session",
                      help="Role session name for STS (default: gdriveToS3Session).")
    p_dl.add_argument("--s3_role_duration_seconds", type=int, default=3600,
                      help="STS session duration in seconds (default: 3600).")
    p_dl.add_argument("--file_type", default=None,
                      help=(
                          "Override export format for Google-native files. "
                          "Supported per type:\n"
                          "  Sheets: xlsx, csv, pdf, ods, tsv, html\n"
                          "  Docs: docx, pdf, txt, odt, rtf, html, epub, zip\n"
                          "  Slides: pptx, pdf, odp, txt\n"
                          "  Drawings: png, svg, pdf, jpg\n"
                          "  Forms: zip"
                      ))

    # ---- download-hierarchy sub-command ----
    p_dh = sub.add_parser(
        "download-hierarchy",
        parents=[shared],
        help=(
            "Download folder(s) from Drive preserving the Drive folder structure "
            "under --volume_path. Files are written to "
            "<volume_path>/<relative_drive_path>/<file>."
        ),
    )
    # Re-use all the same arguments as the download subcommand
    for action in p_dl._actions:
        if action.dest not in ("help",):
            try:
                p_dh._add_action(action)
            except argparse.ArgumentError:
                pass  # argument already added via shared parent

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.action == "list":
        action_list(args)
    elif args.action == "download":
        action_download(args)
    elif args.action == "download-hierarchy":
        action_download_with_hierarchy(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
