# path: cloud_services/storage.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import hashlib
import json
import mimetypes
import os
import tempfile
from typing import Any, Dict, Iterable, Optional

import boto3
import botocore
from botocore.config import Config
from azure.storage.blob import BlobServiceClient

try:
    from google.cloud import storage as gcs_storage
except ImportError:
    gcs_storage = None

from cloud_services.env_vars import AWS_KEY, AWS_REGION, AWS_SECRET, AWS_URL, CONECTION_STRING


@dataclass
class DiscoveredObject:
    path: str
    file_size: int = 0
    source_modified_at: int = 0
    content_hash: Optional[str] = None
    mime_type: Optional[str] = None


class AbstractStorageService(ABC):

    @abstractmethod
    def get_file(self, container:str, key: str):
        ...

    @abstractmethod
    def upload_file(self, data, container:str, key: str):
        ...

    @abstractmethod
    def delete_file(self, container:str, key: str):
        ...

    @abstractmethod
    def dowload_file(self, container_name: str, download_location: str, path_prefix: str = ""):
        ...

    @abstractmethod
    def upload_bites_file(self, data, container:str, key: str):
        ...

    @abstractmethod
    def download_bites_file(self, container:str, key: str):
        ...

    @abstractmethod
    async def files_discovery(
        self,
        container_name: str,
        ingested_paths: Iterable[str],
        latest_created_at: int,
        max_file_size_mb: int = 500,
        use_hash: bool = False,
        prefix: str = "",
    ) -> list[DiscoveredObject]:
        ...

    @abstractmethod
    def list_objects_with_delimiter(
        self,
        container: str,
        prefix: str = "",
        delimiter: str = "/",
    ) -> Dict[str, Any]:
        """List objects using a delimiter to discover virtual folder prefixes.

        Returns a dict with:
            "common_prefixes": list[str]  -- virtual folder prefixes
            "contents": list[dict]        -- objects at this level, each with
                                             keys: "key", "size", "last_modified"
        """
        ...


class S3Service(AbstractStorageService):
    s3_default = {
        "aws_access_key_id": AWS_KEY,
        "aws_secret_access_key": AWS_SECRET,
        "endpoint_url": AWS_URL,
    }

    def __init__(self, aws_key=None, aws_secret=None, aws_url=None, aws_region=None):

        s3_provided_keys = {
            "aws_access_key_id": aws_key,
            "aws_secret_access_key": aws_secret,
            "endpoint_url": aws_url,
            "region": aws_region
        }

        any_provided = any(v is not None for v in s3_provided_keys.values())

        if any_provided:
            region = s3_provided_keys.pop("region")
            effective = {**self.s3_default}
            for k, v in s3_provided_keys.items():
                if v is not None:
                    effective[k] = v
            self.s3_client = boto3.client(
                "s3",
                config=Config(region_name=region or AWS_REGION),
                **effective,
            )
        else:
            self.s3_client = boto3.client(
                "s3",
                config=Config(region_name=AWS_REGION),
                **self.s3_default,
            )
        self.s3_client.list_buckets()

    async def files_discovery(
        self,
        container_name: str,
        ingested_paths: Iterable[str],
        latest_created_at: int,
        max_file_size_mb: int = 500,
        use_hash: bool = False,
        prefix: str = "",
    ) -> list[DiscoveredObject]:
        bucket_name = container_name
        ingested_set = set(ingested_paths)

        seen_identifiers: set[str] = set()
        discovered: list[DiscoveredObject] = []

        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                object_key = obj["Key"]
                if object_key.endswith("/") and obj["Size"] == 0:
                    continue

                s3_path = f"s3://{bucket_name}/{object_key}"

                if s3_path in ingested_set:
                    continue

                last_modified = int(obj["LastModified"].timestamp())
                if last_modified <= latest_created_at:
                    continue

                try:
                    file_size = obj["Size"]
                    if file_size > max_file_size_mb * 1024 * 1024:
                        continue

                    if use_hash:
                        hasher = hashlib.sha256()
                        response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
                        with response["Body"] as file_obj:
                            while True:
                                chunk = file_obj.read(8192)
                                if not chunk:
                                    break
                                hasher.update(chunk)
                        file_identifier = hasher.hexdigest()

                        if file_identifier in seen_identifiers:
                            continue
                        seen_identifiers.add(file_identifier)

                    discovered.append(DiscoveredObject(
                        path=s3_path,
                        file_size=file_size,
                        source_modified_at=last_modified,
                        content_hash=obj.get("ETag", "").strip('"'),
                        mime_type=mimetypes.guess_type(object_key)[0],
                    ))

                except botocore.exceptions.BotoCoreError:
                    continue
                except Exception:
                    continue

        return discovered

    def get_file(self, container:str, key: str):
        response = self.s3_client.get_object(Bucket=container, Key=key)
        return response["Body"]

    def upload_file(self, data, container:str, key: str):
        return self.s3_client.upload_file(data, container, key)

    def delete_file(self, container:str, key: str):
        return self.s3_client.delete_object(Bucket=container, Key=key)

    def dowload_file(self, container: str, download_location: str, path_prefix: str = ""):
        bucket_name = container
        bucket_objects = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path_prefix)
        for s3_key in bucket_objects.get("Contents", []) or []:
            relative_path = os.path.relpath(s3_key["Key"], start=path_prefix)
            local_file_path = os.path.join(download_location, relative_path)
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            self.s3_client.download_file(bucket_name, s3_key["Key"], local_file_path)

    def upload_bites_file(self, data, container:str, key: str):
        return self.s3_client.upload_fileobj(data, container, key)

    def download_bites_file(self, container:str, key: str):
        fp = tempfile.TemporaryFile()
        self.s3_client.download_fileobj(Bucket=container, Key=key, Fileobj=fp)
        fp.seek(0)
        return fp

    def list_objects_with_delimiter(
        self,
        container: str,
        prefix: str = "",
        delimiter: str = "/",
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {"common_prefixes": [], "contents": []}
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=container, Prefix=prefix, Delimiter=delimiter
        ):
            for cp in page.get("CommonPrefixes", []):
                result["common_prefixes"].append(cp["Prefix"])
            for obj in page.get("Contents", []):
                result["contents"].append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"],
                    "etag": obj.get("ETag", "").strip('"'),
                })
        return result


class AzureBlobService(AbstractStorageService):

    connection_string = CONECTION_STRING

    def __init__(self, connection_string = None):
        string = connection_string if connection_string else CONECTION_STRING
        self.blob_service_client = BlobServiceClient.from_connection_string(string)
        self.blob_service_client.get_account_information()

    async def files_discovery(
        self,
        container: str,
        ingested_paths: Iterable[str],
        latest_created_at: int,
        max_file_size_mb: int = 500,
        use_hash: bool = False,
        prefix: str = "",
    ) -> list[DiscoveredObject]:
        ingested_set = set(ingested_paths)
        seen_identifiers: set[str] = set()
        discovered: list[DiscoveredObject] = []

        container_client = self.blob_service_client.get_container_client(container)

        for blob in container_client.list_blobs(name_starts_with=prefix):
            blob_path = f"azure://{container}/{blob.name}"

            if blob_path in ingested_set:
                continue

            if not blob.last_modified:
                continue

            blob_ts = int(blob.last_modified.timestamp())
            if blob_ts <= latest_created_at:
                continue

            blob_size = blob.size or 0
            if blob_size > max_file_size_mb * 1024 * 1024:
                continue

            try:
                content_hash = None
                if use_hash:
                    hasher = hashlib.sha256()
                    downloader = container_client.download_blob(blob.name)
                    hasher.update(downloader.readall())
                    file_identifier = hasher.hexdigest()

                    if file_identifier in seen_identifiers:
                        continue
                    seen_identifiers.add(file_identifier)
                    content_hash = file_identifier
                else:
                    if blob.content_settings and getattr(blob.content_settings, "content_md5", None):
                        content_hash = blob.content_settings.content_md5.hex()
                    elif blob.etag:
                        content_hash = blob.etag.strip('"')

                discovered.append(DiscoveredObject(
                    path=blob_path,
                    file_size=blob_size,
                    source_modified_at=blob_ts,
                    content_hash=content_hash,
                    mime_type=mimetypes.guess_type(blob.name)[0],
                ))

            except Exception:
                continue

        return discovered

    def get_file(self, container:str, key: str):
        blob_client = self.blob_service_client.get_blob_client(container=container, blob=key)
        return blob_client.download_blob().readall()

    def upload_file(self, data, container:str, key: str):
        blob_client = self.blob_service_client.get_blob_client(container=container, blob=key)
        with open(data, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)

    def delete_file(self, container:str, key: str):
        blob_client = self.blob_service_client.get_blob_client(container=container, blob=key)
        blob_client.delete_blob()

    def dowload_file(self, container: str, download_location: str, path_prefix: str = ""):
        container_client = self.blob_service_client.get_container_client(container)
        blobs = container_client.list_blobs(name_starts_with=path_prefix)
        for blob in blobs:
            rel_path = os.path.relpath(blob.name, start=path_prefix)
            local_path = os.path.join(download_location, rel_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as file:
                data = container_client.download_blob(blob.name)
                file.write(data.readall())

    def upload_bites_file(self, data, container:str, key: str):
        blob_client = self.blob_service_client.get_blob_client(container=container, blob=key)
        blob_client.upload_blob(data, overwrite=True)

    def download_bites_file(self, container:str, key: str):
        blob_client = self.blob_service_client.get_blob_client(container=container, blob=key)
        fp = tempfile.TemporaryFile()
        data = blob_client.download_blob()
        fp.write(data.readall())
        fp.seek(0)
        return fp

    def list_objects_with_delimiter(
        self,
        container: str,
        prefix: str = "",
        delimiter: str = "/",
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {"common_prefixes": [], "contents": []}
        container_client = self.blob_service_client.get_container_client(container)
        for item in container_client.walk_blobs(
            name_starts_with=prefix, delimiter=delimiter
        ):
            if hasattr(item, "prefix"):
                result["common_prefixes"].append(item.prefix)
            else:
                content_md5 = None
                if item.content_settings and getattr(item.content_settings, "content_md5", None):
                    content_md5 = item.content_settings.content_md5.hex()
                result["contents"].append({
                    "key": item.name,
                    "size": item.size or 0,
                    "last_modified": item.last_modified,
                    "etag": content_md5,
                })
        return result


class GCSService(AbstractStorageService):
    """Google Cloud Storage provider."""

    def __init__(self, service_account_json=None):
        if gcs_storage is None:
            raise ImportError(
                "google-cloud-storage is required for GCS support. "
                "Install with: pip install google-cloud-storage"
            )
        if service_account_json:
            if isinstance(service_account_json, str):
                info = json.loads(service_account_json)
            else:
                info = service_account_json
            self.gcs_client = gcs_storage.Client.from_service_account_info(info)
        else:
            self.gcs_client = gcs_storage.Client()

    def get_file(self, container: str, key: str):
        bucket = self.gcs_client.bucket(container)
        blob = bucket.blob(key)
        return blob.download_as_bytes()

    def upload_file(self, data, container: str, key: str):
        bucket = self.gcs_client.bucket(container)
        blob = bucket.blob(key)
        blob.upload_from_filename(data)

    def delete_file(self, container: str, key: str):
        bucket = self.gcs_client.bucket(container)
        blob = bucket.blob(key)
        blob.delete()

    def dowload_file(self, container: str, download_location: str, path_prefix: str = ""):
        blobs = self.gcs_client.list_blobs(container, prefix=path_prefix)
        for blob in blobs:
            rel_path = os.path.relpath(blob.name, start=path_prefix)
            local_path = os.path.join(download_location, rel_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            blob.download_to_filename(local_path)

    def upload_bites_file(self, data, container: str, key: str):
        bucket = self.gcs_client.bucket(container)
        blob = bucket.blob(key)
        blob.upload_from_file(data)

    def download_bites_file(self, container: str, key: str):
        bucket = self.gcs_client.bucket(container)
        blob = bucket.blob(key)
        fp = tempfile.TemporaryFile()
        blob.download_to_file(fp)
        fp.seek(0)
        return fp

    async def files_discovery(
        self,
        container_name: str,
        ingested_paths: Iterable[str],
        latest_created_at: int,
        max_file_size_mb: int = 500,
        use_hash: bool = False,
        prefix: str = "",
    ) -> list[DiscoveredObject]:
        ingested_set = set(ingested_paths)
        seen_identifiers: set[str] = set()
        discovered: list[DiscoveredObject] = []

        blobs = self.gcs_client.list_blobs(container_name, prefix=prefix)

        for blob in blobs:
            if blob.name.endswith("/") and (blob.size or 0) == 0:
                continue

            gcs_path = f"gcs://{container_name}/{blob.name}"

            if gcs_path in ingested_set:
                continue

            if not blob.updated:
                continue

            blob_ts = int(blob.updated.timestamp())
            if blob_ts <= latest_created_at:
                continue

            blob_size = blob.size or 0
            if blob_size > max_file_size_mb * 1024 * 1024:
                continue

            try:
                content_hash = None
                if use_hash:
                    hasher = hashlib.sha256()
                    data = blob.download_as_bytes()
                    hasher.update(data)
                    file_identifier = hasher.hexdigest()

                    if file_identifier in seen_identifiers:
                        continue
                    seen_identifiers.add(file_identifier)
                    content_hash = file_identifier
                else:
                    if blob.md5_hash:
                        content_hash = blob.md5_hash
                    elif blob.crc32c:
                        content_hash = blob.crc32c

                discovered.append(DiscoveredObject(
                    path=gcs_path,
                    file_size=blob_size,
                    source_modified_at=blob_ts,
                    content_hash=content_hash,
                    mime_type=mimetypes.guess_type(blob.name)[0],
                ))

            except Exception:
                continue

        return discovered

    def list_objects_with_delimiter(
        self,
        container: str,
        prefix: str = "",
        delimiter: str = "/",
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {"common_prefixes": [], "contents": []}
        blobs_iterator = self.gcs_client.list_blobs(
            container, prefix=prefix, delimiter=delimiter
        )
        for blob in blobs_iterator:
            result["contents"].append({
                "key": blob.name,
                "size": blob.size or 0,
                "last_modified": blob.updated,
                "etag": blob.md5_hash or blob.crc32c,
            })
        # prefixes are available on the iterator after consumption
        result["common_prefixes"] = list(blobs_iterator.prefixes)
        return result
