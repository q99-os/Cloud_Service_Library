# path: cloud_services/storage.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import hashlib
import os
import tempfile
from typing import Any, BinaryIO, Callable, Dict, Iterable, Optional, Type

import boto3
import botocore
from botocore.config import Config
from azure.storage.blob import BlobServiceClient

from cloud_services.env_vars import AWS_KEY, AWS_REGION, AWS_SECRET, AWS_URL, CONECTION_STRING


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
    ) -> list[str]:
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
            s3_provided_keys.pop("region")
            self.s3_client = boto3.client(
            "s3",
            config=Config(region_name=aws_region),
            **self.s3_default,
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
    ) -> list[str]:
        bucket_name = container_name
        ingested_set = set(ingested_paths)

        seen_identifiers: set[str] = set()
        discovered_paths: list[str] = []

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

                    discovered_paths.append(s3_path)

                except botocore.exceptions.BotoCoreError:
                    continue
                except Exception:
                    continue

        return discovered_paths

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
    ) -> list[str]:
        ingested_set = set(ingested_paths)
        seen_identifiers: set[str] = set()
        discovered_paths: list[str] = []

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
                if use_hash:
                    hasher = hashlib.sha256()
                    downloader = container_client.download_blob(blob.name)
                    hasher.update(downloader.readall())
                    file_identifier = hasher.hexdigest()

                    if file_identifier in seen_identifiers:
                        continue
                    seen_identifiers.add(file_identifier)

                discovered_paths.append(blob_path)

            except Exception:
                continue

        return discovered_paths

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
