
import asyncio
from cloud_services import get_cloud_service
from cloud_services.storage_providers import S3Service, DiscoveredObject
from moto import mock_aws
import os

def test_aws_download_file():
    # s3_provider = get_cloud_service("aws", "storage")
    # s3_provider.dowload_file("test-q99-data", "../qdrant_pdf", "gold/default/pdf/qdrant_pdf/")

    # assert os.path.exists("../qdrant_pdf/.lock")
    # assert os.path.exists("../qdrant_pdf/meta.json")
    # assert os.path.exists("../qdrant_pdf/collection/default_winshare/storage.sqlite")
    mock = mock_aws()
    mock.start()

    file_path = 'test_upload.txt'
    with open(file_path, 'w+') as f:
        f.write('test content')

    s3_provider = get_cloud_service("aws", "storage")
    s3_provider.s3_client.create_bucket(Bucket='my_bucket')
    s3_provider.upload_file(data=file_path,container="my_bucket",key="my_bucket/test.txt")
    s3_provider.dowload_file("my_bucket", "../texts")

    assert os.path.exists("../texts/my_bucket/test.txt")

    if os.path.exists(file_path):
        os.remove(file_path)
    mock.stop()

def test_aws_storage_service():
    mock = mock_aws()
    mock.start()
    
    s3_provider = get_cloud_service("aws", "storage")
    assert isinstance(s3_provider, S3Service)
    s3_provider.s3_client.create_bucket(Bucket='my_bucket')

    file_path = 'test_upload.txt'
    with open(file_path, 'w+') as f:
        f.write('test content')
    
    s3_provider.upload_file(data=file_path,container="my_bucket",key="my_bucket/test.txt")

    test_file = s3_provider.get_file(container="my_bucket",key="my_bucket/test.txt")

    assert test_file.read().decode('utf-8') == "test content"

    if os.path.exists(file_path):
        os.remove(file_path)
    mock.stop()

def test_aws_download_file_obj():
    bucket_name = 'my-test-bucket'
    download_location = 'testfile.txt'
    file_content = b"Hello, world!"
    
    mock = mock_aws()
    mock.start()

    s3_provider: S3Service = get_cloud_service("aws", "storage")
    s3_provider.s3_client.create_bucket(Bucket=bucket_name)
    s3_provider.s3_client.put_object(Bucket=bucket_name, Key=download_location, Body=file_content)

    downloaded_file = s3_provider.download_bites_file(container=bucket_name, key=download_location)
    file_data = downloaded_file.read()
    assert file_content == file_data


def test_s3_list_objects_with_delimiter():
    mock = mock_aws()
    mock.start()

    s3_provider: S3Service = get_cloud_service("aws", "storage")
    bucket = "delimiter-test-bucket"
    s3_provider.s3_client.create_bucket(Bucket=bucket)

    # Create objects in a folder structure
    s3_provider.s3_client.put_object(Bucket=bucket, Key="folder1/a.txt", Body=b"a")
    s3_provider.s3_client.put_object(Bucket=bucket, Key="folder1/b.txt", Body=b"b")
    s3_provider.s3_client.put_object(Bucket=bucket, Key="folder2/c.txt", Body=b"c")
    s3_provider.s3_client.put_object(Bucket=bucket, Key="root.txt", Body=b"root")

    # List root with delimiter
    result = s3_provider.list_objects_with_delimiter(container=bucket, prefix="", delimiter="/")
    prefixes = sorted(result["common_prefixes"])
    assert prefixes == ["folder1/", "folder2/"]
    assert len(result["contents"]) == 1
    assert result["contents"][0]["key"] == "root.txt"

    # List folder1/ with delimiter
    result2 = s3_provider.list_objects_with_delimiter(container=bucket, prefix="folder1/", delimiter="/")
    assert result2["common_prefixes"] == []
    assert len(result2["contents"]) == 2
    keys = sorted(c["key"] for c in result2["contents"])
    assert keys == ["folder1/a.txt", "folder1/b.txt"]

    mock.stop()


def test_s3_files_discovery_returns_discovered_objects():
    mock = mock_aws()
    mock.start()

    s3_provider: S3Service = get_cloud_service("aws", "storage")
    bucket = "discovery-test-bucket"
    s3_provider.s3_client.create_bucket(Bucket=bucket)

    s3_provider.s3_client.put_object(Bucket=bucket, Key="doc.pdf", Body=b"x" * 100)
    s3_provider.s3_client.put_object(Bucket=bucket, Key="data.csv", Body=b"y" * 50)
    s3_provider.s3_client.put_object(Bucket=bucket, Key="image.png", Body=b"z" * 200)

    discovered = asyncio.run(
        s3_provider.files_discovery(
            container_name=bucket,
            ingested_paths=[],
            latest_created_at=0,
        )
    )

    assert len(discovered) == 3
    assert all(isinstance(obj, DiscoveredObject) for obj in discovered)

    by_path = {obj.path: obj for obj in discovered}

    pdf = by_path[f"s3://{bucket}/doc.pdf"]
    assert pdf.file_size == 100
    assert pdf.source_modified_at > 0
    assert pdf.content_hash is not None
    assert pdf.mime_type == "application/pdf"

    csv_obj = by_path[f"s3://{bucket}/data.csv"]
    assert csv_obj.file_size == 50
    assert csv_obj.mime_type == "text/csv"

    mock.stop()


def test_s3_custom_credentials():
    mock = mock_aws()
    mock.start()

    s3_provider: S3Service = get_cloud_service(
        "aws", "storage",
        aws_key="custom-key",
        aws_secret="custom-secret",
    )
    # Should successfully create a client with custom credentials
    assert s3_provider.s3_client is not None
    s3_provider.s3_client.create_bucket(Bucket="custom-bucket")
    s3_provider.s3_client.put_object(Bucket="custom-bucket", Key="test.txt", Body=b"hello")
    result = s3_provider.get_file(container="custom-bucket", key="test.txt")
    assert result.read() == b"hello"

    mock.stop()


def test_factory_kwargs_passthrough():
    """Factory passes kwargs to service constructors."""
    mock = mock_aws()
    mock.start()

    s3 = get_cloud_service("aws", "storage", aws_key="k", aws_secret="s")
    assert isinstance(s3, S3Service)

    mock.stop()