
from cloud_services.logs_providers import CloudWachService
from cloud_services.storage_providers import AzureBlobService, S3Service, GCSService, FolderNode


def get_cloud_service(cloud, service, **kwargs):
    providers = {
        "aws":{
            "storage": S3Service,
            "logging": CloudWachService
            },
        "azure":{
            "storage": AzureBlobService
        },
        "gcp":{
            "storage": GCSService
        }
    }


    return providers[cloud][service](**kwargs)
