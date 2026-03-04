from setuptools import find_packages, setup

setup(
    name='cloud_services',
    packages=find_packages(include=['cloud_services']),
    version='0.3.0',
    description='Library to connect to different cloud services',
    author='Q99',
    install_requires=["boto3", "azure-storage-blob", "azure-identity", "google-cloud-storage"],
    tests_require=['pytest', 'moto[s3,logs]'],
    test_suite='tests'
)
