import os

#AWS
AWS_KEY = os.environ.get("AWS_KEY")
AWS_SECRET = os.environ.get("AWS_SECRET")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

#Azure
CONNECTION_STRING = os.getenv("CONNECTION_STRING")

#GCS
GCS_SERVICE_ACCOUNT_JSON = os.environ.get("GCS_SERVICE_ACCOUNT_JSON")

#testing
AWS_URL = os.environ.get("AWS_URL", None)