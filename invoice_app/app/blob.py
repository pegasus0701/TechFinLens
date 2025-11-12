# app/blob.py
import os
from dotenv import load_dotenv
from azure.storage.blob import (
    BlobServiceClient,
    generate_blob_sas,
    BlobSasPermissions
)
from datetime import datetime, timedelta
from urllib.parse import quote
import requests

load_dotenv()

def validate_file_format(file_bytes: bytes, filename: str) -> bool:
    """Validate file format without using magic library"""
    supported_extensions = ['pdf', 'jpg', 'jpeg', 'png', 'tiff', 'tif', 'bmp', 'webp']
    
    # Get file extension
    file_ext = filename.lower().split('.')[-1] if '.' in filename else ''
    
    # Check if extension is supported
    if file_ext not in supported_extensions:
        raise ValueError(f"Unsupported file extension: {file_ext}. Supported: {', '.join(supported_extensions)}")
    
    # Basic file validation
    if len(file_bytes) == 0:
        raise ValueError("File is empty")
    
    # Basic PDF validation
    if file_ext == 'pdf' and not file_bytes.startswith(b'%PDF'):
        print("⚠️ Warning: File doesn't have standard PDF header, but will try to process anyway")
    
    print(f"✅ File validation passed: {filename}, Extension: {file_ext}, Size: {len(file_bytes)} bytes")
    return True

def validate_sas_url(sas_url: str) -> bool:
    """Validate that SAS URL is accessible and properly formatted"""
    try:
        print(f"🔍 Validating SAS URL...")
        
        # Test if URL is accessible
        print("🔍 Testing URL accessibility...")
        response = requests.get(sas_url, timeout=10)
        
        if response.status_code == 200:
            print("✅ SAS URL is accessible and valid")
            print(f"✅ Content length: {len(response.content)} bytes")
            return True
        else:
            print(f"❌ SAS URL returned status code: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ SAS URL validation failed: {e}")
        return False

def upload_to_blob(file_name: str, file_bytes: bytes) -> str:
    try:
        CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "invoices")
        ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
        ACCOUNT_KEY = os.getenv("AZURE_STORAGE_KEY")
        
        if not all([CONN_STR, ACCOUNT_NAME, ACCOUNT_KEY]):
            raise ValueError("Missing required Azure Storage environment variables")

        print(f"🔍 Using Storage Account: {ACCOUNT_NAME}")
        print(f"🔍 Container: {CONTAINER}")
        print(f"📁 File: {file_name}, Size: {len(file_bytes)} bytes")

        # Validate file format before upload
        validate_file_format(file_bytes, file_name)

        # Connect to blob service
        client = BlobServiceClient.from_connection_string(CONN_STR)
        container_client = client.get_container_client(CONTAINER)

        # Ensure container exists
        try:
            container_client.get_container_properties()
            print(f"✅ Container '{CONTAINER}' exists")
        except Exception:
            print(f"⚠️ Container '{CONTAINER}' doesn't exist, creating...")
            container_client.create_container()
            print(f"✅ Container '{CONTAINER}' created")

        # Upload blob
        blob_client = container_client.get_blob_client(file_name)
        print(f"📤 Uploading file: {file_name}")
        
        blob_client.upload_blob(file_bytes, overwrite=True)
        print("✅ File uploaded to blob storage")

        # Generate SAS token with proper permissions
        sas_token = generate_blob_sas(
            account_name=ACCOUNT_NAME,
            container_name=CONTAINER,
            blob_name=file_name,
            account_key=ACCOUNT_KEY,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=24)
        )

        # Properly encode the blob name and construct URL
        encoded_blob_name = quote(file_name, safe='')
        blob_sas_url = f"https://{ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER}/{encoded_blob_name}?{sas_token}"

        print(f"📎 Generated SAS URL")

        # Validate the SAS URL
        if not validate_sas_url(blob_sas_url):
            raise RuntimeError("Generated SAS URL is not accessible")

        return blob_sas_url
        
    except Exception as e:
        print(f"❌ Error in upload_to_blob: {str(e)}")
        raise







    








# import os
# from dotenv import load_dotenv
# from azure.storage.blob import (
#     BlobServiceClient,
#     generate_blob_sas,
#     BlobSasPermissions
# )
# from datetime import datetime, timedelta
# from urllib.parse import quote

# # Load environment variables
# load_dotenv()

# def upload_to_blob(file_name: str, file_bytes: bytes) -> str:
#     try:
#         CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
#         CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "invoices")
#         ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
#         ACCOUNT_KEY = os.getenv("AZURE_STORAGE_KEY")
        
#         if not all([CONN_STR, ACCOUNT_NAME, ACCOUNT_KEY]):
#             raise ValueError("Missing required Azure Storage environment variables")

#         print(f"🔍 Using Storage Account: {ACCOUNT_NAME}")
#         print(f"🔍 Container: {CONTAINER}")

#         # Connect to blob service
#         client = BlobServiceClient.from_connection_string(CONN_STR)
#         container_client = client.get_container_client(CONTAINER)

#         # Upload blob
#         blob_client = container_client.get_blob_client(file_name)
#         print(f"📤 Uploading file: {file_name}")
#         blob_client.upload_blob(file_bytes, overwrite=True)

#         # ✅ FIXED: Generate SAS token with proper permissions and timing
#         sas_token = generate_blob_sas(
#             account_name=ACCOUNT_NAME,
#             container_name=CONTAINER,
#             blob_name=file_name,
#             account_key=ACCOUNT_KEY,
#             permission=BlobSasPermissions(read=True),
#             expiry=datetime.utcnow() + timedelta(hours=2),  # 2 hours expiry
#             start=datetime.utcnow() - timedelta(minutes=5)  # Start 5 minutes ago to avoid clock skew
#         )

#         # ✅ FIXED: Properly encode the blob name in URL
#         encoded_blob_name = quote(file_name)
#         blob_sas_url = f"https://{ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER}/{encoded_blob_name}?{sas_token}"

#         print("✅ Blob uploaded successfully!")
#         print(f"📎 SAS URL generated (length: {len(blob_sas_url)})")
#         print(f"🔗 URL starts with: {blob_sas_url[:80]}...")

#         return blob_sas_url
        
#     except Exception as e:
#         print(f"❌ Error in upload_to_blob: {str(e)}")
#         raise
































































# import os
# from datetime import datetime, timedelta
# from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
# from dotenv import load_dotenv

# load_dotenv()

# def upload_to_blob(file_name: str, file_bytes: bytes) -> str:
#     conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
#     container = os.getenv("AZURE_CONTAINER_NAME")
#     account = os.getenv("AZURE_STORAGE_ACCOUNT")

#     client = BlobServiceClient.from_connection_string(conn_str)
#     container_client = client.get_container_client(container)
#     blob_client = container_client.get_blob_client(file_name)

#     # Upload file (overwrite if exists)
#     blob_client.upload_blob(file_bytes, overwrite=True)

#     # ✅ Generate temporary SAS URL (valid for 1 hour)
#     sas_token = generate_blob_sas(
#         account_name=account,
#         container_name=container,
#         blob_name=file_name,
#         permission=BlobSasPermissions(read=True),
#         expiry=datetime.utcnow() + timedelta(hours=1)
#     )

#     # Return full accessible SAS URL
#     return f"https://{account}.blob.core.windows.net/{container}/{file_name}?{sas_token}"
