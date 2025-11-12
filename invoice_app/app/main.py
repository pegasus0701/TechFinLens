# # app/main.py
# import os
# import json
# from fastapi import FastAPI, UploadFile, File, Request
# from fastapi.responses import JSONResponse, HTMLResponse
# from fastapi.staticfiles import StaticFiles
# from fastapi.templating import Jinja2Templates
# from fastapi.middleware.cors import CORSMiddleware
# from dotenv import load_dotenv

# load_dotenv()

# app = FastAPI(title="Invoice Processing Pipeline")

# # Setup templates
# templates = Jinja2Templates(directory="templates")

# # Enable CORS
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # Import your functions
# from app.blob import upload_to_blob
# from app.document import analyze_invoice_and_save

# @app.get("/", response_class=HTMLResponse)
# async def index(request: Request):
#     return templates.TemplateResponse("index.html", {"request": request})

# @app.post("/upload_blob")
# async def upload_blob(file: UploadFile = File(...)):
#     try:
#         print(f"📤 Uploading file: {file.filename}")
#         file_bytes = await file.read()
        
#         # Use your existing upload_to_blob function
#         blob_sas_url = upload_to_blob(file.filename, file_bytes)
        
#         return {"blob_url": blob_sas_url, "filename": file.filename}
        
#     except Exception as e:
#         print(f"❌ Upload Error: {str(e)}")
#         return JSONResponse(
#             status_code=500,
#             content={"error": f"Upload failed: {str(e)}"}
#         )

# @app.post("/analyze_invoice")
# async def analyze_invoice(data: dict):
#     try:
#         blob_url = data.get("blob_sas_url")
#         if not blob_url:
#             return {"error": "Missing blob_sas_url"}
        
#         print(f"🔍 Analyzing invoice: {blob_url}")
#         result = analyze_invoice_and_save(blob_url)
#         return result
        
#     except Exception as e:
#         print(f"❌ Analysis Error: {str(e)}")
#         return JSONResponse(
#             status_code=500,
#             content={"error": f"Analysis failed: {str(e)}"}
#         )

# @app.get("/health")
# async def health_check():
#     return {"status": "healthy", "service": "Invoice Processor"}

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)





# app/main.py
import os
from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from azure.storage.blob import BlobServiceClient
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from .blob import upload_to_blob
from app.document import analyze_invoice_and_save
from .models import create_table
from .db import get_connection

load_dotenv()

app = FastAPI(title="Invoice Processing Pipeline")

# Setup templates and static folders
if not os.path.isdir("templates"):
    os.makedirs("templates")

templates = Jinja2Templates(directory="templates")

# # Serve static assets if available
# if os.path.isdir("static"):
#     app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Enable CORS (for frontend communication)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# # --- Start up event: create table safely ---
# @app.on_event("startup")
# def startup():
#     try:
#         create_table()
#     except Exception as e:
#         print("⚠️ Warning: Table creation skipped ->", e)

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ENV values
AZURE_STORAGE_CONN = os.getenv("AZURE_STORAGE_CONN")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER", "invoices")

@app.post("/upload_blob")
async def upload_blob(file: UploadFile = File(...)):
    try:
        from azure.storage.blob import BlobServiceClient
        from dotenv import load_dotenv
        load_dotenv()

        conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        print("🔍 Connection string:", conn_str[:50], "...")  # just for check

        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        container_name = "invoices"

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file.filename)
        blob_client.upload_blob(file.file, overwrite=True)

        blob_sas_url = blob_client.url
        print("✅ Uploaded to:", blob_sas_url)
        return {"blob_url": blob_sas_url}
    except Exception as e:
        print("❌ Upload Error:", str(e))
        return {"error": str(e)}


@app.post("/analyze_invoice")
async def analyze_invoice(data: dict):
    try:
        blob_url = data.get("blob_sas_url")
        if not blob_url:
            return {"error": "Missing blob_url"}
        result = analyze_invoice_and_save(blob_url)
        return result
    except Exception as e:
        return {"error": str(e)}































































# --- Upload Invoice Endpoint ---
# @app.post("/upload_invoice")
# async def upload_invoice(file: UploadFile = File(...)):
#     try:
#         file_bytes = await file.read()
#         print(f"📄 Uploading file: {file.filename}")

#         blob_url = upload_to_blob(file.filename, file_bytes)
#         print(f"✅ Blob uploaded at: {blob_url}")

#         extracted_data = analyze_invoice_and_save(blob_url)
#         print("🧠 Extracted Data:", extracted_data)

#         return {
#             "message": "✅ Invoice processed successfully!",
#             "blob_url": blob_url,
#             "data": extracted_data
#         }

#     except Exception as e:
#         print("❌ FULL ERROR TRACEBACK:")
#         import traceback
#         traceback.print_exc()
#         return JSONResponse({"error": f"❌ Internal Server Error: {str(e)}"}, status_code=500)

# from fastapi import FastAPI, Request, UploadFile
# from fastapi.responses import HTMLResponse
# from fastapi.staticfiles import StaticFiles
# from fastapi.templating import Jinja2Templates
# from fastapi import FastAPI, File, UploadFile
# from app.blob import upload_to_blob
# import psycopg2
# import os
# from dotenv import load_dotenv

# load_dotenv()

# app = FastAPI()

# # ✅ Static + Templates paths (make sure folder names match)
# app.mount("/static", StaticFiles(directory="static"), name="static")
# templates = Jinja2Templates(directory="templates")

# # ✅ Database connection (we’ll leave it, but focus now on frontend)
# def get_connection():
#     return psycopg2.connect(
#         host=os.getenv("PG_HOST"),
#         dbname=os.getenv("PG_DB"),
#         user=os.getenv("PG_USER"),
#         password=os.getenv("PG_PASSWORD"),
#         port=os.getenv("PG_PORT", 5432),
#         sslmode="require"
#     )

# # ✅ Home route should serve HTML
# @app.get("/", response_class=HTMLResponse)
# async def home(request: Request):
#     return templates.TemplateResponse("index.html", {"request": request})

# @app.post("/upload_invoice")
# async def upload_invoice(file: UploadFile = File(...)):
#     file_bytes = await file.read()
#     blob_url = upload_to_blob(file.filename, file_bytes)
#     return {"message": "✅ File uploaded successfully!", "blob_url": blob_url}