# app/models.py
from .db import get_connection

def create_table():
    """
    Creates invoices table if not exists.
    Columns: id, file_name, blob_url, vendor_name, invoice_id, invoice_date, total, uploaded_at
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id SERIAL PRIMARY KEY,
            file_name TEXT,
            blob_url TEXT,
            vendor_name TEXT,
            invoice_id TEXT,
            invoice_date TEXT,
            total TEXT,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Table 'invoices' ready")
    except Exception as e:
        # don't crash startup if DB unreachable; log and continue
        print("❌ create_table error:", e)
