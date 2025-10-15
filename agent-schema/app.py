import os
import psycopg2
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from sqlalchemy import create_engine,text

# --- Gemini ---
import google.generativeai as genai

# Load env vars (for local and Docker)
load_dotenv()

app = FastAPI(title="Agent Schema API", description="Auto table creator powered by Gemini")

# --- Config ---
AGENT_API_KEY = os.getenv("AGENT_API_KEY")
DISABLE_LLM = os.getenv("DISABLE_LLM", "True").lower() == "true"
GEMINI_API_KEY = os.getenv("GOOGLE_API_KEY")



# --- Models ---
class NiFiSchemaRequest(BaseModel):
    api_key: str
    file_path: str
    table_name: str
    db_type: str = "postgres" 


# --- Helpers ---
def infer_sql_type(dtype: str) -> str:
    dtype = str(dtype).lower()
    if "int" in dtype:
        return "INT"
    elif "float" in dtype or "double" in dtype:
        return "FLOAT"
    elif "datetime" in dtype or "timestamp" in dtype:
        return "TIMESTAMP"
    elif "bool" in dtype:
        return "BOOLEAN"
    else:
        return "VARCHAR(255)"


def connect_db(db_type="postgres"):
    """Return a SQLAlchemy connection object for the given DB type."""
    if db_type == "postgres":
        db_url = os.getenv("POSTGRES_URL")
    elif db_type == "mysql":
        db_url = os.getenv("MYSQL_URL")
    else:
        raise ValueError(f"Unsupported db_type: {db_type}")

    engine = create_engine(db_url)
    conn = engine.connect()
    return conn


def get_existing_columns(schema_name: str, table_name: str, db_type :str ):
    """Return list of existing columns for any supported SQL dialect."""
    conn = connect_db(db_type)
    query = text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = :tbl_name
        AND (:schema_name IS NULL OR table_schema = :schema_name)
    """)
    result = conn.execute(query, {"tbl_name": table_name, "schema_name": schema_name})
    cols = [{"name": row[0], "type": row[1]} for row in result]
    conn.close()
    return cols




def generate_gemini_sql(table_name: str, inferred_schema: list, existing_schema: list, file_path: str, schema_name: str = None ,db_type: str = "postgres"):
    """
    Use Gemini to compare inferred vs existing schema and generate
    clean, executable SQL (no markdown or code fences).
    generate safe SQL with dynamic schema inference.
    """
    dialect = "MySQL" if db_type == "mysql" else "PostgreSQL"
    try:
        
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-2.0-flash-lite")
        
        if schema_name:
            schema_context = f"The table name is {table_name} inside schema '{schema_name}'."
        else:
            schema_context = f"The table name is {table_name}, and no schema is provided. Infer a logical schema name (e.g., raw, staging, analytics) based on the file path {file_path}."

        prompt = f"""
        You are a **{dialect} DDL generator** for an automated ETL pipeline.
        {schema_context}

        Compare the existing database schema with the new inferred schema from a CSV file.

        Existing columns: {existing_schema}
        Desired schema: {inferred_schema}

        Your goals:
        1️. Describe what changed (added, missing, or mismatched columns).
        2️. Generate valid SQL statements that:
           - When generating ALTER TABLE statements, do not rename columns if the only difference is letter casing
           
           - Create the schema if missing.
           - Generate only valid SQL for {dialect} amd Replace invalid characters like hyphens (-) or spaces with underscores (_).
           - If no schema name is provided, infer one logically from the file or data context (e.g., raw, staging, or analytics) and create it if needed.
           - Create the table if it does not exist.
           - Add only missing columns (do NOT drop or rename existing ones).
           - Avoid duplicates or redundant changes.
        3️. Output format must be **plain text only**, exactly like this:

        Explanation:
        (brief summary of detected differences)

        SQL:
        (clean SQL ready for direct execution)

        DO NOT use Markdown formatting or code fences such as ```sql.
        """

        # Send prompt to Gemini
        response = model.generate_content(prompt)
        text = response.text.strip()

        print("\n🧠 [Gemini] Prompt Sent ---------------------------")
        print(prompt)
        print("--------------------------------------------------")
        print("✅ [Gemini] Raw Response --------------------------")
        print(text)
        print("--------------------------------------------------")

        # Extract SQL section
        ddl = text
        if "SQL:" in text:
            ddl = text.split("SQL:")[1].strip()

        # Fallback cleaning for stray markdown
        ddl = ddl.replace("```", "").replace("sql", "").strip()

        print("✅ [Gemini] SQL Cleaned --------------------------")
        print(ddl)
        print("--------------------------------------------------")

        return ddl

    except Exception as e:
        print(f"⚠️ Gemini failed or disabled: {e}")
        return None





@app.get("/health")
def health():
    return {
        "status": "ok",
        "llm_enabled": not DISABLE_LLM
    }


@app.post("/nifi_table_create")
def create_table_from_nifi(request: NiFiSchemaRequest):
    
    """Main endpoint for NiFi or Airflow to call."""
    if request.api_key != AGENT_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    file_path = request.file_path
    table_name = request.table_name

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")

    try:
        df = pd.read_csv(file_path, nrows=100)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file: {str(e)}")

    # Infer schema
    inferred_schema = [{"name": col, "type": infer_sql_type(dtype)} for col, dtype in df.dtypes.items()]

    # Split schema.table if needed
    # Split schema and table if user provided schema.table
    if "." in table_name:
        schema_name, tbl_name = table_name.split(".", 1)
    else:
        schema_name, tbl_name = None, table_name  # Let Gemini decide schema if missing


    existing_schema = get_existing_columns(schema_name, tbl_name , request.db_type)

    # Use Gemini if enabled
    ddl = None
    if not DISABLE_LLM and GEMINI_API_KEY:
        ddl = generate_gemini_sql(table_name, inferred_schema, existing_schema, file_path, schema_name, db_type=request.db_type)


    # Fallback simple DDL
    if not ddl:
        if NiFiSchemaRequest.db_type == "mysql":
            # MySQL doesn't use schemas the same way; use CREATE DATABASE + USE
            database_name = schema_name or "default_db"
            ddl = f"CREATE DATABASE IF NOT EXISTS {database_name};\nUSE {database_name};\n"
            ddl += f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
            ddl += ",\n".join([f"{c['name']} {c['type']}" for c in inferred_schema])
            ddl += "\n);"
        else:
            # Default to PostgreSQL-style fallback
            schema_clause = f"CREATE SCHEMA IF NOT EXISTS {schema_name};" if schema_name else ""
            ddl = f"{schema_clause}\nCREATE TABLE IF NOT EXISTS {table_name} (\n"
            ddl += ",\n".join([f"{c['name']} {c['type']}" for c in inferred_schema])
            ddl += "\n);"



    # Execute SQL
    try:
        conn = connect_db(request.db_type)
        
        for stmt in ddl.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))
        
        conn.commit()
        conn.close()
        
        print(f"✅ [Agent] Gemini used: {not DISABLE_LLM}, DB: {request.db_type}, Table: {table_name}")
        return {
            "message": f"✅ Table '{table_name}' checked/created successfully.",
            "rows_sampled": len(df),
            "llm_used": not DISABLE_LLM,
            "db_type": request.db_type,
            "ddl": ddl,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


    
    
