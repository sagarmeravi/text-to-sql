import os
import re
from flask import Flask, request, jsonify, render_template
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from google import genai

load_dotenv()

app = Flask(__name__)
client = genai.Client(api_key=os.getenv("GENAI_API_KEY"))

# -----------------------------
# DB CONNECTION
# -----------------------------
DB_URI = (
    f"mysql+pymysql://{os.getenv('MYSQL_USER')}:"
    f"{os.getenv('MYSQL_PASS')}@{os.getenv('MYSQL_HOST')}/"
    f"{os.getenv('MYSQL_DB')}"
)
engine = create_engine(DB_URI, future=True)

# -----------------------------
# HELPERS
# -----------------------------
SELECT_RE = re.compile(r"\bSELECT\b", re.IGNORECASE)

def extract_first_select(text):
    if not text:
        return None
    txt = text.strip()
    m = SELECT_RE.search(txt)
    if not m:
        return None
    sql = txt[m.start():]
    return sql.split(";")[0].strip()

def safe(sql):
    if not sql or not sql.upper().startswith("SELECT"):
        return False
    forbidden = [
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
        "TRUNCATE", "CREATE", "REPLACE", "RENAME",
        "MERGE", "CALL", "GRANT", "REVOKE",
        "DESCRIBE", "SHOW", "SET", "USE"
    ]
    up = sql.upper()
    return not any(word in up for word in forbidden)

def get_schema():
    insp = inspect(engine)
    lines = []
    for t in insp.get_table_names():
        cols = insp.get_columns(t)
        col_str = ", ".join(f"{c['name']} ({c['type']})" for c in cols)
        lines.append(f"Table {t}: {col_str}")
    return "\n".join(lines)

def build_prompt(schema, question):
    return f"""
You are an expert MySQL SQL generator.
Return ONLY ONE MySQL SELECT query. No explanation.
Rules:
- Use only SELECT (no INSERT/UPDATE/DELETE/DDL).
- No comments.
- No backticks.
- No explanation.
- If impossible, return exactly: UNABLE_TO_ANSWER
Schema:
{schema}

Question:
{question}

Return ONLY the SQL:
"""


# -----------------------------
# ROUTES
# -----------------------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/schema")
def schema_route():
    return jsonify({"schema": get_schema()})

@app.route("/ask", methods=["POST"])
def ask():
    question = (request.get_json() or {}).get("question", "").strip()
    if not question:
        return jsonify({"error": "Question is required."}), 400

    prompt = build_prompt(get_schema(), question)

    # LLM CALL
    try:
        res = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt
        )
        raw = res.text
    except Exception as e:
        return jsonify({"error": "LLM error", "details": str(e)}), 500

    print("RAW FROM GEMINI:", raw)

    sql = extract_first_select(raw)
    if not sql:
        return jsonify({"error": "No valid SQL found"}), 400

    sql = sql.replace("`", "").replace('"', "").strip()

    if not safe(sql):
        return jsonify({"sql": sql, "error": "Unsafe SQL"}), 400

    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            mapped = result.mappings().all()
            rows = [dict(r) for r in mapped]
            columns = list(mapped[0].keys()) if mapped else []
    except Exception as e:
        return jsonify({"sql": sql, "error": "SQL error", "details": str(e)})

    return jsonify({
        "sql": sql,
        "result": {
            "columns": columns,
            "rows": rows
        }
    })

if __name__ == "__main__":
    app.run(debug=True)
