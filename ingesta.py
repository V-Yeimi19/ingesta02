import os
import sys
import csv
from typing import Optional

try:
    import mysql.connector as mysql
except ImportError:
    print("ERROR: Falta la librería 'mysql-connector-python'. Instálala con:\n    pip install mysql-connector-python", file=sys.stderr)
    raise

try:
    import boto3
except ImportError:
    print("ERROR: Falta la librería 'boto3'. Instálala con:\n    pip install boto3", file=sys.stderr)
    raise

# --- Configuración fija (ajústala a tu entorno) ---
DEFAULTS = {
    "MYSQL": {
        "HOST": "localhost",
        "PORT": 3306,
        "USER": "yvarela",
        "PASSWORD": "tu_password_segura",
        "DATABASE": "taller6",
    },
    "CSV": {
        "OUTFILE": "/tmp/data.csv",
        "BATCH_SIZE": 1000,
    },
    "S3": {
        "BUCKET": "yvarela2",
        "REGION": "us-east-1",
    },
    "TABLE": "data",   # 👈 aquí definimos directamente la tabla
}


def export_table_to_csv():
    conn = None
    cur = None
    try:
        conn = mysql.connect(
            host=DEFAULTS["MYSQL"]["HOST"],
            port=DEFAULTS["MYSQL"]["PORT"],
            user=DEFAULTS["MYSQL"]["USER"],
            password=DEFAULTS["MYSQL"]["PASSWORD"],
            database=DEFAULTS["MYSQL"]["DATABASE"],
        )
        cur = conn.cursor()
        q = f"SELECT * FROM {DEFAULTS['TABLE']}"
        cur.execute(q)

        column_names = [desc[0] for desc in cur.description]
        with open(DEFAULTS["CSV"]["OUTFILE"], "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(column_names)
            while True:
                rows = cur.fetchmany(size=DEFAULTS["CSV"]["BATCH_SIZE"])
                if not rows:
                    break
                writer.writerows(rows)
        print(f"✅ Exportación a CSV completada: {DEFAULTS['CSV']['OUTFILE']}")
    finally:
        if cur: cur.close()
        if conn: conn.close()


def upload_to_s3():
    s3 = boto3.client("s3", region_name=DEFAULTS["S3"]["REGION"])
    key = os.path.basename(DEFAULTS["CSV"]["OUTFILE"])
    s3.upload_file(DEFAULTS["CSV"]["OUTFILE"], DEFAULTS["S3"]["BUCKET"], key)
    print(f"✅ Archivo subido a s3://{DEFAULTS['S3']['BUCKET']}/{key}")


def main():
    export_table_to_csv()
    upload_to_s3()
    print("🎯 Ingesta completada")


if __name__ == "__main__":
    main()
