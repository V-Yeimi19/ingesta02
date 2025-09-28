import os
import sys
import csv
import argparse
from typing import Optional

# Dependencias requeridas:
#  - mysql-connector-python
#  - boto3
try:
    import mysql.connector as mysql
except ImportError:
    print(
        "ERROR: Falta la librería 'mysql-connector-python'. Instálala con:\n"
        "    pip install mysql-connector-python",
        file=sys.stderr,
    )
    raise

try:
    import boto3
except ImportError:
    print(
        "ERROR: Falta la librería 'boto3'. Instálala con:\n"
        "    pip install boto3",
        file=sys.stderr,
    )
    raise

# Configuración por defecto incrustada en el código (reemplaza los valores CHANGE_ME por los reales)
DEFAULTS = {
    "MYSQL": {
        "HOST": "localhost",
        "PORT": 3306,
        "USER": "yvarela",
        "PASSWORD": "yvarela123",      # reemplaza por tu contraseña
        "DATABASE": "taller6",    # reemplaza por tu base de datos
    },
    "CSV": {
        "OUTFILE": "data.csv",
        "BATCH_SIZE": 2,
    },
    "S3": {
        "BUCKET": "yvarela2",     # reemplaza por tu bucket
        "KEY": None,                    # opcional; por defecto usa el nombre del outfile
        "REGION": None,                 # opcional; ejemplo: "us-east-1"
    },
}


def quote_identifier(name: str) -> str:
    """Valida y cita un identificador de MySQL (simple)."""
    import re

    if not re.match(r"^[0-9A-Za-z_\$]+$", name):
        raise ValueError(f"Identificador inválido: {name!r}")
    return f"`{name}`"


def export_table_to_csv(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    table: str,
    outfile: str,
    batch_size: int = 1000,
) -> None:
    """Conecta a MySQL, lee todos los registros de una tabla y los guarda en CSV."""
    conn = None
    cur = None
    try:
        conn = mysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )
        cur = conn.cursor()
        q = f"SELECT * FROM {quote_identifier(table)}"
        cur.execute(q)

        column_names = [desc[0] for desc in cur.description]
        # Aseguramos directorio de salida
        os.makedirs(os.path.dirname(outfile) or ".", exist_ok=True)
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(column_names)
            while True:
                rows = cur.fetchmany(size=batch_size)
                if not rows:
                    break
                writer.writerows(rows)
        print(f"Exportación a CSV completada: {outfile}")
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def upload_to_s3(filepath: str, bucket: str, key: Optional[str] = None, aws_region: Optional[str] = None) -> None:
    """Sube un archivo local a un bucket S3."""
    key = key or os.path.basename(filepath)
    if aws_region:
        s3 = boto3.client("s3", region_name=aws_region)
    else:
        s3 = boto3.client("s3")
    s3.upload_file(filepath, bucket, key)
    print(f"Archivo subido a s3://{bucket}/{key}")


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exporta una tabla de MySQL a un CSV local y lo sube a un bucket S3. "
            "Los valores por defecto están incrustados en el código y pueden sobrescribirse con flags."
        )
    )
    # Parámetros MySQL (con valores por defecto incrustados)
    parser.add_argument("--host", default=DEFAULTS["MYSQL"]["HOST"], help="Host de MySQL")
    parser.add_argument("--port", type=int, default=DEFAULTS["MYSQL"]["PORT"], help="Puerto de MySQL")
    parser.add_argument("--user", default=DEFAULTS["MYSQL"]["USER"], help="Usuario de MySQL")
    parser.add_argument(
        "--password",
        default=DEFAULTS["MYSQL"]["PASSWORD"],
        help="Password de MySQL",
    )
    parser.add_argument(
        "--database",
        default=DEFAULTS["MYSQL"]["DATABASE"],
        help="Base de datos",
    )
    parser.add_argument("--table", required=True, help="Nombre de la tabla a exportar")

    # Salida CSV
    parser.add_argument(
        "--outfile",
        default=DEFAULTS["CSV"]["OUTFILE"],
        help="Ruta del archivo CSV de salida",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULTS["CSV"]["BATCH_SIZE"],
        help="Tamaño de lote para leer filas",
    )

    # Parámetros S3
    parser.add_argument("--bucket", default=DEFAULTS["S3"]["BUCKET"], help="Bucket S3 destino")
    parser.add_argument(
        "--s3-key",
        default=DEFAULTS["S3"]["KEY"],
        help="Key/objeto destino en S3 (por defecto usa el nombre de outfile)",
    )
    parser.add_argument(
        "--aws-region",
        default=DEFAULTS["S3"]["REGION"],
        help="Región AWS (opcional)",
    )

    args = parser.parse_args(argv)
    return args


def main(argv: Optional[list] = None) -> None:
    args = parse_args(argv)

    # 1) Exportar tabla a CSV local
    export_table_to_csv(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        table=args.table,
        outfile=args.outfile,
        batch_size=args.batch_size,
    )

    # 2) Subir CSV a S3
    key = args.s3_key if args.s3_key else os.path.basename(args.outfile)
    upload_to_s3(args.outfile, args.bucket, key=key, aws_region=args.aws_region)

    print("Ingesta completada")


if __name__ == "__main__":
    main()



