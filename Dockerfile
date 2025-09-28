FROM python:3.11-slim

WORKDIR /app

# Copiamos requirements (si los tienes) o instalamos directo
RUN pip install --no-cache-dir mysql-connector-python boto3

# Copiamos el script
COPY ingesta.py .

# Punto de entrada por defecto
CMD ["python", "ingesta.py"]
