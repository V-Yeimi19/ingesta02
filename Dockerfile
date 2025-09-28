FROM python:3-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /programas/ingesta

# Instalar dependencias: MySQL connector y boto3 para S3
RUN pip3 install --no-cache-dir \
    mysql-connector-python \
    boto3

# Copiar el código dentro de la imagen
COPY . .

# Ejecutar el script; los argumentos se pueden pasar con `docker run ... --table ...`
CMD ["python3", "./ingesta.py"]
