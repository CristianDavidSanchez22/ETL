FROM python:3.11-slim

# Evita interacciones durante instalación
ENV DEBIAN_FRONTEND=noninteractive

# Instala dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    libhdf5-dev \
    libnetcdf-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia archivos del proyecto
WORKDIR /app
COPY . .

# Instala paquetes de Python
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Comando por defecto (puedes sobreescribirlo)
CMD ["python", "etl.py"]
