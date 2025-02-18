import pyarrow.parquet as pq
import boto3 as b3
import os
import re
from datetime import datetime

def read_txt(file_txt):
    with open(file_txt, 'r') as file:
        files_data = file.readlines()
    return [data.strip() for data in files_data]

#upload data
file_txt = 'D:/Documents/Firma/Extract documents/tranformacion-archivos/docs_mision-rename.txt'
list_data = read_txt(file_txt)

def descargar_archivos_desde_s3(bucket_nombre, lista_nombres, destino_carpeta, aws_access_key, aws_secret_key):
    s3 = b3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    contador = 0
    for file in lista_nombres:
        data = file.split(" ; ");
        name = data[0]+".pdf";
        nameFile = data[1].replace('"', "");
        date = data[2].replace('"', "");
        if ".pdf" not in nameFile:
            nameFile = data[1]+".pdf";

        # Extraer la fecha del nombre del archivo (suponiendo formato YYYYMMDD)
        match = re.search(r"(\d{4})[-]?(\d{2})[-]?\d{2}", date)
        if match:
            year, month = match.groups()
            carpeta_mes = os.path.join(destino_carpeta, f"{year}-{month}")
        else:
            carpeta_mes = os.path.join(destino_carpeta, "sin_fecha")
        
        # Crear la carpeta si no existe
        os.makedirs(carpeta_mes, exist_ok=True)

        destino = os.path.join(carpeta_mes, nameFile)

        try:
            print(f"Descargando {name}...")
            s3.download_file(bucket_nombre, "Documents/"+name, destino)
            print(f"{name} descargado exitosamente.")
        except Exception as e:
            contador += 1
            print(f"No se pudo descargar {name}. Error: {str(e)}")
    print(f"Errores {contador} de: {lista_nombres.__len__()}")

if __name__ == "__main__":
    # Configuración
    bucket_nombre = 'prueba-bucket'
    destino_carpeta = 'D:/Documents/Firma/Extract documents/tranformacion-archivos/MisionFiles/'
    aws_access_key = '{access_key}'
    aws_secret_key = '{secret_key}'

    descargar_archivos_desde_s3(bucket_nombre, list_data, destino_carpeta, aws_access_key, aws_secret_key)


