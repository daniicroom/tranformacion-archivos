import pyarrow.parquet as pq
import boto3 as b3


def read_txt(file_txt):
    with open(file_txt, 'r') as file:
        files_data = file.readlines()
    return [data.strip() for data in files_data]

#upload data
file_txt = 'C:/Users/User/Downloads/docs_ducol_vencer.txt'
list_data = read_txt(file_txt)

def descargar_archivos_desde_s3(bucket_nombre, lista_nombres, destino_carpeta, aws_access_key, aws_secret_key):
    s3 = b3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

    for file in lista_nombres:
        data = file.split(" ; ");
        name = data[0]+".pdf";
        nameFile = data[1]
        if ".pdf" not in nameFile:
            nameFile = data[1]+".pdf";
        origen = f"s3://{bucket_nombre}/{name}"
        destino = f"{destino_carpeta}{name}"

        try:
            print(f"Descargando {name}...")
            s3.download_file(bucket_nombre, "Documents/"+name, destino)
            print(f"{name} descargado exitosamente.")
        except Exception as e:
            print(f"No se pudo descargar {name}. Error: {str(e)}")

if __name__ == "__main__":
    # Configuración
    bucket_nombre = 'prod-fs-bucket'
    destino_carpeta = 'C:/Users/User/Downloads/DucolFiles/'
    aws_access_key = '{acces_key}'
    aws_secret_key = '{secret_key}'

    descargar_archivos_desde_s3(bucket_nombre, list_data, destino_carpeta, aws_access_key, aws_secret_key)


