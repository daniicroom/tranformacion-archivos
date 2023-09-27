import pyarrow.parquet as pq

# Cargar el archivo Parquet con compresión Gzip
table = pq.read_table('part-00000-2f26f00d-6e0b-43e1-abb6-4099208a326c-c000.gz.parquet')

# Convertir la tabla en un DataFrame de pandas (opcional)
df = table.to_pandas()

# Ahora puedes trabajar con los datos en df
df.to_excel('datos.xlsx', index=False)