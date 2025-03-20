from flask import Flask, request, render_template
import pandas as pd
from supabase import create_client, Client

app = Flask(__name__)

# Configuración de Supabase
SUPABASE_URL = 'https://whsxsuhcivmgotpkexak.supabase.co'  # URL de Supabase
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Indoc3hzdWhjaXZtZ290cGtleGFrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDI0Nzg1NTQsImV4cCI6MjA1ODA1NDU1NH0.9Vwz6ZDbhUp0E6CKwfKCIBL2ky6GI5uln1twZzJzVkE'  # Clave de API de Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Función para procesar un archivo y subir datos a Supabase
def procesar_archivo(df):
    # Verificar que las columnas del archivo coincidan con las de la tabla
    columnas_requeridas = {'nombre', 'unidad', 'cantidad', 'calidad', 'formato', 'img_url', 'presentacion'}
    if not columnas_requeridas.issubset(df.columns):
        raise Exception(f"El archivo no contiene las columnas requeridas: {columnas_requeridas}")
    
    # Subir los datos a Supabase
    subir_a_supabase(df)

# Función para subir datos a Supabase
def subir_a_supabase(df):
    # Eliminar la columna 'product_id' si existe
    if 'product_id' in df.columns:
        df = df.drop(columns=['product_id'])
    
    # Reemplazar NaN con None (o un valor predeterminado)
    df = df.where(pd.notnull(df), None)
    
    # Convertir el DataFrame a un formato que Supabase pueda manejar (lista de diccionarios)
    records = df.to_dict('records')
    
    # Insertar los registros en la tabla de Supabase
    response = supabase.table('productos').insert(records).execute()
    
    # Verificar si la respuesta contiene errores
    if hasattr(response, 'is_error') and response.is_error:
        raise Exception(f"Error al subir datos a Supabase: {response.error}")
    elif hasattr(response, 'error') and response.error:
        raise Exception(f"Error al subir datos a Supabase: {response.error}")
    elif not response.data:
        raise Exception("No se recibieron datos en la respuesta de Supabase")

# Ruta principal que muestra el formulario
@app.route('/')
def index():
    return render_template('index.html', message=None, message_type=None)  # Inicializa las variables como None

# Ruta para manejar la carga de archivos y procesamiento
@app.route('/procesar', methods=['POST'])
def procesar_archivos_route():
    if 'files' not in request.files:
        return render_template('index.html', message="No se han subido archivos", message_type="error")
    
    archivos = request.files.getlist('files')

    if not archivos or all(archivo.filename == '' for archivo in archivos):
        return render_template('index.html', message="No se han seleccionado archivos", message_type="error")

    try:
        for archivo in archivos:
            if archivo.filename.endswith('.csv'):
                df = pd.read_csv(archivo)
            elif archivo.filename.endswith('.xlsx'):
                df = pd.read_excel(archivo)
            else:
                continue  # Saltar archivos no soportados

            # Procesar el archivo y subir los datos a Supabase
            procesar_archivo(df)

        return render_template('index.html', message="Datos procesados y subidos a Supabase correctamente", message_type="success")
    except Exception as e:
        return render_template('index.html', message=str(e), message_type="error")

if __name__ == '__main__':
    app.run(debug=True)