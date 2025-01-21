from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext, ConversationHandler, JobQueue
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import urllib.parse
import logging
import pandas as pd
from datetime import datetime, timedelta
from threading import Thread
import threading
import time
import asyncio
import io



# Definir los estados
REGISTRO, PLANIFICACION, SELECCIONAR_OPCION, SELECCIONAR_MARCA, INGRESAR_MEDIDOR = range(5)

# Configuración de logging
logging.basicConfig(
    level = logging.INFO, # Nivel minimo de severidad pa  ra registrar 
    format='%(asctime)s - %(levelname)s - %(message)s', # Formato de registro
    filename='bot.log', # Nombre del archivo del registro
    filemode='a' # Modo de archivo: 'a' para agregar, 'w' para sobreescribir
)

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Conexión a la base de datos usando SQLAlchemy
usuario = os.getenv('DB_USER')
contrasena = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
base_datos = os.getenv('DB_NAME')

logging.info("CODIFICANDO CONTRASEÑA")
encoded_password = urllib.parse.quote_plus(contrasena)

ssl_args = {
    'ssl_cert': os.getenv('SSL_CERT_PATH'),
    'ssl_key': os.getenv('SSL_KEY_PATH')
}
engine = create_engine(f"mysql+pymysql://{usuario}:{encoded_password}@{host}/{base_datos}", connect_args=ssl_args)


def solicitud_query(QUERY):
    """
    Ejecuta una consulta SQL y devuelve el resultado como un DataFrame de pandas.

    Args:
        QUERY (str): Consulta SQL a ejecutar.

    Returns:
        pd.DataFrame: Resultado de la consulta en forma de DataFrame.
    """
    df = pd.read_sql_query(QUERY, engine)
    return df

async def start(update: Update, context: CallbackContext):
    """
    Inicia el proceso de registro. Verifica si el usuario ya está registrado.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del flujo de conversación.
    """
    user_id = update.message.from_user.id
    usuarios = solicitud_query("SELECT * FROM bot_usuarios_autorizados;")
    iD_TELEGRAM = usuarios['ID_TELEGRAM'].tolist()

    if int(user_id) not in iD_TELEGRAM:
        await update.message.reply_text('¡Bienvenido! Por favor, ingresa tu nombre completo para validar tus datos:')
        return REGISTRO
    else:
        await update.message.reply_text('Ya estás registrado. Usa el comando /menu para acceder a las opciones.')
        return ConversationHandler.END

async def registro(update: Update, context: CallbackContext):
    """
    Maneja el registro de un nuevo usuario.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del flujo de conversación.
    """
    user_name = update.message.text
    user_id = update.message.from_user.id
    user_name_id = update.message.from_user.username
    user_first_name = update.message.from_user.first_name

    try:
        with engine.begin() as con:
            query_update = text("""
                UPDATE bot_usuarios_autorizados 
                SET ID_TELEGRAM=:user_id,
                ROL='SUPERVISOR'
                WHERE NOMBRE_COMPLETO=:user_name 
                OR NOMBRE_TELEGRAM=:user_first_name 
                OR USUARIO_TELEGRAM=:user_name_id
            """)
            con.execute(query_update, {'user_id': user_id, 'user_name': user_name, 'user_first_name': user_first_name, 'user_name_id': user_name_id})
            await update.message.reply_text(f'Te has registrado con éxito como {user_name}. Usa el comando /menu para acceder a las opciones.')
    except SQLAlchemyError as e:
        print(e)
        logging.error(f"Error al registrar usuario: {e}")
        await update.message.reply_text('Ocurrió un error durante el registro. Por favor, inténtalo de nuevo.')

    return ConversationHandler.END

async def verificar_rol(update: Update, context: CallbackContext):
    """
    Verifica el rol del usuario y permite la planificación si está autorizado.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del flujo de conversación.
    """
    user_id = update.message.from_user.id
    usuarios = solicitud_query("SELECT * FROM bot_usuarios_autorizados;")
    iD_TELEGRAM = usuarios['ID_TELEGRAM'].tolist()
    Rol_user = solicitud_query(f"SELECT ROL FROM bot_usuarios_autorizados where ID_TELEGRAM = {user_id};")
    roluser = Rol_user.iloc[0, 0]

    if int(user_id) in iD_TELEGRAM and roluser == 'PLANIFICADOR' or roluser == 'ADMINISTRADOR':
        await update.message.reply_text(
            '¡Bienvenido! Por favor, ingresa los medidores que planificarán enviando un archivo Excel, '
            'una lista de medidores separada por comas, o un listado de medidores en diferentes líneas.'
        )
        return PLANIFICACION
    else:
        await update.message.reply_text('No estás autorizado para realizar una planificación.')
        return ConversationHandler.END

async def planificacion(update: Update, context: CallbackContext):
    """
    Maneja la planificación de medidores. Permite al usuario enviar un archivo Excel o una lista de medidores.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del flujo de conversación.
    """
    user_id = update.message.from_user.id
    user_first_name = update.message.from_user.first_name
    print("Realizando la planificación")

    # Obtener datos de usuarios autorizados
    usuarios = solicitud_query("SELECT * FROM bot_usuarios_autorizados;")
    iD_TELEGRAM = usuarios['ID_TELEGRAM'].tolist()

    if int(user_id) in iD_TELEGRAM:
        usuario_encontrado = usuarios.loc[usuarios['ID_TELEGRAM'] == int(user_id)].iloc[0]
        nombre_completo = usuario_encontrado['NOMBRE_COMPLETO'] if pd.notna(usuario_encontrado['NOMBRE_COMPLETO']) else user_first_name
        print(f"Nombre completo: {nombre_completo}")

        # Manejo de archivos enviados
        if update.message.document:
            file = update.message.document
            print("El usuario envió un documento")

            # Verificación del tipo de archivo
            if file.file_name.endswith('.xlsx') or file.file_name.endswith('.xls'):
                try:
                    file_path = await context.bot.get_file(file.file_id)
                    file_data = await file_path.download_as_bytearray()
                    print("Datos del archivo descargado")

                    # Convertir bytearray a BytesIO para que pandas pueda leerlo
                    file_stream = io.BytesIO(file_data)

                    # Leer datos de Excel usando BytesIO
                    excel_data = pd.read_excel(file_stream)
                    print("Datos del Excel leídos")
                    print(excel_data)

                    # Limpiar el DataFrame: eliminar filas donde la columna 'Clave' es nula, vacía o ""
                    excel_data = excel_data.dropna(subset=['Clave'])  # Eliminar filas con NaN en 'Clave'
                    excel_data = excel_data[excel_data['Clave'].astype(str).str.strip() != '']  # Eliminar filas con cadenas vacías
                    print(excel_data)

                    records = [(user_id, nombre_completo, row['Medidor'], row['Fecha'].strftime('%Y-%m-%d'))
                               for index, row in excel_data.iterrows()]

                    try:
                        with engine.begin() as con:
                            # Usar la sintaxis de SQLAlchemy para múltiples inserciones
                            query_insert = text("""
                                INSERT INTO pnrp.bot_planificacion_md(ID_TELEGRAM, NOMBRE, MEDIDOR, FECHA_PLANIFICACION, REVISION)
                                VALUES(:user_id, :user_nombre, :medidor, :fecha, 0)
                            """)
                            # Insertar múltiples registros usando executemany
                            con.execute(query_insert, [dict(zip(['user_id', 'user_nombre', 'medidor', 'fecha'], record)) for record in records])
                    except SQLAlchemyError as e:
                        logging.error(f"Error al insertar los medidores: {e}")
                        await update.message.reply_text('Error al registrar los medidores. Por favor, inténtalo de nuevo.')

                    await update.message.reply_text('Todos los medidores han sido registrados con éxito. Usa el comando /menu para acceder a las opciones.')

                except Exception as e:
                    print(e)
                    logging.error(f"Error al procesar los datos del archivo Excel: {e}")
                    await update.message.reply_text('Error al procesar el archivo. Por favor, asegúrate de que el formato sea correcto.')

                return ConversationHandler.END

            else:
                await update.message.reply_text('Por favor, envía un archivo Excel válido (.xlsx o .xls).')
                return PLANIFICACION
        else:
            await update.message.reply_text('Por favor, envía un archivo Excel para continuar con la planificación.')
            return PLANIFICACION
    else:
        await update.message.reply_text('No estás autorizado para realizar una planificación.')
        return ConversationHandler.END


# Función para iniciar el menú de opciones
async def iniciar_menu(update: Update, context: CallbackContext):
    """
    Muestra el menú de opciones al usuario.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del siguiente paso en la conversación (SELECCIONAR_OPCION).
    """
    opciones = [
        ['Informacion del medidor', '1'],
        ['Comunicacion del medidor', '2'],
        ['Alarmas del medidor', '3'],
        ['Ordenes de servicio del medidor', '4'],
        ['Comentario de Telegestion', '5']
    ]
    reply_markup = ReplyKeyboardMarkup([[opcion[0]] for opcion in opciones], one_time_keyboard=True)
    context.user_data['opciones'] = opciones
    await update.message.reply_text('Por favor, selecciona una opción:', reply_markup=reply_markup)
    return SELECCIONAR_OPCION

# Función para manejar la selección de opciones
async def seleccionar_opcion(update: Update, context: CallbackContext):
    """
    Maneja la opción seleccionada por el usuario y solicita la marca del medidor.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del siguiente paso en la conversación (SELECCIONAR_MARCA).
    """
    opcion_texto = update.message.text
    opciones = context.user_data['opciones']
    
    # Buscar el ID correspondiente a la opción seleccionada
    for opcion in opciones:
        if opcion[0] == opcion_texto:
            context.user_data['user_command'] = opcion[1]
            break
    
    # Opciones de marca
    marcas = [['Elster']]
    reply_markup = ReplyKeyboardMarkup(marcas, one_time_keyboard=True)
    await update.message.reply_text(f'Seleccionaste: {opcion_texto}. Ahora selecciona la marca del medidor:', reply_markup=reply_markup)
    return SELECCIONAR_MARCA

# Función para manejar la selección de marca
async def seleccionar_marca(update: Update, context: CallbackContext):
    """
    Maneja la selección de la marca del medidor.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado del siguiente paso en la conversación (INGRESAR_MEDIDOR).
    """
    context.user_data['marca'] = update.message.text
    await update.message.reply_text(f'Seleccionaste: {update.message.text}. Ahora ingresa el número del medidor:')
    return INGRESAR_MEDIDOR

def transform_client_to(client):
    """
    Transforma el identificador del cliente en un formato específico.

    Args:
        client (str): El identificador original del cliente.

    Returns:
        str: El identificador transformado en el formato "AAAA-XXX-YYYYYY".
    """
    year = client[:4]
    group = client[4:7]
    number = client[7:]
    transformed_client = f"{year}-{group.zfill(3)}-{number.zfill(6)}"
    return transformed_client

# Función para manejar la entrada del número de medidor
async def ingresar_medidor(update: Update, context: CallbackContext):
    """
    Maneja la entrada del número del medidor y lo inserta en la base de datos.

    Args:
        update (Update): Contiene la información del mensaje recibido.
        context (CallbackContext): Contexto del comando.

    Returns:
        int: Estado de finalización de la conversación (ConversationHandler.END).
    """
    user_medidor = update.message.text
    user_id = update.message.from_user.id
    user_first_name = update.message.from_user.first_name
    user_command = context.user_data['user_command']
    user_marca = context.user_data['marca']
    fecha_instantanea = datetime.now()
    usuarios = solicitud_query("SELECT * FROM bot_usuarios_autorizados;")
    iD_TELEGRAM = usuarios['ID_TELEGRAM'].tolist()

    if user_medidor != "None" and (13 <= len(user_medidor) <= 15):
        if not (len(user_medidor) > 8 and user_medidor[4] == '-' and user_medidor[8] == '-'):
            user_medidor = transform_client_to(user_medidor)
           
    if int(user_id) in iD_TELEGRAM:
        try:
            usuario_encontrado = usuarios.loc[usuarios['ID_TELEGRAM'] == int(user_id)].iloc[0]
            nombre_completo = usuario_encontrado['NOMBRE_COMPLETO'] if pd.notna(usuario_encontrado['NOMBRE_COMPLETO']) else user_first_name
 
            with engine.begin() as con:
                logging.info(f"Insertando consulta a la base de datos, nombre: {nombre_completo}, medidor:{user_medidor}, comando:{user_command}, fecha:{fecha_instantanea}")
                print(f"el nombre completo es: {nombre_completo} y el medidor que ingreso es: {user_medidor}, comando: {user_command}")
                query_insert = text("INSERT INTO proceso_bot (ID_TG, COMANDO, MEDIDOR, FECHA, PROCESO, ENVIADO, NOMBRE) VALUES (:user_id, :user_command, :user_medidor, :fecha_instantanea, 0, 0, :nombre_completo)")
                con.execute(query_insert, {
                    'user_id': user_id,
                    'user_command': user_command,
                    'user_medidor': user_medidor,
                    'fecha_instantanea': fecha_instantanea,
                    'nombre_completo': nombre_completo
                })
                logging.info(f"Insercion exitosa, nombre: {nombre_completo}, medidor:{user_medidor} ,comando:{user_command}, fecha:{fecha_instantanea}")
                await update.message.reply_text(f"La solicitud de validación está en proceso para el medidor: {user_medidor}")

        except SQLAlchemyError as e:
            logging.error(f"Error al insertar solicitud en la base de datos: {e}")
            await update.message.reply_text(f"Error en la inserción de la solicitud para el medidor: {user_medidor}")

    return ConversationHandler.END


async def procesar_solicitudes(application):
    """
    Procesa las solicitudes pendientes de un bot y envía respuestas personalizadas a los usuarios.

    Este proceso involucra la consulta de una base de datos para obtener solicitudes que no han sido procesadas
    ni enviadas. Para cada solicitud, se determina el comando del usuario y se recupera la información relevante
    del medidor asociado. Según el comando, se construye un mensaje que se envía al usuario a través de un bot.

    Los comandos posibles son:
        1. Información del medidor
        2. Estado de comunicación del medidor
        3. Alarmas del medidor
        4. Órdenes de servicio asociadas al medidor
        5. Comentarios de telegestión

    Parámetros:
        application (object): La aplicación que contiene el bot para enviar mensajes.

    Manejo de errores:
        - Registra errores en el procesamiento de solicitudes y el envío de mensajes.

    Requisitos:
        - Conexión a la base de datos utilizando `engine`.
        - Funciones auxiliares para realizar consultas a la base de datos.
    """
    try:
        with engine.connect() as conn:
            solicitudes_df = solicitud_query("SELECT * FROM proceso_bot WHERE PROCESO = 0 AND ENVIADO = 0  AND year(FECHA) = 2025;")
            logging.info(f"Solicitudes encontradas: {solicitudes_df}")
            print(solicitudes_df)
            for _, solicitud in solicitudes_df.iterrows():
                solicitud_id = solicitud['ITEM']
                user_id = solicitud['ID_TG']
                user_command = solicitud['COMANDO']
                medidor = solicitud['MEDIDOR']
                user_first_name = solicitud['NOMBRE']

                print(f"{solicitud_id},  {user_id}, {user_command}, {medidor}, {user_first_name}")
                
                with engine.begin() as conn:
                    query_update_proceso = text("UPDATE proceso_bot SET PROCESO='1' WHERE ITEM = :id")
                    conn.execute(query_update_proceso, {'id': solicitud_id})
                
                mensaje = None
                clave = solicitud_query(f"select CLAVE_CATALOGO from pnrp.airflow_elster_universo where MEDIDOR_CATALOGO = '{medidor}' limit 1;")
                if not clave.empty:
                    clave = clave.iloc[0,0]
                else:
                    clave = "EMPTY"
            

                # print(clave)
                if user_command == "1":
                    if clave != "EMPTY":   
                        informacion_medidor = solicitud_query(f"SELECT * FROM pnrp.airflow_elster_universo WHERE MEDIDOR_CATALOGO = '{medidor}';")
                        
                        if not informacion_medidor.empty:
                            medidor_info = informacion_medidor.iloc[0]
                            mensaje = (
                                f"Hola Ingeniero {user_first_name}\n\n"
                                f"El siguiente reporte es para el medidor: {medidor}.\n\n"
                                f"Clave: {medidor_info['CLAVE_INCMS']}\n"
                                f"Nombre Abonado: {medidor_info['NOMBRE_ABONADO_INCMS']}\n"
                                f"Medidor: {medidor_info['MEDIDOR_INCMS']}\n"
                                f"Multiplicador: {medidor_info['MULTIPLICADOR']}\n"
                                f"Último Consumo: {medidor_info['ULTIMO_CONSUMO']}\n"
                                f"Lectura Actual: {medidor_info['LECTURA_ACTUAL']}\n"
                                f"Código de Lectura: {medidor_info['CODIGO_LECTURA']}\n"
                                f"Tarifa: {medidor_info['TARIFA']}\n"
                                f"Tipo de Medida: {medidor_info['TIPO_MEDIDA']}\n"
                                f"Zona: {medidor_info['ZONA']}\n"
                                f"Región PNRP: {medidor_info['REGION_PNRP']}\n"
                                f"Circuito: {medidor_info['CIRCUITO']}\n"
                                f"Subestación: {medidor_info['SUBESTACION']}\n"
                                f"Coord. Geograficas(X,Y): {medidor_info['COORD_U_X']}, {medidor_info['COORD_U_Y']}\n"
                                f"Coord. UTM(X,Y):{medidor_info['COORD_X']}, {medidor_info['COORD_Y']}\n"
                                f"Ubicacion de medidor: https://www.google.com/maps?q={medidor_info['COORD_U_Y']},{medidor_info['COORD_U_X']}"
                            )
                        
                        if informacion_medidor.empty:
                            mensaje = f"Hola ingeniero {user_first_name}, no hay informacion del medidor: {medidor}"
                    else:
                        mensaje = f"No se encontro informacion del medidor: {medidor}"
                        logging.warning(f"No se encontró información para el medidor: {medidor}")
                
                if user_command == "2":
                    if clave != "EMPTY":
                        comunicacion_elster = solicitud_query(f"SELECT * FROM pnrp.ws_elster_rele where device_name = '{medidor}';")
                        
                        # Verifica si el DataFrame no está vacío
                        if not comunicacion_elster.empty:
                            medidor_comunicacion = comunicacion_elster.iloc[0]
                            gatekeeper = medidor_comunicacion['gatekeeper']
                            rele = medidor_comunicacion['service_status']
                            last_registered = medidor_comunicacion['last_registered']
                            last_register_read = medidor_comunicacion['last_register_read']
                            fecha_actual = datetime.now()

                            # Determinar si el medidor comunica usando la fecha correspondiente
                            if not pd.isnull(last_register_read):
                                comunica = "Si comunica" if (fecha_actual - last_register_read) < timedelta(days=3) else "No comunica"
                                ultima_comunicacion = f"Última fecha de comunicacion del medidor a través del gatekeeper: {last_register_read}"
                            else:
                                comunica = "Si comunica" if (fecha_actual - last_registered) < timedelta(days=3) else "No comunica"
                                ultima_comunicacion = f"Última fecha de comunicacion directa del medidor: {last_registered}"

                            # Convertir el estado del rele
                            estado_rele = {
                                "connect": "Conectado",
                                "disconnect": "Desconectado",
                                "unknown": "Desconocido"
                            }.get(rele.lower(), "Desconocido")

                            # Mensaje para el estado del gatekeeper
                            if pd.isnull(gatekeeper):
                                gatekeeper_info = (
                                    "El medidor comunica, pero no tiene gatekeeper asociado."
                                    if comunica == "Sí comunica"
                                    else "El medidor no comunica y no tiene gatekeeper asociado."
                                )
                            else:
                                gatekeeper_info = (
                                    f"El medidor comunica a través del gatekeeper asociado."
                                    if not pd.isnull(last_register_read)
                                    else "El medidor tiene un gatekeeper asociado, pero no ha comunicado a traves de el."
                                )

                            # Construir el mensaje final
                            mensaje = (
                                f"Hola Ingeniero {user_first_name}\n\n"
                                f"El siguiente reporte es para el medidor: {medidor} con clave: {clave}.\n\n"
                                f"{ultima_comunicacion}\n"
                                f"{gatekeeper_info}\n"
                                f"Estado del rele: {estado_rele}\n\n"
                                f"Comunicación: {comunica}"
                            )
                        
                        # Mensaje cuando el DataFrame está vacío
                        else:
                            mensaje = f"Hola ingeniero {user_first_name}, no hay informacion del medidor: {medidor}"

                    else:
                        mensaje = f"No se encontro informacion del medidor: {medidor}"

                

                if user_command == "3":

                    if clave != "EMPTY":
                            
                        alarmas_medidor = solicitud_query(f"SELECT NOMBRE_EVENTO, MAX(FECHA) AS FECHA,  count(NOMBRE_EVENTO) AS CANTIDAD  FROM pnrp.airflow_elster_alarmas WHERE medidor = '{medidor}' GROUP BY NOMBRE_EVENTO ORDER BY FECHA DESC LIMIT 30;")
                        # print(alarmas_medidor)
                        if not alarmas_medidor.empty:
                            mensaje= (f"Hola ingeniero {user_first_name}\n\n"
                                f"El siguiente reporte es para el medidor: {medidor}.\n\n"
                                f"Alarmas del medidor:\n\n"
                            )
                            for index, row in alarmas_medidor.iterrows():
                                mensaje += (f"- {row['NOMBRE_EVENTO']} \n(Ultima Fecha Detectada: {row['FECHA']}, Cantidad: {row['CANTIDAD']})\n\n")

                            mensaje += "\nPor favor revise las alarmas mencionadas."
                        
                        if alarmas_medidor.empty:
                            mensaje = f"Hola ingeniero {user_first_name}, no hay informacion del medidor: {medidor}"
                    else:
                        logging.warning(f"No se encontro información para el medidor: {medidor} o clave: {clave}")
                        mensaje = f"No se encontro información de alarmas para el medidor {medidor}."
    
                if user_command == "4":
                    if clave != "EMPTY":
                        # Consultar las órdenes para el medidor específico
                        ordenes = solicitud_query(f"SELECT * FROM pnrp.airflow_elster_os WHERE clave = '{clave}' ORDER BY FECHA_EJECUCION DESC;")
                        
                        # Verificar si el DataFrame no está vacío
                        if not ordenes.empty:
                            # Crear el mensaje concatenando la información de cada orden
                            mensaje = (f"Hola ingeniero {user_first_name},\n\n"
                                    f"El siguiente reporte es para el medidor: {medidor}.\n\n"
                                    f"Alarmas del medidor:\n\n" + 
                                    "\n\n".join(
                                        f"Número de OS: {orden['OS']}\n"
                                        f"Estado de la OS: {orden['ESTADO']}\n"
                                        f"Categoría de la anomalía: {orden['CATEGORIA']}\n"
                                        f"Descripción de OS: {orden['DESCRIPCION']}\n"
                                        f"Fecha Generada: {orden['FECHA_GENERADA']}\n"
                                        f"Fecha de Ejecución: {orden['FECHA_EJECUCION']}\n"
                                        for _, orden in ordenes.iterrows()
                                    ))
                        
                        if ordenes.empty:
                            mensaje = f"Hola ingeniero {user_first_name}, no hay informacion del medidor: {medidor}"
                    else:
                        logging.warning(f"No se encontro información para el medidor: {medidor} o clave: {clave}")
                        mensaje = f"No se encontro ordenes de servicio para el medidor {medidor} con clave {clave}."

                if user_command == '5':
                    comentario_telegestion = solicitud_query(f"SELECT * FROM bitacora_ac where clave = '{clave}' and ESTADO <> 'ANULADO' and REQUIERE_OS = TRUE order by fecha_asignacion desc;")
                    if not comentario_telegestion.empty:
                        mensaje = (f"\nHola ingeniero {user_first_name}, \n\n"
                                   f"El siguiente reporte es para el medidor: {medidor}\n\n"
                                   f"El departamento de telegestion ha hecho una o mas revisiones al medidor.\n"
                                   "\n\n".join(
                                            f"Fecha de analisis: {comentario['FECHA_ANALISIS']}\n"
                                            f"Alarma encontrada: {comentario['ALARMA']}\n"
                                            f"Comentario del analista: {comentario['COMENTARIO_ANALISTA']}\n"
                                            for _, comentario in comentario_telegestion.iterrows() 
                                   ) 
                        )
                    if comentario_telegestion.empty:
                        mensaje = f"Hola ingeniero {user_first_name}, no se ha realizado analisis para el medidor: {medidor} con clave: {clave}"

                if mensaje:
                    try:
                        await application.bot.send_message(chat_id=user_id, text=mensaje)
                        logging.info(f"Mensaje enviado a ID_TG: {user_id}")
                        
                        with engine.begin() as conn:
                            query_update_enviado = text("UPDATE proceso_bot SET ENVIADO='1' WHERE ITEM = :id")
                            conn.execute(query_update_enviado, {'id': solicitud_id})

                    except Exception as e:
                        logging.error(f"Error al enviar mensaje a ID_TG: {user_id}: {e}")

    except Exception as e:
        logging.error(f"Error en procesamiento de solicitudes: {e}")
    

"""def iniciar_proceso_asincrono(application):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(procesar_solicitudes(application))"""

# Función para manejar mensajes que no son comandos
async def manejar_mensaje(update: Update, context: CallbackContext):
    """
    Maneja los mensajes que no son comandos. Verifica si el usuario está registrado y
    redirige al menú de opciones si es así, o responde solicitando que se registre
    si no lo está.

    Args:
        update (Update): Contiene la información sobre el mensaje recibido.
        context (CallbackContext): Proporciona información adicional sobre el contexto
                                    del comando o mensaje.

    Returns:
        None
    """
    user_id = update.message.from_user.id
    usuarios = solicitud_query("SELECT * FROM bot_usuarios_autorizados;")
    iD_TELEGRAM = usuarios['ID_TELEGRAM'].tolist()

    if int(user_id) in iD_TELEGRAM:
        # Usuario registrado, iniciar flujo de selección de opciones
        return await iniciar_menu(update, context)
    else:
        # Usuario no registrado, ignorar el mensaje o enviar una respuesta
        await update.message.reply_text('Por favor, usa el comando /start para registrarte.')


# Función para manejar comandos desconocidos
async def cancel(update: Update, context: CallbackContext):
    """
    Maneja el comando de cancelación. Informa al usuario que la operación ha sido
    cancelada y termina la conversación.

    Args:
        update (Update): Contiene la información sobre el mensaje recibido.
        context (CallbackContext): Proporciona información adicional sobre el contexto
                                    del comando o mensaje.

    Returns:
        ConversationHandler.END: Termina la conversación.
    """
    await update.message.reply_text('Cancelado.')
    return ConversationHandler.END


# Función para manejar errores
async def error(update: Update, context: CallbackContext):
    """
    Maneja errores que ocurren durante la ejecución del bot. Registra el error en
    los logs para su posterior revisión.

    Args:
        update (Update): Contiene la información sobre el mensaje que causó el error.
        context (CallbackContext): Proporciona información adicional sobre el contexto
                                    del error.

    Returns:
        None
    """
    logging.error(f"Actualización {update} causó el error {context.error}")


def main():
    """
    Función principal para iniciar el bot. Configura los manejadores de comandos y
    mensajes, gestiona el flujo de conversación y establece un JobQueue para tareas
    programadas.

    Returns:
        None
    """
    # Tu token de bot aquí
    application = Application.builder().token(os.getenv('YOUR_TOKEN')).build()

    # Primer ConversationHandler para el registro
    registro_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            REGISTRO: [MessageHandler(filters.TEXT & ~filters.COMMAND, registro)],
        },
        fallbacks=[],
    )

    # Segundo ConversationHandler para manejar el menú y otras opciones
    menu_handler = ConversationHandler(
        entry_points=[CommandHandler('menu', iniciar_menu)],
        states={
           SELECCIONAR_OPCION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, seleccionar_opcion),
                CommandHandler('menu', manejar_mensaje)],
            SELECCIONAR_MARCA: [MessageHandler(filters.TEXT & ~filters.COMMAND, seleccionar_marca),
                CommandHandler('menu', manejar_mensaje)],
            INGRESAR_MEDIDOR: [MessageHandler(filters.TEXT & ~filters.COMMAND, ingresar_medidor),
                CommandHandler('menu', manejar_mensaje)],
        },
        fallbacks=[CommandHandler('menu', manejar_mensaje)],
    )

    # Tercer ConversationHandler para la planificación
    planificacion_handler = ConversationHandler(
        entry_points=[CommandHandler('planificacion', verificar_rol)],
        states={
            PLANIFICACION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, planificacion),
                MessageHandler(filters.Document.FileExtension("xls") | filters.Document.FileExtension("xlsx"), planificacion)
            ],
        },
        fallbacks=[],
    )

    application.add_handler(planificacion_handler)

    # Añadir los manejadores al bot
    application.add_handler(registro_handler)
    application.add_handler(menu_handler)
    # Manejador de errores
    application.add_error_handler(error)

    # Asegúrate de que la job_queue esté disponible
    if application.job_queue is None:
        logging.error("JobQueue no está disponible. Asegúrate de que python-telegram-bot esté instalado con el extra `job-queue`.")
        return

    # Configuración del JobQueue
    job_queue = application.job_queue
    job_queue.run_repeating(procesar_solicitudes, interval=10, first=0)

    application.run_polling()


if __name__ == '__main__':
    main()

