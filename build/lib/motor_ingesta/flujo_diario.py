import json
from datetime import timedelta
from pathlib import Path
from loguru import logger

from pyspark.sql import SparkSession, functions as F
from .motor_ingesta import MotorIngesta
from .agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto


class FlujoDiario:

    def __init__(self, config_file: str):
        """
        Inicializa el flujo diario con configuración y sesión de Spark apropiada
        :param config_file: Ruta al archivo de configuración JSON
        """
        # Leer el fichero de configuración como diccionario
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        
        # Crear SparkSession o DatabricksSession según el entorno de ejecución
        if self.config["EXECUTION_ENVIRONMENT"] == "databricks":
            # Importar DatabricksSession solo si estamos en entorno databricks
            from databricks.connect import DatabricksSession
            self.spark = DatabricksSession.builder.getOrCreate()
        else:
            # Usar SparkSession convencional para entorno local o producción
            self.spark = SparkSession.builder.getOrCreate()
        
        # Cargar y cachear tabla de timezones para evitar lecturas repetidas
        # Usar path configurado o fallback al archivo local
        timezones_path = self.config.get("timezones_path", 
                                        str(Path(__file__).parent) + "/resources/timezones.csv")
        self.timezones_df = (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(timezones_path)
            .cache()
        )


    def procesa_diario(self, data_file: str):
        """
        Completa la documentación
        :param data_file:
        :return:
        """

        try:
            # Procesamiento diario: crea un nuevo objeto motor de ingesta con self.config, invoca a ingesta_fichero,
            # después a las funciones que añaden columnas adicionales, y finalmente guarda el DF en la tabla indicada en
            # self.config["output_table"], que debe crearse como tabla manejada (gestionada), sin usar ningún path,
            # siempre particionando por FlightDate. Tendrás que usar .write.option("path", ...).saveAsTable(...) para
            # indicar que queremos crear una tabla externa en el momento de guardar.
            # Conviene cachear el DF flights_df así como utilizar el número de particiones indicado en
            # config["output_partitions"]

            motor_ingesta = MotorIngesta(self.config)
            flights_df = motor_ingesta.ingesta_fichero(data_file).cache()

            # Paso 1. Invocamos al método para añadir la hora de salida UTC
            flights_with_utc = aniade_hora_utc(self.spark, flights_df, self.timezones_df)


            # -----------------------------
            #  CÓDIGO PARA EL EJERCICIO 4
            # -----------------------------
            # Paso 2. Para resolver el ejercicio 4 que arregla el intervalo faltante entre días,
            # hay que leer de la tabla self.config["output_table"] la partición del día previo si existiera. Podemos
            # obviar este código hasta llegar al ejercicio 4 del notebook
            dia_actual = flights_df.first().FlightDate
            dia_previo = dia_actual - timedelta(days=1)
            try:
                flights_previo = self.spark.read.table(self.config["output_table"]).where(F.col("FlightDate") == dia_previo)
                logger.info(f"Leída partición del día {dia_previo} con éxito")
            except Exception as e:
                logger.info(f"No se han podido leer datos del día {dia_previo}: {str(e)}")
                flights_previo = None

            if flights_previo:
                # añadir columnas a F.lit(None) haciendo cast al tipo adecuado de cada una, y unirlo con flights_previo.
                # OJO: hacer select(flights_previo.columns) para tenerlas en el mismo orden antes de
                # la unión, ya que la columna de partición se había ido al final al escribir

                flights_with_utc_extended = flights_with_utc\
                    .withColumn("FlightTime_next", F.lit(None).cast("timestamp"))\
                    .withColumn("Airline_next", F.lit(None).cast("string"))\
                    .withColumn("diff_next", F.lit(None).cast("bigint"))
                
                df_unido = flights_previo.select(flights_previo.columns).unionByName(flights_with_utc_extended)
                # Spark no permite escribir en la misma tabla de la que estamos leyendo. Por eso salvamos
                df_unido.write.mode("overwrite").saveAsTable("tabla_provisional")
                df_unido = self.spark.read.table("tabla_provisional")

            else:
                df_unido = flights_with_utc           # lo dejamos como está

            # Paso 3. Invocamos al método para añadir información del vuelo siguiente
            df_with_next_flight = aniade_intervalos_por_aeropuerto(df_unido)

            # Paso 4. Escribimos el DF en la tabla manejada config["output_table"], con
            # el número de particiones indicado en config["output_partitions"]
            df_with_next_flight\
                .coalesce(self.config["output_partitions"])\
                .write\
                .mode("overwrite")\
                .option("partitionOverwriteMode", "dynamic")\
                .partitionBy("FlightDate")\
                .saveAsTable(self.config["output_table"])


            # Borrar la tabla provisional si la hubiéramos creado
            self.spark.sql("DROP TABLE IF EXISTS tabla_provisional")

        except Exception as e:
            logger.error(f"No se pudo escribir la tabla del fichero {data_file}")
            raise e


if __name__ == '__main__':
    # Ejemplo de uso local
    flujo = FlujoDiario("config/config.json")
    flujo.procesa_diario("ruta/al/archivo.json")

    # Recuerda que puedes crear el wheel ejecutando en la línea de comandos: python setup.py bdist_wheel
