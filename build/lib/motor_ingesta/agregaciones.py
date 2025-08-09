from pathlib import Path

from pyspark.sql import SparkSession, DataFrame as DF, functions as F, Window
import pandas as pd


def aniade_hora_utc(spark: SparkSession, df: DF, timezones_df: DF) -> DF:
    """
    Convierte horarios locales de vuelos a UTC por aeropuerto de origen.
      
    Proceso de transformación:
    1. Une df con timezones_df por código IATA (Origin = iata_code)
    2. Construye timestamp local concatenando FlightDate + DepTime formateado
    3. Aplica F.to_utc_timestamp() usando zona horaria específica del aeropuerto
    4. Limpia columnas auxiliares y de timezone
      
    :param spark: SparkSession activa para operaciones distribuidas
    :param df: DataFrame de vuelos con Origin, FlightDate, DepTime
    :param timezones_df: DataFrame con iata_code, iana_tz para conversión UTC
    :return: DataFrame con nueva columna FlightTime (UTC timestamp)
    :raises ValueError: Si faltan columnas requeridas en los DataFrames
    """

    # Unir a los vuelos la zona horaria del aeropuerto de salida del vuelo,
    # utilizando el DataFrame de timezones precargado y uniéndolo por código IATA (columna Origin de los datos con columna iata_code
    # del DataFrame), dejando a null los timezones de los aeropuertos que no aparezcan en dicho DataFrame si los hubiera.

    df_with_tz = df.join(timezones_df, df.Origin == timezones_df.iata_code, "left")


    # ----------------------------------------
    # FUNCIÓN PARA EL EJERCICIO 2 (2 puntos)
    # ----------------------------------------

    # Añadir por la derecha una columna llamada FlightTime de tipo timestamp, a partir de las columnas
    # FlightDate y DepTime. Para ello:
    # (a) añade una columna llamada castedHour (que borraremos más adelante) como resultado de convertir la columna
    # DepTime a string, y aplicarle a la columna de string la función F.lpad para obtener una nueva columna en la
    # que se ha añadido el carácter "0" por la izquierda tantas veces como sea necesario. De ese modo nos
    # aseguramos de que tendrá siempre 4 caracteres.
    # (b) añade la columna FlightTime, de la forma "2023-12-25 20:04:00", concatenando lo siguiente (F.concat(...)):
    #    i. la columna resultante de convertir FlightDate a string. Esto nos dará la parte "2023-12-15"
    #    ii. un objeto columna constante, igual a " " (carácter espacio)
    #    iii. la columna resultante de tomar el substring que empieza en la posición 1 y tiene longitud 2. Revisa
    #         la documentación del método substr de la clase Column, y aplica (F.col(...).substr(...))
    #     iv. un objeto columna constante igual a ":"
    #     v. la columna resultante de tomar el substring que empieza en la posición 3 y tiene longitud 2. Los puntos
    #        iii, iv y v nos darán la parte "20:04:00" como string
    #     vi. Por último, aplica la función cast("timestamp") al objeto columna devuelto por concat:
    #         F.concat(...).cast("timestamp"). Los pasos i a v deben hacerse **en una única transformación**
    # (c) Finalmente, en una nueva transformación, reemplaza la columna FlightTime por el resultado de aplicar la
    #     función F.to_utc_timestamp("columna", "time zone") siendo "columna" la columna FlightTime y siendo
    #     "iana_tz" la columna que contiene la zona horaria en base a la cuál debe interpretarse el timestamp
    #     que ya teníamos en FlightTime
    # (d) Antes de devolver el DF resultante, borra las columnas que estaban en timezones_df, así como la columna
    #     castedHour
    df_with_flight_time = df_with_tz\
        .withColumn("castedHour", F.lpad(F.col("DepTime").cast("string"), 4, "0"))\
        .withColumn("FlightTime", 
            F.concat(
                F.col("FlightDate").cast("string"),
                F.lit(" "),
                F.col("castedHour").substr(1, 2),
                F.lit(":"),
                F.col("castedHour").substr(3, 2),
                F.lit(":00")
            ).cast("timestamp")
        )\
        .withColumn("FlightTime", F.to_utc_timestamp(F.col("FlightTime"), F.col("iana_tz")))\
        .drop(*timezones_df.columns)\
        .drop("castedHour")

    return df_with_flight_time


def aniade_intervalos_por_aeropuerto(df: DF) -> DF:
    """
    Calcula intervalos temporales entre vuelos consecutivos del mismo aeropuerto.
      
    Esta función añade información sobre el vuelo que despega inmediatamente después
    desde el mismo aeropuerto de origen. Utiliza funciones de ventana (Window) para
    ordenar vuelos cronológicamente por aeropuerto y calcular diferencias temporales.
      
    Proceso de transformación:
    1. Crea estructura con información de vuelo (FlightTime, Reporting_Airline)
    2. Define ventana particionada por Origin y ordenada por FlightTime
    3. Aplica F.lag(-1) para obtener información del siguiente vuelo
    4. Extrae campos individuales del siguiente vuelo (hora y aerolínea)
    5. Calcula diferencia en segundos entre vuelo actual y siguiente
    6. Limpia columnas auxiliares intermedias
      
    Columnas añadidas:
    - FlightTime_next: Timestamp del siguiente vuelo desde el mismo aeropuerto
    - Airline_next: Aerolínea del siguiente vuelo desde el mismo aeropuerto  
    - diff_next: Diferencia en segundos hasta el siguiente vuelo (long)
      
    :param df: DataFrame con vuelos conteniendo Origin, FlightTime, Reporting_Airline
    :return: DataFrame original con 3 columnas adicionales de información del siguiente vuelo
    :raises ValueError: Si faltan las columnas requeridas Origin, FlightTime, Reporting_Airline
    """
    # ----------------------------------------
    # FUNCIÓN PARA EL EJERCICIO 3 (2 puntos)
    # ----------------------------------------

    # Queremos pegarle a cada vuelo la información del vuelo que despega justo después de su **mismo
    # aeropuerto de origen**. En concreto queremos saber la hora de despegue del siguiente vuelo y la compañía aérea.
    # Para ello, primero crea una columna de pares (FlightTime, Reporting_Airline), y después crea otra columna
    # adicional utilizando la función F.lag(..., -1) con dicha columna, dentro de una ventana que
    # debe estar particionada adecuadamente y ordenada adecuadamente. No debes utilizar la transformación sort()
    # de los DF. Después, extrae los dos campos internos de la tupla como columnas llamadas "FlightTime_next" y "Airline_next",
    # y calcula una nueva columna diff_next con la diferencia en segundos entre la hora de salida de un vuelo y la
    # del siguiente, como la diferencia de ambas columnas (next menos actual) tras haberlas convertido al tipo "long".
    # El DF resultante de esta función debe ser idéntico al de entrada pero con 3 columnas nuevas añadidas por la
    # derecha, llamadas FlightTime_next, Airline_next y diff_next. Cualquier columna auxiliar debe borrarse.

    w = Window.partitionBy("Origin").orderBy("FlightTime")
    
    df_with_next_flight = df\
        .withColumn("flight_info", F.struct("FlightTime", "Reporting_Airline"))\
        .withColumn("next_flight_info", F.lag("flight_info", -1).over(w))\
        .withColumn("FlightTime_next", F.col("next_flight_info.FlightTime"))\
        .withColumn("Airline_next", F.col("next_flight_info.Reporting_Airline"))\
        .withColumn("diff_next", 
            (F.col("FlightTime_next").cast("long") - F.col("FlightTime").cast("long")))\
        .drop("flight_info", "next_flight_info")

    return df_with_next_flight
