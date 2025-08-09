# Motor de Ingesta PySpark - Documentación del Alumno

## Descripción del Proyecto

Este proyecto implementa un motor de ingesta de datos usando PySpark para procesar información de vuelos. El sistema está diseñado para ejecutarse tanto en entornos locales como en Databricks, permitiendo el procesamiento distribuido de grandes volúmenes de datos aeroportuarios.

### Funcionalidades Principales

- **Ingesta de datos JSON**: Procesamiento de archivos con información de vuelos
- **Aplanado de DataFrames**: Transformación de estructuras anidadas en formato tabular
- **Conversión de zonas horarias**: Conversión automática a UTC basada en aeropuerto de origen
- **Agregaciones temporales**: Cálculo de intervalos entre vuelos por aeropuerto

## Arquitectura de Spark sobre Databricks

### Componentes del Sistema

```
┌─────────────────────────────────────────────────────────────┐
│                    DATABRICKS CLUSTER                       │
├─────────────────────────────────────────────────────────────┤
│  Driver Node                │  Worker Nodes                 │
│  - SparkSession             │  - Ejecutores distribuidos    │
│  - Coordinación tasks       │  - Procesamiento paralelo     │
│  - Gestión metadatos        │  - Almacenamiento temporal    │
└─────────────────────────────────────────────────────────────┘
           │                              │
           ▼                              ▼
┌──────────────────┐              ┌─────────────────┐
│   DBFS Storage   │              │   Delta Tables  │
│   - Raw data     │              │   - Processed   │
│   - Config files │              │   - Optimized   │
└──────────────────┘              └─────────────────┘
```

### Flujo de Procesamiento

1. **Ingesta**: Los datos JSON se cargan desde DBFS (Databricks File System)
2. **Transformación**: El cluster distribuye el procesamiento entre workers
3. **Agregación**: Se calculan métricas y se aplican transformaciones temporales
4. **Persistencia**: Los resultados se almacenan en formato Delta para consultas optimizadas


## Estructura del Código

```
motor_ingesta/
├── motor_ingesta.py          # Clase principal de ingesta
├── agregaciones.py           # Funciones de transformación temporal
├── flujo_diario.py           # Procesamiento batch diario
└── resources/
    └── timezones.csv         # Mapeo aeropuertos-zonas horarias
```

## Tests Individuales

### 1. `test_aplana`

**Propósito**: Valida el aplanado de estructuras de datos anidadas (structs y arrays).

**Funcionalidad**:
- Transforma un DataFrame con columnas tipo `struct` y `array[struct]`
- Convierte estructuras anidadas en columnas planas
- Elimina las columnas originales complejas

**Datos de prueba**:
```python
tupla3("a", "b", "c")  # struct con 3 campos
[tupla2("pepe", "juan"), tupla2("pepito", "juanito")]  # array de structs
```

**Verificación**: Confirma que las columnas `a1`, `a2`, `a3`, `b1`, `b2` existen y que `tupla` y `amigos` han sido eliminadas.

### 2. `test_ingesta_fichero`

**Propósito**: Prueba la ingesta completa de un archivo JSON con configuración específica.

**Funcionalidad**:
- Carga configuración desde `test_config.json`
- Procesa archivo de datos `test_data.json`
- Valida el esquema y contenido resultante

**Datos de prueba**:
```json
{"nombre": "Juan", "familiares": [{"parentesco": "sobrino", "numero": 3}], "profesion": "Ingeniero"}
```

**Verificación**: Comprueba que se extraen correctamente los campos `nombre`, `parentesco`, `numero` y `profesion`.

### 3. `test_aniade_intervalos_por_aeropuerto`

**Propósito**: Valida el cálculo de intervalos entre vuelos consecutivos del mismo aeropuerto.

**Funcionalidad**:
- Agrupa vuelos por aeropuerto de origen (`Origin`)
- Ordena cronológicamente por `FlightTime`
- Calcula tiempo hasta el siguiente vuelo usando `F.lag(-1)`

**Datos de prueba**:
```python
("JFK", "2023-12-25 15:35:00", "American_Airlines")
("JFK", "2023-12-25 17:35:00", "Iberia")
```

**Verificación**: Confirma que el intervalo entre vuelos es 7200 segundos (2 horas) y que se capturan correctamente la aerolínea y hora del vuelo siguiente.

### 4. `test_aniade_hora_utc`

**Propósito**: Prueba la conversión de horarios locales a UTC usando zonas horarias por aeropuerto.

**Funcionalidad**:
- Une datos de vuelos con información de zonas horarias por código IATA
- Construye timestamp local desde `FlightDate` y `DepTime`
- Convierte a UTC usando `F.to_utc_timestamp()`

**Datos de prueba**:
```python
("JFK", "2023-12-25", 1535)  # JFK a las 15:35 local
```

**Verificación**: Confirma que 15:35 EST se convierte correctamente a 20:35 UTC (diferencia de 5 horas).

## Configuración del Entorno de Testing

### Requisitos del Sistema

- **Python 3.12**: Entorno de desarrollo principal
- **Java 11**: Requerido para compatibilidad con PySpark 3.4.1
- **PySpark 3.4.1**: Versión compatible con Python 3.12

### Configuración Específica

El archivo `tests/conftest.py` establece:

```python
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
```

Esta configuración asegura que los tests usen Java 11 mientras el sistema mantiene Java 21 para otras aplicaciones.

### Ejecución de Tests

```bash
# Activar entorno
source venv_test/bin/activate

# Ejecutar test individual  
python -m pytest tests/test_ingesta.py::test_aplana -v

# Ejecutar todos los tests
python -m pytest tests/test_ingesta.py -v
```

## Resolución de Problemas Comunes

1. **Error JVM**: Verificar que Java 11 esté instalado y `JAVA_HOME` configurado
2. **Serialization issues**: Usar PySpark 3.4.1 en lugar de versiones más recientes
3. **Missing timezones**: Confirmar que `timezones.csv` existe en `motor_ingesta/resources/`

---

**Autor**: Moises Barbancho Duque
**Fecha**: Agosto 2025  
**Versión PySpark**: 3.4.1  
**Entorno**: Python 3.12 + Java 11