from setuptools import setup, find_packages

setup(
    name="motor-ingesta",
    version="0.1.0",
    author="Estudiante",
    author_email="estudiante@ucm.es",
    description="Motor de ingesta de datos de vuelos para Apache Spark",
    long_description="Motor de ingesta que procesa datos de vuelos en formato JSON utilizando Apache Spark con soporte para Databricks",
    long_description_content_type="text/markdown",
    url="https://github.com/nombrealumno",
    python_requires=">=3.8",
    packages=find_packages(),
    package_data={"motor_ingesta": ["resources/*.csv"]}
)
