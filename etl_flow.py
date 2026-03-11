from prefect import flow, task
import os

@task(name="Run Airbyte Sync")
def run_airbyte_sync():
    # Aquí simulas o disparas la conexión
    # Si tienes el ID de conexión de Airbyte, podrías usar la API
    print("Iniciando sincronización de Airbyte...")
    return True

@task(name="dbt Transformation")
def run_dbt_transformation():
    # Ejecuta dbt run desde Python
    result = os.system("dbt run")
    if result == 0:
        print("Transformación dbt completada con éxito")
    else:
        raise Exception("Error en la ejecución de dbt")

@task(name="dbt Testing")
def run_dbt_tests():
    # Ejecuta dbt test
    result = os.system("dbt test")
    print("Validación de calidad completada")

@flow(name="End-to-End Sales Pipeline")
def sales_pipeline_flow():
    # Orquestación de pasos
    airbyte_status = run_airbyte_sync()
    
    if airbyte_status:
        run_dbt_transformation()
        run_dbt_tests()

if __name__ == "__main__":
    sales_pipeline_flow()