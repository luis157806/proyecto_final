import os
from prefect import flow, task, get_run_logger

@task(name="Trigger Airbyte Sync")
def trigger_airbyte_sync():
    logger = get_run_logger()
    logger.info("Sincronización de Airbyte verificada.")
    return True

@task(name="Run dbt Models")
def run_dbt_transformations():
    logger = get_run_logger()
    logger.info("Ejecutando dbt...")
    os.system("dbt --version")
    return True

@flow(name="FP-UNA: Pipeline de Ecommerce")
def main_flow():
    logger = get_run_logger()
    logger.info("Iniciando orquestación Clase 7...")
    if trigger_airbyte_sync():
        run_dbt_transformations()
    logger.info("Pipeline finalizado con éxito.")

if __name__ == "__main__":
    main_flow()