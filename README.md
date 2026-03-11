🚀 Data Pipeline End-to-End: CRM Sales Analytics
Este proyecto implementa un ecosistema de datos moderno (Modern Data Stack) para centralizar y analizar información de ventas y cuentas. El objetivo es transformar datos crudos de un CRM en un tablero de control (Dashboard) accionable, garantizando la calidad y automatización del proceso.

🏗️ Arquitectura del Proyecto
El pipeline sigue un enfoque ELT (Extract, Load, Transform) utilizando las siguientes herramientas:

Ingesta: Airbyte para la extracción y carga desde el CRM hacia el Warehouse.
Data Warehouse: MotherDuck (DuckDB en la nube) como almacenamiento centralizado.
Transformación: dbt Core para el modelado de datos en capas (Staging y Marts).
Calidad de Datos: dbt-expectations para la implementación de 7 tests de integridad.
Orquestación: Prefect para la automatización y monitoreo de flujos.
BI & Visualización: Metabase para la creación del Dashboard estratégico.
📊 Modelo de Datos
Se implementó una arquitectura de dos niveles:

Staging: Limpieza, renombrado y tipado de datos crudos.
Marts (OBT): Creación de una One Big Table (obt_ventas_final) diseñada para optimizar el rendimiento de las consultas analíticas.
