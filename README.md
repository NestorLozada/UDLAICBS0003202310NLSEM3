# Implementacion SOR y STAGE (Bodega de Datos)

* Nestor Lozada 

## Requerimientos
Este proyecto se encuentra hecho a base de Python 3.9 y MySQL 8.0
## instalaciones 
para poder instalar y correr correctamente el poryecto es necario corre el siguiente comando en el terminal `pip install -r requirements.txt`
## Ejecución de scripts SQL

Dentro del proyecto hay un directorio llamado : `sql`, el cual cuenta con 3 archivos de tipo sql y estos deben ser ejecutados en el siguiente orden:
1. `initialization.sql` (Crea los esquemas en la base de datos)
2. `tables-stg.sql` (este comando permite crear la tabal de Stage en la base de datos MySql)
3. `tables-sor.sql` (este comando permite crear la tabal de sor en la base de datos MySql)
