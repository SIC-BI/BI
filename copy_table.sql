CREATE TABLE `dhw-gmarti-prd.STG_PS4.stg_acdoca`
PARTITION BY budat
CLUSTER BY rbukrs, docln
OPTIONS (
  description='Prueba de partición y creación de tabla'
) AS
SELECT * EXCEPT(operation_flag, is_deleted)
FROM `dhw-gmarti-prd.BQ_PS4.acdoca`
WHERE FALSE;