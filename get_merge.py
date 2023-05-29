# Se importan las librerías necesarias
from google.cloud import bigquery

# Set up BigQuery client
client = bigquery.Client()

# Las variables  raw_table_name y base_table tienen el mismo valor, pero se manejó así para más claridad en lo que se ocupan
raw_table_name = 'dhw-gmarti-prd.BQ_PS4.acdoca'
base_table = 'dhw-gmarti-prd.BQ_PS4.acdoca'
target_table = 'dhw-gmarti-prd.CDC_PS4.acdoca'

# La siguiente función devuelve los campos llave comprandose, por ejemplo : `MANDT` = T1.`MANDT` AND S1.`EBELN` = T1.`EBELN`
def get_key_comparator(table_prefix, keys):
    p_key_list = []
    for key in keys:
        # pylint:disable=consider-using-f-string
        p_key_list.append('{0}.`{2}` = {1}.`{2}`'.format(
            table_prefix[0], table_prefix[1], key))
    return p_key_list


def get_keys(full_table_name):
    """Retrieves primary key columns for raw table from metadata table.

    Args:
        full_table_name: Full table name in project.dataset.table_name format.
    """

    _, dataset, table_name = full_table_name.split('.')
    query = (f'SELECT fieldname '
             f'FROM `{dataset}.dd03l` '
             f'WHERE KEYFLAG = "X" AND fieldname != ".INCLUDE" '
             f'AND tabname = "{table_name.upper()}"')
    query_job = client.query(query)

    fields = []
    for row in query_job:
        fields.append(row['fieldname'])
    return fields
keys = get_keys(raw_table_name)
if not keys:
    e_msg = f'Keys for table {raw_table_name} not found in table DD03L'
    raise Exception(e_msg) from None

p_key_list = get_key_comparator(['S', 'T'], keys)
p_key_list_for_sub_query = get_key_comparator(['S1', 'T1'], keys)
p_key = ' AND '.join(p_key_list)
p_key_sub_query = ' AND '.join(p_key_list_for_sub_query)
fields = []
update_fields = []


_CDC_EXCLUDED_COLUMN_LIST = ['_PARTITIONTIME', 'operation_flag', 'is_deleted']
raw_table_schema = client.get_table(raw_table_name).schema
for field in raw_table_schema:
    if field.name not in _CDC_EXCLUDED_COLUMN_LIST:
        fields.append(f'`{field.name}`')
        update_fields.append((f'T.`{field.name}` = S.`{field.name}`'))

separator = ', '
p_key=p_key
fields=separator.join(fields)
update_fields=separator.join(update_fields)
keys=', '.join(keys)
p_key_sub_query=p_key_sub_query
# Retrieve column names for target_table and base_table
# target_table_columns = [field.name for field in client.get_table(target_table).schema]
# base_table_columns = [field.name for field in client.get_table(base_table).schema]

# La variable template contiene toda la consulta, solo le pasamos las variables
template = f"""--  Copyright 2021 Google Inc.

--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at

--      http://www.apache.org/licenses/LICENSE-2.0

--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.

MERGE `{target_table}` AS T
USING (
  WITH
    S0 AS (
      SELECT * FROM `{base_table}`
      WHERE recordstamp >= (
        SELECT IFNULL(MAX(recordstamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `{target_table}`)
    ),
    -- To handle occasional dups from SLT connector
    S1 AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {keys}, recordstamp ORDER BY recordstamp) AS row_num
        FROM S0
      )
      WHERE row_num = 1
    ),
    T1 AS (
      SELECT {keys}, MAX(recordstamp) AS recordstamp
      FROM `{base_table}`
      WHERE recordstamp >= (
        SELECT IFNULL(MAX(recordstamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `{target_table}`)
      GROUP BY {keys}
    )
  SELECT S1.*
  FROM S1
  INNER JOIN T1
    ON {p_key_sub_query}
      AND S1.recordstamp = T1.recordstamp
  ) AS S
ON {p_key}
-- ## CORTEX-CUSTOMER You can use "`is_deleted` = true" condition along with "operation_flag = 'D'",
-- if that is applicable to your CDC set up.
WHEN NOT MATCHED AND IFNULL(S.operation_flag, 'I') != 'D' THEN
  INSERT ({fields})
  VALUES ({fields})
WHEN MATCHED AND S.operation_flag = 'D' THEN
  DELETE
WHEN MATCHED AND S.operation_flag = 'U' THEN
  UPDATE SET {update_fields};
"""

# Print the filled template

print(template)
