-- Staging exposes no unsigned integer types: subtracting two of them raises an
-- overflow error once the result is negative. One row per offending column.
select
    table_schema,
    table_name,
    column_name,
    data_type
from information_schema.columns
where table_schema like '%staging'
  and regexp_matches(data_type, '\b(UTINYINT|USMALLINT|UINTEGER|UBIGINT|UHUGEINT)\b')
order by table_schema, table_name, column_name
