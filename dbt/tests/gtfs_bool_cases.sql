-- DuckDB 2.0.1 rejects try() around error() ("TRY can not be used in
-- combination with a volatile function"), so malformed inputs are checked
-- through the guard the converter uses instead of calling the converter.
with conversion_cases as (
    select * from (values
        ('1', true),
        ('0', false),
        (null::varchar, null::boolean)
    ) as t(input, expected)
),

conversion_mismatches as (
    select
        input,
        expected,
        {{ gtfs_bool('input') }} as actual
    from conversion_cases
    where {{ gtfs_bool('input') }} is distinct from expected
),

malformed_cases as (
    select * from (values
        ('2'),
        (' 1'),
        ('true'),
        ('')
    ) as t(input)
),

accepted_malformed as (
    select input
    from malformed_cases
    where {{ is_valid_gtfs_bool('input') }}
)

select
    'conversion mismatch' as failure,
    input,
    cast(expected as varchar) as expected,
    cast(actual as varchar) as actual
from conversion_mismatches

union all

select
    'malformed flag accepted',
    input,
    'error',
    'boolean'
from accepted_malformed
