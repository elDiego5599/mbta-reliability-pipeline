-- DuckDB 2.0.1 rejects try() around error() ("TRY can not be used in
-- combination with a volatile function"), so malformed inputs are checked
-- through the guard the converter uses instead of calling the converter.
with conversion_cases as (
    select * from (values
        ('08:00:00', 28800::integer),
        ('8:00:00', 28800::integer),
        ('26:57:00', 97020::integer),
        ('00:00:00', 0::integer),
        (null::varchar, null::integer)
    ) as t(input, expected)
),

conversion_mismatches as (
    select
        input,
        expected,
        {{ gtfs_time_to_seconds('input') }} as actual
    from conversion_cases
    where {{ gtfs_time_to_seconds('input') }} is distinct from expected
),

malformed_cases as (
    select * from (values
        ('08:65:99'),
        ('08:00:60'),
        ('1:2:3:4'),
        ('-1:00:00'),
        ('8h'),
        ('08:00'),
        (''),
        (' 08:00:00')
    ) as t(input)
),

accepted_malformed as (
    select input
    from malformed_cases
    where {{ is_valid_gtfs_time('input') }}
)

select
    'conversion mismatch' as failure,
    input,
    cast(expected as varchar) as expected,
    cast(actual as varchar) as actual
from conversion_mismatches

union all

select
    'malformed time accepted',
    input,
    'error',
    'seconds'
from accepted_malformed
