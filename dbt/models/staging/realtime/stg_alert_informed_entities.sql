-- One row per informed entity of an alert version.
with entities as (
    select
        alert_id,
        version_first_seen_ts,
        unnest(informed_entity) as entity,
        generate_subscripts(informed_entity, 1) as entity_index
    from {{ ref('stg_alerts') }}
)
select
    alert_id,
    version_first_seen_ts,
    entity_index,
    entity.agency_id,
    entity.route_id,
    entity.route_type,
    cast(entity.direction_id as integer) as direction_id,
    entity.stop_id,
    entity.trip.trip_id as trip__trip_id,
    entity.trip.route_id as trip__route_id,
    cast(entity.trip.direction_id as integer) as trip__direction_id,
    entity.trip.start_date as trip__start_date,
    entity.trip.start_time as trip__start_time,
    entity.trip.schedule_relationship as trip__schedule_relationship,
    entity.trip.modified_trip.modifications_id as trip__modified_trip__modifications_id,
    entity.trip.modified_trip.affected_trip_id as trip__modified_trip__affected_trip_id,
    entity.trip.modified_trip.start_time as trip__modified_trip__start_time,
    entity.trip.modified_trip.start_date as trip__modified_trip__start_date
from entities
