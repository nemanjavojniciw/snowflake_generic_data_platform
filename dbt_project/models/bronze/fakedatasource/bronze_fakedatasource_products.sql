-- GENERATED FILE — DO NOT EDIT MANUALLY
-- source: fakedatasource | table: products
-- generated_at: 2026-06-04T12:27:53.481224Z | schema_hash: 13f8c9e596463f970f9ec61ed3bbd4be

{{ config(tags=['bronze', 'generated', 'fakedatasource']) }}

with source as (
    select * from GENERIC_AIRBYTE_LANDING.GENERIC_AIRBYTE_LANDING.products
),

bronze as (
    select
        -- platform metadata (Destinations V2: no _airbyte_loaded_at)
        _airbyte_raw_id         as _raw_id,
        _airbyte_extracted_at   as _extracted_at,
        _airbyte_extracted_at   as _loaded_at,

        -- source columns
        created_at,
        id,
        make,
        model,
        price,
        updated_at,
        year,

        current_timestamp()     as _bronze_created_at

    from source
)

select * from bronze