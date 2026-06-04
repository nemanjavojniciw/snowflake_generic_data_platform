-- GENERATED FILE — DO NOT EDIT MANUALLY
-- source: fakedatasource | table: users
-- generated_at: 2026-06-04T12:27:53.481224Z | schema_hash: cdb8d23c990395c37e1f8eec64de2aa2

{{ config(tags=['bronze', 'generated', 'fakedatasource']) }}

with source as (
    select * from GENERIC_AIRBYTE_LANDING.GENERIC_AIRBYTE_LANDING.users
),

bronze as (
    select
        -- platform metadata (Destinations V2: no _airbyte_loaded_at)
        _airbyte_raw_id         as _raw_id,
        _airbyte_extracted_at   as _extracted_at,
        _airbyte_extracted_at   as _loaded_at,

        -- source columns
        academic_degree,
        address,
        age,
        blood_type,
        created_at,
        email,
        gender,
        height,
        id,
        language,
        name,
        nationality,
        occupation,
        telephone,
        title,
        updated_at,
        weight,

        current_timestamp()     as _bronze_created_at

    from source
)

select * from bronze