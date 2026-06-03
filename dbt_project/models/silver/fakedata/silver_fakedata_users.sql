-- GENERATED FILE — DO NOT EDIT MANUALLY
-- source: fakedata | table: users | strategy: incremental
-- generated_at: 2026-06-03T14:02:26.666519Z

{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        tags=['silver', 'generated', 'fakedata']
    )
}}

with bronze as (
    select * from {{ ref('bronze_fakedata_users') }}

    {% if is_incremental() %}
    where _extracted_at > (select coalesce(max(_extracted_at), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}
),

deduped as (
    select
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
        _raw_id,
        _extracted_at,
        _loaded_at,
        row_number() over (
            partition by id
            order by _extracted_at desc
        ) as _rn

    from bronze
),

silver as (
    select
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
        _raw_id,
        _extracted_at,
        _loaded_at,
        current_timestamp() as _platform_updated_at

    from deduped
    where _rn = 1
)

select * from silver