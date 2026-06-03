-- GENERATED FILE — DO NOT EDIT MANUALLY
-- source: fakedata | table: products | strategy: incremental
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
    select * from {{ ref('bronze_fakedata_products') }}

    {% if is_incremental() %}
    where _extracted_at > (select coalesce(max(_extracted_at), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}
),

deduped as (
    select
        created_at,
        id,
        make,
        model,
        price,
        updated_at,
        year,
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
        created_at,
        id,
        make,
        model,
        price,
        updated_at,
        year,
        _raw_id,
        _extracted_at,
        _loaded_at,
        current_timestamp() as _platform_updated_at

    from deduped
    where _rn = 1
)

select * from silver