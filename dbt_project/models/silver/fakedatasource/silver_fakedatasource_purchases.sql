-- GENERATED FILE — DO NOT EDIT MANUALLY
-- source: fakedatasource | table: purchases | strategy: incremental
-- generated_at: 2026-06-04T12:27:55.026191Z

{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        tags=['silver', 'generated', 'fakedatasource']
    )
}}

with bronze as (
    select * from {{ ref('bronze_fakedatasource_purchases') }}

    {% if is_incremental() %}
    where _extracted_at > (select coalesce(max(_extracted_at), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}
),

deduped as (
    select
        added_to_cart_at,
        created_at,
        id,
        product_id,
        purchased_at,
        returned_at,
        updated_at,
        user_id,
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
        added_to_cart_at,
        created_at,
        id,
        product_id,
        purchased_at,
        returned_at,
        updated_at,
        user_id,
        _raw_id,
        _extracted_at,
        _loaded_at,
        current_timestamp() as _platform_updated_at

    from deduped
    where _rn = 1
)

select * from silver