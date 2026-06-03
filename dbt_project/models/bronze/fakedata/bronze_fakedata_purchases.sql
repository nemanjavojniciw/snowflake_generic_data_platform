-- GENERATED FILE — DO NOT EDIT MANUALLY
-- source: fakedata | table: purchases
-- generated_at: 2026-06-03T11:21:14.538108Z | schema_hash: 190b226d5a368a0c4b876a19fe8f954c

{{ config(tags=['bronze', 'generated', 'fakedata']) }}

with source as (
    select * from GENERIC_AIRBYTE_LANDING.GENERIC_AIRBYTE_LANDING.purchases
),

bronze as (
    select
        -- platform metadata (Destinations V2: no _airbyte_loaded_at)
        _airbyte_raw_id         as _raw_id,
        _airbyte_extracted_at   as _extracted_at,
        _airbyte_extracted_at   as _loaded_at,

        -- source columns
        added_to_cart_at,
        created_at,
        id,
        product_id,
        purchased_at,
        returned_at,
        updated_at,
        user_id,

        current_timestamp()     as _bronze_created_at

    from source
)

select * from bronze