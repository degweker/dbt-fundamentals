with

source as (

    select * from {{ source('jaffle_shop', 'customers') }}

),


transformed as (
    select 
        first_name || ' ' || last_name as name, 
        * 
      from source
)

select * from transformed