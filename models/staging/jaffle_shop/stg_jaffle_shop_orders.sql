
with

source as (

    select * from {{ source('jaffle_shop', 'orders') }}

),

transformed as (
    select         
        row_number() over (
            partition by user_id 
            order by order_date, id
        ) as user_order_seq,
        *
    from source
)

select * from transformed