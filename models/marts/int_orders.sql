with

orders as (

    select * from {{ ref('stg_jaffle_shop_orders') }}
),

payments as (

    select * from {{ ref('stg_stripe_payments') }}
),

order_totals as (

    select

    order_id,
    sum(payment_amount) as order_value_dollars

    from {{ ref('stg_stripe_payments') }}
    group by 1
),

joined as (
    
    select 
        orders.* , 
        order_totals.order_value_dollars

    from orders
    left join order_totals
    on orders.order_id = order_totals.order_id

)

select * from joined
