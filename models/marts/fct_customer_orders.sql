select 
    orders.id as order_id,
    orders.user_id as customer_id,
    last_name as surname,
    first_name as givenname,
    first_order_date,
    order_count,
    total_lifetime_value,
    round(amount/100.0,2) as order_value_dollars,
    orders.status as order_status,
    orders.status as payment_status
from {{ ref('stg_jaffle_shop_orders') }} as orders

join {{ ref('stg_jaffle_shop_customers') }}  customers
on orders.user_id = customers.id

join {{ ref('stg_jaffle_shop_customer_order_history') }} customer_order_history
on orders.user_id = customer_order_history.customer_id

left outer join {{ ref('stg_stripe_payments') }} payments
on orders.id = payments.order_id

where orders.status != 'fail'