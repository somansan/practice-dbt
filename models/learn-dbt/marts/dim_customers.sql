{{
    config(
        materialized='table'
    )
}}


with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customer_orders as (
    select 
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as recent_order_date,
        count(order_id) as num_orders
    from orders
    group by customer_id
),

final as (
    select 
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.recent_order_date,
        coalesce(customer_orders.num_orders, 0) as num_orders
    from customers
    left join customer_orders using (customer_id)
)

select * from final