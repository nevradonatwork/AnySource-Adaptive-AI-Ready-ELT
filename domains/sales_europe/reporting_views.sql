-- domains/sales_europe/reporting_views.sql
-- Reporting views for the sales_europe domain.
-- Only join dim_* and fct_* tables — never raw_* or stg_*.

CREATE VIEW IF NOT EXISTS vw_rep_monthly_sales AS
SELECT
    substr(f.order_date, 1, 7)  AS month,
    f.region,
    SUM(f.total_amount)         AS revenue,
    SUM(f.order_count)          AS orders,
    SUM(f.total_quantity)       AS units_sold
FROM fct_sales f
GROUP BY substr(f.order_date, 1, 7), f.region;


CREATE VIEW IF NOT EXISTS vw_rep_active_customers AS
SELECT
    customer_id,
    customer_email,
    region,
    valid_from
FROM dim_customer
WHERE is_current = 1
ORDER BY customer_id;
