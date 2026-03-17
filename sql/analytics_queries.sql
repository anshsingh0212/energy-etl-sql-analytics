-- ============================================================
-- analytics_queries.sql
-- Production-grade SQL analytics for smart meter MDMS database
-- Author: Ansh Singh
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 1. MONTHLY KPI DASHBOARD
--    Overall SLA metrics per month
-- ────────────────────────────────────────────────────────────
SELECT
    month_year,
    COUNT(DISTINCT consumer_id)                                    AS active_consumers,
    ROUND(AVG(data_availability_pct), 2)                           AS avg_data_availability_pct,
    ROUND(SUM(total_energy_kwh), 2)                                AS total_energy_kwh,
    ROUND(SUM(billing_amount), 2)                                  AS total_billing_inr,
    ROUND(AVG(total_energy_kwh), 2)                                AS avg_consumption_per_consumer,
    SUM(comm_failure_days)                                         AS total_comm_failure_days
FROM fact_monthly_kpi
GROUP BY month_year
ORDER BY month_year;