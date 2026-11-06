CREATE VIEW dataops_sentinel.gold_unified_bank_monitoring AS
WITH
  combined AS (
   SELECT
     f.CERT
   , f.BANK_NAME
   , f.report_date
   , m.deposit_change_pct
   , m.deposit_flag
   , m.deposit_status
   , m.asset_change_pct
   , m.asset_status
   , p.current_roa
   , p.current_roe
   , p.roa_change_pp
   , p.roe_change_pp
   , p.comparison_status profitability_data_status
   , p.negative_roa_flag
   , p.roa_decline_flag
   , c.current_noncurrent_ratio
   , c.ratio_change_pp credit_change_pp
   , c.credit_status
   , c.comparison_status credit_data_status
   , k.current_capital_ratio
   , k.capital_change_pp
   , k.capital_status
   , k.capital_data_status
   FROM
     ((((dataops_sentinel.fdic_financials_named f
   LEFT JOIN dataops_sentinel.gold_bank_monitoring m ON ((f.CERT = m.CERT) AND (f.report_date = m.report_date)))
   LEFT JOIN dataops_sentinel.gold_bank_profitability p ON ((f.CERT = p.CERT) AND (f.report_date = p.report_date)))
   LEFT JOIN dataops_sentinel.gold_bank_credit_quality c ON ((f.CERT = c.CERT) AND (f.report_date = c.report_date)))
   LEFT JOIN dataops_sentinel.gold_bank_capital_adequacy k ON ((f.CERT = k.CERT) AND (f.report_date = k.report_date)))
) 
, signals AS (
   SELECT
     *
   , (CASE WHEN (deposit_flag IN ('BOTH', 'PERCENT_ONLY', 'ABSOLUTE_ONLY')) THEN 1 ELSE 0 END) deposit_alert
   , (CASE WHEN (asset_status = 'REVIEW') THEN 1 ELSE 0 END) asset_alert
   , (CASE WHEN ((negative_roa_flag = true) OR (roa_decline_flag = true)) THEN 1 ELSE 0 END) profitability_alert
   , (CASE WHEN (credit_status = 'CREDIT_DETERIORATION_REVIEW') THEN 1 ELSE 0 END) credit_alert
   , (CASE WHEN (capital_status = 'CAPITAL_DECLINE_REVIEW') THEN 1 ELSE 0 END) capital_alert
   , (CASE WHEN ((deposit_status IS NULL) OR (NOT (deposit_status IN ('NORMAL', 'REVIEW'))) OR (asset_status IS NULL) OR ((asset_status <> 'NORMAL') AND (asset_status <> 'REVIEW')) OR (profitability_data_status IS NULL) OR (profitability_data_status <> 'VALID') OR (credit_data_status IS NULL) OR (credit_data_status <> 'VALID') OR (capital_data_status IS NULL) OR (capital_data_status <> 'STANDARD_COMPARISON')) THEN true ELSE false END) has_data_limitation
   FROM
     combined
) 
, scored AS (
   SELECT
     *
   , ((((deposit_alert + asset_alert) + profitability_alert) + credit_alert) + capital_alert) alert_count
   FROM
     signals
) 
SELECT
  *
, (CASE WHEN (alert_count >= 3) THEN 'HIGH_REVIEW_PRIORITY' WHEN (alert_count = 2) THEN 'MEDIUM_REVIEW_PRIORITY' WHEN (alert_count = 1) THEN 'LOW_REVIEW_PRIORITY' WHEN has_data_limitation THEN 'INSUFFICIENT_DATA' ELSE 'NO_ALERT' END) investigation_priority
FROM
  scored