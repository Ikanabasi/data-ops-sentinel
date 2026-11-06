CREATE VIEW dataops_sentinel.gold_bank_monitoring AS
SELECT
  f.CERT
, f.BANK_NAME
, f.report_date
, d.previous_deposits
, d.deposits
, d.deposit_change
, d.deposit_change_pct
, dt.deposit_status
, COALESCE(d.investigation_flag, 'NOT_EVALUATED') deposit_flag
, a.previous_assets
, a.assets
, a.asset_change
, a.asset_change_pct
, a.asset_status
, (CASE WHEN ((a.asset_status = 'REVIEW') AND (d.investigation_flag IN ('BOTH', 'PERCENT_ONLY', 'ABSOLUTE_ONLY'))) THEN 'DEPOSIT_AND_ASSET' WHEN (a.asset_status = 'REVIEW') THEN 'ASSET_ONLY' WHEN (d.investigation_flag IN ('BOTH', 'PERCENT_ONLY', 'ABSOLUTE_ONLY')) THEN 'DEPOSIT_ONLY' WHEN ((a.asset_status = 'NORMAL') AND (d.investigation_flag = 'NORMAL')) THEN 'NORMAL' ELSE 'INSUFFICIENT_DATA' END) monitoring_status
FROM
  (((dataops_sentinel.fdic_financials_named f
LEFT JOIN dataops_sentinel.gold_bank_asset_trends a ON ((f.CERT = a.CERT) AND (f.report_date = a.report_date)))
LEFT JOIN dataops_sentinel.gold_bank_deposit_trends dt ON ((f.CERT = dt.CERT) AND (f.report_date = dt.report_date)))
LEFT JOIN dataops_sentinel.gold_bank_deposit_alerts d ON ((f.CERT = d.CERT) AND (f.report_date = d.report_date)))