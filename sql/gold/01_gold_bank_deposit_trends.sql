CREATE VIEW dataops_sentinel.gold_bank_deposit_trends AS
WITH
  quarterly_data AS (
   SELECT
     CERT
   , BANK_NAME
   , report_date
   , DEP deposits
   , LAG(DEP) OVER (PARTITION BY CERT ORDER BY report_date ASC) previous_deposits
   , LAG(report_date) OVER (PARTITION BY CERT ORDER BY report_date ASC) previous_report_date
   FROM
     dataops_sentinel.fdic_financials_named
) 
SELECT
  CERT
, BANK_NAME
, report_date
, previous_report_date
, deposits
, previous_deposits
, (deposits - previous_deposits) deposit_change
, ROUND(((1E2 * (deposits - previous_deposits)) / NULLIF(previous_deposits, 0)), 2) deposit_change_pct
, (CASE WHEN (previous_deposits IS NULL) THEN 'NO_BASELINE' WHEN (previous_report_date <> date_format(date_add('day', -1, date_trunc('quarter', date_parse(report_date, '%Y%m%d'))), '%Y%m%d')) THEN 'MISSING_PREVIOUS_QUARTER' WHEN (previous_deposits <= 0) THEN 'INVALID_BASELINE' WHEN (((1E2 * (deposits - previous_deposits)) / NULLIF(previous_deposits, 0)) <= -20) THEN 'REVIEW' ELSE 'NORMAL' END) deposit_status
FROM
  quarterly_data