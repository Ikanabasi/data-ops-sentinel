CREATE VIEW dataops_sentinel.gold_bank_capital_adequacy AS
WITH
  capital_history AS (
   SELECT
     CERT
   , BANK_NAME
   , report_date
   , RBCRWAJ current_capital_ratio
   , LAG(RBCRWAJ) OVER (PARTITION BY CERT ORDER BY report_date ASC) previous_capital_ratio
   , LAG(report_date) OVER (PARTITION BY CERT ORDER BY report_date ASC) previous_report_date
   FROM
     dataops_sentinel.fdic_financials_named
) 
, validated AS (
   SELECT
     *
   , (CASE WHEN (previous_report_date IS NULL) THEN 'NO_BASELINE' WHEN (previous_report_date <> date_format(date_add('day', -1, date_trunc('quarter', date_parse(report_date, '%Y%m%d'))), '%Y%m%d')) THEN 'MISSING_PREVIOUS_QUARTER' WHEN ((previous_capital_ratio IS NULL) OR (current_capital_ratio IS NULL)) THEN 'MISSING_DATA' WHEN ((previous_capital_ratio < 0) OR (current_capital_ratio < 0)) THEN 'NEGATIVE_RATIO_REVIEW' WHEN ((previous_capital_ratio = 0) OR (current_capital_ratio = 0)) THEN 'ZERO_RATIO_REVIEW' WHEN ((previous_capital_ratio > 100) OR (current_capital_ratio > 100)) THEN 'HIGH_RATIO_REVIEW' ELSE 'STANDARD_COMPARISON' END) capital_data_status
   FROM
     capital_history
) 
SELECT
  CERT
, BANK_NAME
, report_date
, previous_report_date
, previous_capital_ratio
, current_capital_ratio
, (CASE WHEN ((previous_capital_ratio IS NOT NULL) AND (current_capital_ratio IS NOT NULL)) THEN ROUND((current_capital_ratio - previous_capital_ratio), 2) ELSE null END) capital_change_pp
, capital_data_status
, (CASE WHEN ((capital_data_status = 'STANDARD_COMPARISON') AND ((current_capital_ratio - previous_capital_ratio) <= -2E0)) THEN 'CAPITAL_DECLINE_REVIEW' WHEN (capital_data_status = 'STANDARD_COMPARISON') THEN 'NORMAL' ELSE 'NOT_EVALUATED' END) capital_status
FROM
  validated