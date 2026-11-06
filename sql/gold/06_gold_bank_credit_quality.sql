CREATE VIEW dataops_sentinel.gold_bank_credit_quality AS
WITH
  credit_history AS (
   SELECT
     CERT
   , BANK_NAME
   , report_date
   , NCLNLSR current_noncurrent_ratio
   , LNLSGR current_gross_loans
   , LAG(NCLNLSR) OVER (PARTITION BY CERT ORDER BY report_date ASC) previous_noncurrent_ratio
   , LAG(report_date) OVER (PARTITION BY CERT ORDER BY report_date ASC) previous_report_date
   FROM
     dataops_sentinel.fdic_financials_named
) 
, validated AS (
   SELECT
     *
   , (CASE WHEN (previous_report_date IS NULL) THEN 'NO_BASELINE' WHEN (previous_report_date <> date_format(date_add('day', -1, date_trunc('quarter', date_parse(report_date, '%Y%m%d'))), '%Y%m%d')) THEN 'MISSING_PREVIOUS_QUARTER' WHEN ((current_noncurrent_ratio IS NULL) OR (previous_noncurrent_ratio IS NULL)) THEN 'MISSING_CREDIT_DATA' WHEN ((current_noncurrent_ratio < 0) OR (previous_noncurrent_ratio < 0)) THEN 'INVALID_RATIO' ELSE 'VALID' END) comparison_status
   FROM
     credit_history
) 
SELECT
  CERT
, BANK_NAME
, report_date
, previous_report_date
, previous_noncurrent_ratio
, current_noncurrent_ratio
, current_gross_loans
, (CASE WHEN (comparison_status = 'VALID') THEN ROUND((current_noncurrent_ratio - previous_noncurrent_ratio), 2) ELSE null END) ratio_change_pp
, comparison_status
, (CASE WHEN (comparison_status <> 'VALID') THEN 'NOT_EVALUATED' WHEN ((current_noncurrent_ratio - previous_noncurrent_ratio) >= 1E0) THEN 'CREDIT_DETERIORATION_REVIEW' ELSE 'NORMAL' END) credit_status
FROM
  validated