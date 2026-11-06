CREATE VIEW dataops_sentinel.gold_bank_profitability AS
WITH
  history AS (
   SELECT
     CERT
   , BANK_NAME
   , report_date
   , ROA current_roa
   , ROE current_roe
   , LAG(ROA) OVER (PARTITION BY CERT ORDER BY report_date ASC) previous_roa
   , LAG(ROE) OVER (PARTITION BY CERT ORDER BY report_date ASC) previous_roe
   , LAG(report_date) OVER (PARTITION BY CERT ORDER BY report_date ASC) previous_report_date
   FROM
     dataops_sentinel.fdic_financials_named
) 
, validated AS (
   SELECT
     *
   , (CASE WHEN (previous_report_date IS NULL) THEN 'NO_BASELINE' WHEN (previous_report_date <> date_format(date_add('day', -1, date_trunc('quarter', date_parse(report_date, '%Y%m%d'))), '%Y%m%d')) THEN 'MISSING_PREVIOUS_QUARTER' WHEN ((current_roa IS NULL) OR (previous_roa IS NULL) OR (current_roe IS NULL) OR (previous_roe IS NULL)) THEN 'MISSING_PROFITABILITY_DATA' ELSE 'VALID' END) comparison_status
   FROM
     history
) 
SELECT
  CERT
, BANK_NAME
, report_date
, previous_report_date
, previous_roa
, current_roa
, (CASE WHEN (comparison_status = 'VALID') THEN ROUND((current_roa - previous_roa), 2) END) roa_change_pp
, previous_roe
, current_roe
, (CASE WHEN (comparison_status = 'VALID') THEN ROUND((current_roe - previous_roe), 2) END) roe_change_pp
, comparison_status
, (CASE WHEN ((current_roa IS NOT NULL) AND (current_roa < 0)) THEN true ELSE false END) negative_roa_flag
, (CASE WHEN ((comparison_status = 'VALID') AND ((current_roa - previous_roa) <= -5E-1)) THEN true ELSE false END) roa_decline_flag
, (CASE WHEN ((current_roa IS NOT NULL) AND (ABS(current_roa) > 20)) THEN true ELSE false END) unusual_roa_flag
FROM
  validated