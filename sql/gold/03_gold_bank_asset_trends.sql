CREATE VIEW dataops_sentinel.gold_bank_asset_trends AS
WITH
  quarterly_assets AS (
   SELECT
     CERT
   , BANK_NAME
   , report_date
   , ASSET assets
   , LAG(ASSET) OVER (PARTITION BY CERT ORDER BY report_date ASC) previous_assets
   , LAG(report_date) OVER (PARTITION BY CERT ORDER BY report_date ASC) previous_report_date
   FROM
     dataops_sentinel.fdic_financials_named
) 
, validated_assets AS (
   SELECT
     CERT
   , BANK_NAME
   , report_date
   , previous_report_date
   , assets
   , previous_assets
   , (CASE WHEN ((previous_report_date = date_format(date_add('day', -1, date_trunc('quarter', date_parse(report_date, '%Y%m%d'))), '%Y%m%d')) AND (previous_assets > 0) AND (assets >= 0)) THEN true ELSE false END) valid_comparison
   FROM
     quarterly_assets
) 
SELECT
  CERT
, BANK_NAME
, report_date
, previous_report_date
, assets
, previous_assets
, (CASE WHEN valid_comparison THEN (assets - previous_assets) ELSE null END) asset_change
, (CASE WHEN valid_comparison THEN ROUND(((1E2 * (assets - previous_assets)) / NULLIF(previous_assets, 0)), 2) ELSE null END) asset_change_pct
, (CASE WHEN (previous_assets IS NULL) THEN 'NO_BASELINE' WHEN ((NOT valid_comparison) AND (previous_report_date <> date_format(date_add('day', -1, date_trunc('quarter', date_parse(report_date, '%Y%m%d'))), '%Y%m%d'))) THEN 'MISSING_PREVIOUS_QUARTER' WHEN (NOT valid_comparison) THEN 'INVALID_BASELINE' WHEN (((1E2 * (assets - previous_assets)) / NULLIF(previous_assets, 0)) <= -20) THEN 'REVIEW' ELSE 'NORMAL' END) asset_status
FROM
  validated_assets