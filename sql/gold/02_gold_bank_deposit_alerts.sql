CREATE VIEW dataops_sentinel.gold_bank_deposit_alerts AS
WITH
  valid_deposit_changes AS (
   SELECT
     CERT
   , BANK_NAME
   , report_date
   , previous_report_date
   , previous_deposits
   , deposits
   , deposit_change
   , deposit_change_pct
   , ((1E2 * deposit_change) / NULLIF(previous_deposits, 0)) exact_change_pct
   FROM
     dataops_sentinel.gold_bank_deposit_trends
   WHERE ((deposit_status IN ('NORMAL', 'REVIEW')) AND (previous_deposits > 0) AND (deposits >= 0))
) 
, classified AS (
   SELECT
     CERT
   , BANK_NAME
   , report_date
   , previous_report_date
   , previous_deposits
   , deposits
   , deposit_change
   , deposit_change_pct
   , (CASE WHEN ((exact_change_pct <= -20) AND (deposit_change <= -1000000)) THEN 'BOTH' WHEN (exact_change_pct <= -20) THEN 'PERCENT_ONLY' WHEN (deposit_change <= -1000000) THEN 'ABSOLUTE_ONLY' ELSE 'NORMAL' END) investigation_flag
   FROM
     valid_deposit_changes
) 
SELECT
  CERT
, BANK_NAME
, report_date
, previous_report_date
, previous_deposits
, deposits
, deposit_change
, deposit_change_pct
, investigation_flag
FROM
  classified