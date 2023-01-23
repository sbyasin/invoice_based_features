CREATE OR REPLACE TABLE `fb-data-science-dev.invoice_features.invoice` AS (
  SELECT
    systemid,
    active,
    amount,
    create_date,
    created_at,
    currency_code,
    customerid,
    date_paid,
    deposit_percentage,
    deposit_status,
    estimateid,
    fulfillment_date,
    generation_date,
    invoice_number,
    invoiceid,
    `language` as invoice_language,
    paid,
    payment_status
  FROM `dataops-replica-production.coalesced_live_shards.invoice`
  WHERE create_date>='2019-01-01' and create_date<'2023-01-01'
)
;