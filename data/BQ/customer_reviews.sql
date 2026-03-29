CREATE OR REPLACE EXTERNAL TABLE `project-467f29c9-4435-4634-97d.bronze_dataset.customer_reviews` (
  id STRING,
  customer_id INT64,
  product_id INT64,
  rating INT64,
  review_text STRING,
  review_date INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://retailer-datalake-project-05022026/landing/customer_reviews/customer_reviews_*.parquet']
);



========================================

--Step 1: Create the customer_reviews Table in the Silver Layer
CREATE TABLE IF NOT EXISTS `project-467f29c9-4435-4634-97d.silver_dataset.customer_reviews`
(
    id STRING,
    customer_id INT64,
    product_id INT64,
    rating INT64,
    review_text STRING,
    review_date STRING,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOL
);

--Step 2: Close existing active records if data changed
MERGE INTO `project-467f29c9-4435-4634-97d.silver_dataset.customer_reviews` target
USING (
  SELECT
    id,
    customer_id,
    product_id,
    rating,
    review_text,
    CAST(review_date AS STRING) AS review_date,
    CURRENT_TIMESTAMP() AS effective_start_date,
    CURRENT_TIMESTAMP() AS effective_end_date,
    TRUE AS is_active
  FROM `project-467f29c9-4435-4634-97d.bronze_dataset.customer_reviews`
) source
ON target.id = source.id AND target.is_active = TRUE
WHEN MATCHED AND (
    target.customer_id != source.customer_id OR
    target.product_id != source.product_id OR
    target.rating != source.rating OR
    target.review_text != source.review_text OR
    target.review_date != source.review_date
)
THEN UPDATE SET
  target.is_active = FALSE,
  target.effective_end_date = CURRENT_TIMESTAMP();

--Step 3: Insert new or updated records
MERGE INTO `project-467f29c9-4435-4634-97d.silver_dataset.customer_reviews` target
USING (
  SELECT
    id,
    customer_id,
    product_id,
    rating,
    review_text,
    CAST(review_date AS STRING) AS review_date,
    CURRENT_TIMESTAMP() AS effective_start_date,
    CURRENT_TIMESTAMP() AS effective_end_date,
    TRUE AS is_active
  FROM `project-467f29c9-4435-4634-97d.bronze_dataset.customer_reviews`
) source
ON target.id = source.id AND target.is_active = TRUE
WHEN NOT MATCHED THEN
  INSERT (
    id,
    customer_id,
    product_id,
    rating,
    review_text,
    review_date,
    effective_start_date,
    effective_end_date,
    is_active
  )
  VALUES (
    source.id,
    source.customer_id,
    source.product_id,
    source.rating,
    source.review_text,
    source.review_date,
    source.effective_start_date,
    source.effective_end_date,
    source.is_active
  );