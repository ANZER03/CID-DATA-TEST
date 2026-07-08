

WITH raw_data AS (
    
    SELECT * FROM nessie.bronze.transactions
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id_transaction 
            ORDER BY CAST(timestamp AS TIMESTAMP) DESC
        ) as row_num
    FROM raw_data
)

SELECT 
    id_transaction,
    UPPER(client) as client_name,
    produit as product_name,
    prix as price,
    CAST(timestamp AS TIMESTAMP) as transaction_time,
    current_timestamp() as processed_at
FROM deduplicated
WHERE row_num = 1