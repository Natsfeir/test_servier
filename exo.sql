-- exo 1
SELECT 
  date,
  SUM(prod_price * prod_qty) AS ventes
FROM `sandbox-nbrami-sfeir.servier_test.transaction`
WHERE
  date BETWEEN '2019-01-01' AND '2019-12-31'
GROUP BY 
  date
ORDER BY 
  date
;


-- exo 2
WITH transa_product AS 
(
  SELECT 
    transa.*,
    product.product_type
  FROM 
    `sandbox-nbrami-sfeir.servier_test.transaction` AS transa
  LEFT JOIN
    `sandbox-nbrami-sfeir.servier_test.product` AS product
  ON
    transa.prod_id = product.product_id
  WHERE
    date BETWEEN '2019-01-01' AND '2030-12-31'
)

SELECT
  client_id,
  SUM(CASE WHEN product_type = 'MEUBLE' THEN prod_price * prod_qty ELSE 0 END) AS ventes_meuble,
  SUM(CASE WHEN product_type = 'DECO' THEN prod_price * prod_qty ELSE 0 END) AS ventes_deco,
FROM 
  transa_product
GROUP BY 
  client_id
ORDER BY 
  client_id;
