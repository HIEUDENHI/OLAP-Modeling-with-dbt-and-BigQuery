SELECT rd.Country,pr.Ram,ROUND(SUM(sf.Quantity),0) AS Total_Quantity

FROM {{ref('sales_fact')}} sf

JOIN {{ref('region_dim')}} rd ON rd.region_id_pk=sf.region_id_pk
JOIN {{ref('product_dim')}} pr ON pr.product_id_pk=sf.product_id_pk
GROUP BY rd.Country, pr.Ram
