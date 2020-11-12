CREATE TABLE report{{ ds_nodash }}
AS
SELECT invoices.stockcode,
       invoices.country,
       AVG(invoices.unitprice) as avg_unit_price,
       AVG(invoices.quantity) as avg_quantity,
       AVG(invoices.unitprice) * AVG(invoices.quantity) as gain
FROM invoices
WHERE invoices.invoicedate = '{{ ds }}'
GROUP BY 1,2
ORDER BY 5 DESC
