CREATE TABLE report
AS
SELECT invoices.stockcode,
       invoices.country,
       AVG(invoices.unitprice) as avg_unit_price,
       AVG(invoices.quantity) as avg_quantity,
       AVG(invoices.unitprice) * AVG(invoices.quantity) as gain
FROM invoices
GROUP BY 1,2
ORDER BY 5 DESC
