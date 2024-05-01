WITH salessummary AS (
  SELECT
    dd.dayofweek,
    COUNT(DISTINCT fs.productid) AS numberofproducts, 
    COUNT(DISTINCT fs.customerid) AS numberofcustomers, 
    SUM(fs.quantitysold) AS totalproductssold, 
    SUM(fs.totalsaleamount) AS totalsales 
  FROM './csv_files/factsales.csv' AS fs
  JOIN './csv_files/dimdate.csv' AS dd ON dd.dateid = fs.dateid
  GROUP BY dd.dayofweek
),
rankedsales AS (
  SELECT
    *,
    RANK() OVER (ORDER BY totalsales DESC) AS salesrank
  FROM salessummary
)
SELECT
  dayofweek,
  numberofproducts,
  numberofcustomers,
  totalproductssold,
  totalsales,
  salesrank
FROM rankedsales
WHERE salesrank IN (1, 2)
FORMAT Null; 