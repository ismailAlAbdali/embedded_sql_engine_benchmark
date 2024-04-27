SELECT
  fs.saleid,
  fs.quantitysold,
  fs.totalsaleamount,
  dp.productname,
  dp.productcategory,
  dp.productprice,
  dd.date,
  dd.dayofweek,
  dc.customerid,
  dc.customername,
  dc.customersegment,
  dc.country,
  dc.city
FROM
  './csv_files/factsales.csv' as fs
JOIN
  './csv_files/dimproduct.csv' as  dp ON dp.productid = fs.productid
JOIN
  './csv_files/dimdate.csv' as dd ON dd.dateid = fs.dateid
JOIN
  './csv_files/dimcustomer.csv' as  dc ON dc.customerid = fs.customerid;