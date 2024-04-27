select fs.saleid, fs.quantitysold, fs.totalsaleamount, dp.productname, dp.productcategory, dp.productprice, dd.date, dd.dayofweek, dc.customerid, dc.customername, dc.customersegment, dc.country, dc.city 
from './csv_files/factsales.csv' as fs
join './csv_files/dimproduct.csv' as dp on dp.productid = fs.productid
join './csv_files/dimdate.csv' as dd on dd.dateid = fs.dateid
join './csv_files/dimcustomer.csv' as dc on dc.customerid = fs.customerid
where (select avg(fs_2.totalsaleamount) from './csv_files/factsales.csv' fs_2) < fs.totalsaleamount
order by quantitysold desc, totalsaleamount desc;