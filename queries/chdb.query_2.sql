select 
    dp.productcategory as "category", 
    dd.dayofweek, 
    count(fs.saleid) as "number of sales", 
    round(sum(fs.totalsaleamount), 2) as "total sales", 
    count(fs.customerid) as "number of customers" 
from './csv_files/factsales.csv' as fs
join './csv_files/dimproduct.csv' as dp on dp.productid = fs.productid
join './csv_files/dimdate.csv' as dd on dd.dateid = fs.dateid
join './csv_files/dimcustomer.csv' as dc on dc.customerid = fs.customerid
group by dp.productcategory, dd.dayofweek
order by "total sales" desc
format Null;