select dp.productcategory as "category", dd.dayofweek, count(fs.saleid) as "number of sales", round(sum(fs.totalsaleamount), 2) as "total sales", count(dc.customerid) as "number of customers" from factsales as fs
join dimproduct dp on dp.productid = fs.productid
join dimcustomer dc on dc.customerid = fs.customerid
join dimdate dd on dd.dateid = fs.dateid
group by dp.productcategory, dd.dayofweek
order by "total sales" desc;