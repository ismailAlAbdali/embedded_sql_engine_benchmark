select fs.saleid, fs.quantitysold, fs.totalsaleamount, dp.productname, dp.productcategory, dp.productprice, dd.date, dd.dayofweek, dc.customerid, dc.customername, dc.customersegment, dc.country, dc.city from factsales fs
join dimproduct dp on dp.productid = fs.productid
join dimdate dd on dd.dateid = fs.dateid
join dimcustomer dc on dc.customerid = fs.customerid;