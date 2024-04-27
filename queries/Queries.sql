
-- 1. Full table selection scan with mutliway joins. 
select fs.SaleID,fs.QuantitySold,fs.TotalSaleAmount,dp.ProductName,dp.ProductCategory,dp.ProductPrice,dd.Date,dd.DayOfWeek,dc.CustomerID,dc.CustomerName,dc.CustomerSegment,dc.Country,dc.City from FactSales fs
join DimProduct dp on dp.ProductID = fs.ProductID
join DimDate dd on dd.DateID = fs.DateID
join DimCustomer dc on dc.CustomerID = fs.CustomerID;



-- 2. Aggregation + multiway join and group by and sort by query:

-- this selects the best-performing sold categories and what which day it is mostly sold by grouping the data by ProductCategory and DayOfWeek at which an items is sold. and also count distinct  amount of customer who purchase in those days and those categories


-- aggregation with multi-table joins, grouping results by product category and day of the week, and sorting by total sales in descending order.
select dp.ProductCategory as "Category",dd.DayOfWeek,count(fs.SaleID) as "number of sales", round(sum(fs.TotalSaleAmount),2) as "total sales", count(dc.CustomerID) as "number of customers",from FactSales as fs
join DimProduct dp on dp.ProductID = fs.ProductID
join DimCustomer dc on dc.CustomerID = fs.CustomerID
join DimDate dd on dd.DateID = fs.DateID
group by dp.ProductCategory,dd.DayOfWeek
order by "total sales" desc;


-- 3. multiway join with a nested aggregation to filter and sort sales records that exceed the average total sale amount

select fs.SaleID,fs.QuantitySold,fs.TotalSaleAmount,dp.ProductName,dp.ProductCategory,dp.ProductPrice,dd.Date,dd.DayOfWeek,dc.CustomerID,dc.CustomerName,dc.CustomerSegment,dc.Country,dc.City from FactSales fs
join DimProduct dp on dp.ProductID = fs.ProductID
join DimDate dd on dd.DateID = fs.DateID
join DimCustomer dc on dc.CustomerID = fs.CustomerID
where (select avg(fs_2.TotalSaleAmount) from FactSales fs_2) < fs.TotalSaleAmount
order by QuantitySold desc, TotalSaleAmount desc;



-- this query will get the best sales grouped by and ordered by day of week where the product category sold is Clothing or Sports and the Country is either Country A or Country B.
select dd.DayOfWeek,
COUNT(DISTINCT fs.ProductID) AS NumberOfProducts,
COUNT(DISTINCT fs.CustomerID) AS NumberOfCustomers,
SUM(fs.QuantitySold) AS TotalProductsSold,
SUM(fs.TotalSaleAmount) AS TotalSales
FROM FactSales fs
JOIN DimDate dd ON dd.DateID = fs.DateID
JOIN DimProduct dp on dp.ProductID = fs.ProductID
join DimCustomer dc on dc.CustomerID = fs.CustomerID
WHERE (dp.ProductCategory = 'Clothing' OR dp.ProductCategory = 'Sports') AND (dc.Country = 'Country A' OR dc.Country = 'Country B')
GROUP BY dd.DayOfWeek
ORDER BY TotalSales
LIMIT 2;