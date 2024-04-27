with salessummary as (
      select
        dd.dayofweek,
        count(distinct fs.productid) as numberofproducts, -- count total unique products sold
        count(distinct fs.customerid) as numberofcustomers, -- count total unique customers
        sum(fs.quantitysold) as totalproductssold, -- get the total quantity sold
        sum(fs.totalsaleamount) as totalsales -- get the total sales amount
      from factsales fs
      join dimdate dd on dd.dateid = fs.dateid
      group by dd.dayofweek -- group by day of week 
    ),
    -- now lets rank the days of the week over total amount of sales so that we get the first two ranks
    rankedsales as (
      select
        *,
        rank() over (order by totalsales desc) as salesrank
      from salessummary
    )
    select
      dayofweek,
      numberofproducts,
      numberofcustomers,
      totalproductssold,
      totalsales,
      salesrank
    from rankedsales
    where salesrank in (1, 2);