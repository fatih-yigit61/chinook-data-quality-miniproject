-- This query classifies customers based on their total spending into 3 segments:
-- highValue (> $50), midValue ($20–$50), and lowValue (< $20).
-- It then aggregates the number of customers in each segment per sales support agent.


with customerSpending as (
    select 
        c.CustomerId,
        c.SupportRepId,
        sum(i.Total) as totalSpent,
        case 
            when sum(i.Total) > 50 then 'highValue'
            when sum(i.Total) between 20 and 50 then 'midValue'
            else 'lowValue'
        end as customerSegment
    from Customer c
    left join Invoice i on c.CustomerId = i.CustomerId
    group by c.CustomerId, c.SupportRepId
),
repSegments as (
    select 
        e.FirstName + ' ' + e.LastName as employeeName,
        cs.customerSegment,
        count(cs.CustomerId) as customerCount
    from customerSpending cs
    left join Employee e on cs.SupportRepId = e.EmployeeId
    where lower(e.Title) = 'sales support agent'
    group by e.FirstName, e.LastName, cs.customerSegment
)
select 
    employeeName,
    sum(case when customerSegment = 'highValue' then customerCount else 0 end) as highValueCount,
    sum(case when customerSegment = 'midValue' then customerCount else 0 end) as midValueCount,
    sum(case when customerSegment = 'lowValue' then customerCount else 0 end) as lowValueCount,
    sum(customerCount) as totalCustomers
from repSegments
group by employeeName;
----------------------------------------------------------------------
-- Monthly Support Load Report
-- This query shows how many invoices were generated each month by customers under each sales support agent.
-- Useful for identifying seasonal patterns or support team workload distribution.

select 
    e.FirstName + ' ' + e.LastName as employeeName,
    format(i.InvoiceDate, 'yyyy-MM') as invoiceMonth,
    count(i.InvoiceId) as invoiceCount,
    sum(i.Total) as totalRevenue
from Employee e
left join Customer c on c.SupportRepId = e.EmployeeId
left join Invoice i on i.CustomerId = c.CustomerId
where lower(e.Title) = 'sales support agent'
group by e.FirstName, e.LastName, format(i.InvoiceDate, 'yyyy-MM')
order by employeeName, invoiceMonth;


--------------------------------------------------------------------
-- Customer Lifetime Value Report
-- This query calculates the average lifetime revenue per customer managed by each sales support agent.
-- It includes total revenue and customer counts to assess representative performance.
with customerLTV as (
    select 
        c.SupportRepId,
        c.CustomerId,
        sum(i.Total) as totalRevenue
    from Customer c
    join Invoice i on c.CustomerId = i.CustomerId
    group by c.SupportRepId, c.CustomerId
)
select 
    e.FirstName + ' ' + e.LastName as employeeName,
    count(cl.CustomerId) as totalCustomers,
    sum(cl.totalRevenue) as totalRevenue,
    avg(cl.totalRevenue) as avgCustomerLTV
from customerLTV cl 
join Employee e on cl.SupportRepId = e.EmployeeId
where lower(e.Title) = 'sales support agent'
group by e.FirstName, e.LastName;
-----------------------------------------------------------------
-- Top Artists by Revenue per Support Representative
-- This query calculates total revenue generated per artist, grouped by support representative.

with artistRevenue as (
    select 
        e.FirstName + ' ' + e.LastName as employeeName,
        a.Name as artistName,
        sum(il.UnitPrice * il.Quantity) as revenue
    from Employee e
    join Customer c on c.SupportRepId = e.EmployeeId
    join Invoice i on i.CustomerId = c.CustomerId
    join InvoiceLine il on il.InvoiceId = i.InvoiceId
    join Track t on t.TrackId = il.TrackId
    join Album al on al.AlbumId = t.AlbumId
    join Artist a on a.ArtistId = al.ArtistId
    where lower(e.Title) = 'sales support agent'
    group by e.EmployeeId, e.FirstName, e.LastName, a.Name
)
select *
from artistRevenue
order by employeeName, revenue desc;

----------------------------------------------------------------------------------
-- Repeat Customer Rate per Representative
-- This query shows how many customers made more than one purchase,
-- calculating the repeat rate per support rep.

with customerInvoiceCount as (
    select 
        c.CustomerId,
        c.SupportRepId,
        count(i.InvoiceId) as invoiceCount
    from Customer c
    join Invoice i on i.CustomerId = c.CustomerId
    group by c.CustomerId, c.SupportRepId
),
repeatStats as (
    select 
        e.FirstName + ' ' + e.LastName as employeeName,
        count(case when cic.invoiceCount > 1 then 1 end) as repeatCustomerCount,
        count(*) as totalCustomers,
        cast(100.0 * count(case when cic.invoiceCount > 1 then 1 end) / count(*) as decimal(5,2)) as repeatCustomerRate
    from customerInvoiceCount cic
    join Employee e on cic.SupportRepId = e.EmployeeId
    where lower(e.Title) = 'sales support agent'
    group by e.FirstName, e.LastName
)
select * from repeatStats;

--------------------------------------------------------------------
-- 🎶 Most Popular Genre per Representative
-- This query finds the top-selling genre for each support representative
-- based on the number of tracks purchased.

with genreCounts as (
    select 
        e.EmployeeId,
        e.FirstName + ' ' + e.LastName as employeeName,
        g.Name as genreName,
        count(*) as genrePurchaseCount
    from Employee e
    join Customer c on c.SupportRepId = e.EmployeeId
    join Invoice i on i.CustomerId = c.CustomerId
    join InvoiceLine il on il.InvoiceId = i.InvoiceId
    join Track t on t.TrackId = il.TrackId
    join Genre g on g.GenreId = t.GenreId
    where lower(e.Title) = 'sales support agent'
    group by e.EmployeeId, e.FirstName, e.LastName, g.Name
),
rankedGenres as (
    select *,
           row_number() over (partition by employeeId order by genrePurchaseCount desc) as x
    from genreCounts
)
select 
    employeeName,
    genreName as topGenreName,
    genrePurchaseCount
from rankedGenres
where x = 1;



---------------------------------------------------------------
-- NULL Data Audit Report for Support Load Analysis
-- This report investigates critical NULLs that may affect the accuracy of workload and behavior analyses.
-- It checks for missing support assignments, undefined music attributes, and incomplete financial data.

-- Customers without assigned Support Representative
select 
    count(*) as unassignedCustomerCount,
    count(*) * 100.0 / (select count(*) from Customer) as unassignedCustomerRate
from Customer
where SupportRepId is null;

-- Invoices without assigned Customer (should be 0 in a clean DB)
select 
    count(*) as orphanInvoiceCount
from Invoice
where CustomerId is null;

-- Tracks without Composer (can affect genre/intellectual property analysis)
select 
    count(*) as nullComposerTrackCount,
    count(*) * 100.0 / (select count(*) from Track) as nullComposerRate
from Track
where Composer is null or ltrim(rtrim(Composer)) = '';

-- Tracks without Genre assigned (impacts genre-based analysis)
select 
    count(*) as nullGenreTrackCount
from Track
where GenreId is null;

-- InvoiceLines with NULL UnitPrice or Quantity (impacts revenue metrics)
select 
    count(*) as brokenInvoiceLines
from InvoiceLine
where UnitPrice is null or Quantity is null;

-- Summary: % of Tracks with Missing Attributes (Composer, Genre)
select 
    sum(case when Composer is null or ltrim(rtrim(Composer)) = '' then 1 else 0 end) as missingComposer,
    sum(case when GenreId is null then 1 else 0 end) as missingGenre,
    count(*) as totalTracks,
    round(100.0 * sum(case when Composer is null or ltrim(rtrim(Composer)) = '' then 1 else 0 end) / count(*), 2) as percentMissingComposer,
    round(100.0 * sum(case when GenreId is null then 1 else 0 end) / count(*), 2) as percentMissingGenre
from Track;
