use Chinook;
-- ---------------------------------------------------------------------------------------
-- scenario: total revenue loss estimation due to nulls
-- description: estimate how much revenue is potentially lost due to missing unit price or quantity in invoice lines.
-- insight: quantifies financial risk or reporting gaps caused by data quality issues.
-- ---------------------------------------------------------------------------------------

select 
    sum(case 
            when il.UnitPrice is null or il.Quantity is null then 1 
            else 0 
        end) as AffectedRows,
    sum(case 
            when il.UnitPrice is null or il.Quantity is null then 0 
            else il.UnitPrice * il.Quantity 
        end) as ValidRevenue,
    sum(case 
            when il.UnitPrice is null or il.Quantity is null then il.UnitPrice * isnull(il.Quantity, 1)
            else 0 
        end) as LostRevenueEstimate
from InvoiceLine il;




-- ---------------------------------------------------------------------------------------
-- scenario: repeated nulls by customer
-- description: identify customers who are associated with multiple invoice lines containing nulls.
-- insight: helps pinpoint unreliable data sources or systems causing repeat data issues.
-- ---------------------------------------------------------------------------------------

select 
    c.CustomerId,
    c.FirstName + ' ' + c.LastName as CustomerName,
    count(*) as NullAffectedLines
from Customer c
join Invoice i on c.CustomerId = i.CustomerId
join InvoiceLine il on i.InvoiceId = il.InvoiceId
where il.UnitPrice is null or il.Quantity is null or il.TrackId is null
group by c.CustomerId, c.FirstName, c.LastName
order by NullAffectedLines desc;



-- ---------------------------------------------------------------------------------------
-- scenario: invoices with null or zero revenue
-- description: find invoices where all lines are missing critical data or result in zero total.
-- insight: may indicate incomplete billing or failed transaction logs.
-- ---------------------------------------------------------------------------------------

select 
    i.InvoiceId,
    i.InvoiceDate,
    sum(il.UnitPrice * il.Quantity) as InvoiceTotal,
    count(*) as LineCount,
    sum(case 
            when il.UnitPrice is null or il.Quantity is null then 1 
            else 0 
        end) as NullLines
from Invoice i
join InvoiceLine il on i.InvoiceId = il.InvoiceId
group by i.InvoiceId, i.InvoiceDate
having sum(il.UnitPrice * il.Quantity) is null or sum(il.UnitPrice * il.Quantity) = 0
order by i.InvoiceDate desc;


-- ---------------------------------------------------------------------------------------
-- scenario: null rate summary per invoice field
-- description: show null distribution across key fields in invoice line data.
-- insight: field-level focus allows targeted fixes or schema enforcement.
-- ---------------------------------------------------------------------------------------

select 
    'TrackId' as FieldName,
    count(*) as NullCount
from InvoiceLine
where TrackId is null

union all

select 
    'UnitPrice',
    count(*)
from InvoiceLine
where UnitPrice is null

union all

select 
    'Quantity',
    count(*)
from InvoiceLine
where Quantity is null;
-- ---------------------------------------------------------------------------------------
-- scenario: null density by table
-- description: calculate percentage of NULLs for selected key fields across important tables.
-- insight: helps prioritize tables with the most serious data quality risks.
-- ---------------------------------------------------------------------------------------

select 'InvoiceLine' as TableName, 
    round(100.0 * sum(case when UnitPrice is null or Quantity is null or TrackId is null then 1 else 0 end) / count(*), 2) as NullRate
from InvoiceLine

union all

select 'Track',
    round(100.0 * sum(case when GenreId is null or AlbumId is null then 1 else 0 end) / count(*), 2)
from Track

union all

select 'Customer',
    round(100.0 * sum(case when Email is null or Country is null then 1 else 0 end) / count(*), 2)
from Customer;




-- ---------------------------------------------------------------------------------------
-- scenario: null clusters (rows with multiple null fields)
-- description: detect rows that have multiple nulls simultaneously, indicating severe data loss.
-- insight: helpful for targeted cleaning or exclusion from exports.
-- ---------------------------------------------------------------------------------------

select 
    InvoiceLineId,
    case when TrackId is null then 1 else 0 end +
    case when UnitPrice is null then 1 else 0 end +
    case when Quantity is null then 1 else 0 end as NullFieldCount
from InvoiceLine
where TrackId is null or UnitPrice is null or Quantity is null
order by NullFieldCount desc;




-- ---------------------------------------------------------------------------------------
-- scenario: null trend over time
-- description: analyze whether the frequency of nulls increases or decreases over time.
-- insight: may indicate recent system issues or improvement in data pipelines.
-- ---------------------------------------------------------------------------------------

select 
    cast(i.InvoiceDate as date) as InvoiceDay,
    count(*) as TotalLines,
    sum(case when il.UnitPrice is null or il.Quantity is null or il.TrackId is null then 1 else 0 end) as NullLineCount
from Invoice i
join InvoiceLine il on i.InvoiceId = il.InvoiceId
group by cast(i.InvoiceDate as date)
order by InvoiceDay;


-- ---------------------------------------------------------------------------------------
-- scenario: invoices with only null invoice lines
-- description: detect invoices where all lines are affected by nulls.
-- insight: these invoices should probably be excluded or flagged for re-processing.
-- ---------------------------------------------------------------------------------------

select 
    i.InvoiceId,
    count(*) as TotalLines,
    sum(case when il.UnitPrice is null or il.Quantity is null or il.TrackId is null then 1 else 0 end) as NullLines
from Invoice i
join InvoiceLine il on i.InvoiceId = il.InvoiceId
group by i.InvoiceId
having count(*) = sum(case when il.UnitPrice is null or il.Quantity is null or il.TrackId is null then 1 else 0 end);




-- ---------------------------------------------------------------------------------------
-- scenario: missing data impact on artist revenue attribution
-- description: estimate how much revenue cannot be attributed to any artist due to missing album or artist info.
-- insight: such cases cause reporting issues, unfair royalty splits, and flawed analytics.
-- ---------------------------------------------------------------------------------------

select 
    case 
        when al.AlbumId is null then 'missing album'
        when ar.ArtistId is null then 'missing artist'
        else 'complete'
    end as AttributionStatus,
    sum(il.UnitPrice * il.Quantity) as Revenue
from InvoiceLine il
left join Track t on il.TrackId = t.TrackId
left join Album al on t.AlbumId = al.AlbumId
left join Artist ar on al.ArtistId = ar.ArtistId
group by 
    case 
        when al.AlbumId is null then 'missing album'
        when ar.ArtistId is null then 'missing artist'
        else 'complete'
    end
order by Revenue desc;







