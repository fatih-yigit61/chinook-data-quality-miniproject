
use Chinook;
-----------------------------------------------------------
-- Scenario: Monthly Top Revenue-Generating Artists
-- Identify the top earning artist for each month based on total track sales revenue.

with ArtistMonthlyRevenue as (
    select 
        a.ArtistId,
        ar.Name as ArtistName,
        format(i.InvoiceDate, 'yyyy-MM') as RevenueMonth,
        sum(il.UnitPrice * il.Quantity) as MonthlyRevenue
    from Invoice i
    join InvoiceLine il on i.InvoiceId = il.InvoiceId
    join Track t on il.TrackId = t.TrackId
    join Album a on t.AlbumId = a.AlbumId
    join Artist ar on a.ArtistId = ar.ArtistId
    group by a.ArtistId, ar.Name, format(i.InvoiceDate, 'yyyy-MM')
),
RankedArtists as (
    select *,
        rank() over (partition by RevenueMonth order by MonthlyRevenue desc) as MonthlyRank
    from ArtistMonthlyRevenue
)
select *
from RankedArtists
where MonthlyRank = 1
order by RevenueMonth;

---------------------------------------------------------
-- Scenario: Genre-Adjusted Revenue Share per Artist
-- Normalize artist revenue within their genre to find who dominates their category regardless of genre size.

with ArtistGenreRevenue as (
    select 
        ar.ArtistId,
        ar.Name as ArtistName,
        g.GenreId,
        g.Name as GenreName,
        sum(il.UnitPrice * il.Quantity) as ArtistRevenue
    from InvoiceLine il
    join Track t on il.TrackId = t.TrackId
    join Genre g on t.GenreId = g.GenreId
    join Album al on t.AlbumId = al.AlbumId
    join Artist ar on al.ArtistId = ar.ArtistId
    group by ar.ArtistId, ar.Name, g.GenreId, g.Name
),
GenreTotalRevenue as (
    select 
        GenreId,
        sum(il.UnitPrice * il.Quantity) as TotalGenreRevenue
    from InvoiceLine il
    join Track t on il.TrackId = t.TrackId
    group by t.GenreId
)
select 
    agr.ArtistName,
    agr.GenreName,
    agr.ArtistRevenue,
    gtr.TotalGenreRevenue,
    round(100.0 * agr.ArtistRevenue / gtr.TotalGenreRevenue, 2) as RevenueSharePercentage
from ArtistGenreRevenue agr
join GenreTotalRevenue gtr on agr.GenreId = gtr.GenreId
order by RevenueSharePercentage desc;




---------------------------------------------------------


-- Scenario: Revenue Efficiency - Artists with High Revenue per Track
-- Identify artists who generate high revenue with relatively few tracks.

with ArtistTrackRevenue as (
    select 
        ar.ArtistId,
        ar.Name as ArtistName,
        count(distinct t.TrackId) as TrackCount,
        sum(il.UnitPrice * il.Quantity) as TotalRevenue
    from InvoiceLine il
    join Track t on il.TrackId = t.TrackId
    join Album al on t.AlbumId = al.AlbumId
    join Artist ar on al.ArtistId = ar.ArtistId
    group by ar.ArtistId, ar.Name
)
select 
    ArtistName,
    TrackCount,
    TotalRevenue,
    round(1.0 * TotalRevenue / TrackCount, 2) as RevenuePerTrack
from ArtistTrackRevenue
where TrackCount >= 1
order by RevenuePerTrack desc;



---------------------------------------------------------


-- Scenario: Revenue Distribution within Artist Catalog
-- Measure what percentage of total artist revenue comes from their top tracks.

with TrackRevenue as (
    select 
        ar.ArtistId,
        ar.Name as ArtistName,
        t.TrackId,
        t.Name as TrackName,
        sum(il.UnitPrice * il.Quantity) as TrackRevenue
    from InvoiceLine il
    join Track t on il.TrackId = t.TrackId
    join Album al on t.AlbumId = al.AlbumId
    join Artist ar on al.ArtistId = ar.ArtistId
    group by ar.ArtistId, ar.Name, t.TrackId, t.Name
),
TrackRevenueWithRank as (
    select *,
        sum(TrackRevenue) over (partition by ArtistId) as ArtistTotalRevenue,
        sum(TrackRevenue) over (partition by ArtistId order by TrackRevenue desc rows between unbounded preceding and current row) as CumulativeRevenue
    from TrackRevenue
),
RevenueDistribution as (
    select 
        ArtistName,
        TrackName,
        TrackRevenue,
        ArtistTotalRevenue,
        round(100.0 * CumulativeRevenue / ArtistTotalRevenue, 2) as CumulativePercentage
    from TrackRevenueWithRank
)
select *
from RevenueDistribution
where CumulativePercentage <= 80 
order by ArtistName, CumulativePercentage;

---------------------------------------------------------------------------------------------
-- Scenario: Artist Revenue Change - First Half vs Second Half
-- Compare artist revenue between the first and second half of the year to detect growth or decline.

with ArtistHalfYearRevenue as (
    select 
        ar.ArtistId,
        ar.Name as ArtistName,
        case 
            when month(i.InvoiceDate) <= 6 then 'H1'
            else 'H2'
        end as HalfYear,
        sum(il.UnitPrice * il.Quantity) as Revenue
    from Invoice i
    join InvoiceLine il on i.InvoiceId = il.InvoiceId
    join Track t on il.TrackId = t.TrackId
    join Album al on t.AlbumId = al.AlbumId
    join Artist ar on al.ArtistId = ar.ArtistId
    group by ar.ArtistId, ar.Name, case when month(i.InvoiceDate) <= 6 then 'H1' else 'H2' end
),
Pivoted as (
    select 
        ArtistName,
        max(case when HalfYear = 'H1' then Revenue else 0 end) as Revenue_H1,
        max(case when HalfYear = 'H2' then Revenue else 0 end) as Revenue_H2
    from ArtistHalfYearRevenue
    group by ArtistName
)
select *,
    case 
        when Revenue_H2 > Revenue_H1 then 'Increased'
        when Revenue_H2 < Revenue_H1 then 'Decreased'
        else 'No Change'
    end as Trend
from Pivoted
order by Revenue_H2 - Revenue_H1 desc;




-------------------------------------------------------------------
-- Scenario: Potential Revenue Loss Due to Incomplete Invoice Data
-- Identify invoice lines that have NULLs in critical revenue fields

select 
    il.InvoiceLineId,
    il.InvoiceId,
    il.TrackId,
    t.Name as TrackName,
    il.UnitPrice,
    il.Quantity,
    case 
        when il.TrackId is null then 'Missing Track'
        when il.UnitPrice is null then 'Missing Price'
        when il.Quantity is null then 'Missing Quantity'
        else 'OK'
    end as RevenueImpactType
from InvoiceLine il
left join Track t on il.TrackId = t.TrackId
where il.TrackId is null or il.UnitPrice is null or il.Quantity is null;


-------------------------------------------------------
-- Scenario: Revenue from Tracks with Missing Genre or Album Info

select 
    case 
        when t.GenreId is null and t.AlbumId is null then 'Missing Genre & Album'
        when t.GenreId is null then 'Missing Genre'
        when t.AlbumId is null then 'Missing Album'
        else 'Complete'
    end as MetadataStatus,
    sum(il.UnitPrice * il.Quantity) as TotalRevenue
from InvoiceLine il
join Track t on il.TrackId = t.TrackId
group by 
    case 
        when t.GenreId is null and t.AlbumId is null then 'Missing Genre & Album'
        when t.GenreId is null then 'Missing Genre' 
        when t.AlbumId is null then 'Missing Album'
        else 'Complete'
    end;




    --------------------------------------------------------------------------------------
    -- Scenario: Missing Artist/Album References Affecting Revenue Attribution

select 
    case 
        when al.AlbumId is null then 'Missing Album'
        when ar.ArtistId is null then 'Missing Artist'
        else 'Complete'
    end as AttributionStatus,
    sum(il.UnitPrice * il.Quantity) as Revenue
from InvoiceLine il
left join Track t on il.TrackId = t.TrackId
left join Album al on t.AlbumId = al.AlbumId
left join Artist ar on al.ArtistId = ar.ArtistId
group by 
    case 
        when al.AlbumId is null then 'Missing Album'
        when ar.ArtistId is null then 'Missing Artist'
        else 'Complete'
    end;

