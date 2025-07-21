use Chinook;

-- Scenario: Inferred Composer Prediction
-- For albums where the majority of tracks have the same composer,
-- identify tracks missing the composer value and suggest the most frequent one from the same album.
-- This is useful for data enrichment or auto-fix suggestion pipelines.

WITH AlbumComposerStats AS (
    SELECT 
        AlbumId,
        Composer,
        COUNT(*) AS Count
    FROM Track
    WHERE Composer IS NOT NULL
    GROUP BY AlbumId, Composer
),
MostCommonComposer AS (
    SELECT 
        AlbumId,
        Composer,
        Count,
        RANK() OVER (PARTITION BY AlbumId ORDER BY Count DESC) AS rnk
    FROM AlbumComposerStats
),
CandidateFixes AS (
    SELECT 
        t.TrackId,
        t.Name AS TrackName,
        mcc.Composer AS SuggestedComposer
    FROM Track t
    JOIN MostCommonComposer mcc 
      ON t.AlbumId = mcc.AlbumId 
     AND mcc.rnk = 1
    WHERE t.Composer IS NULL
)
SELECT * FROM CandidateFixes;

-------------------------------------------------------------------------
-- Scenario : Composer Null Anomaly in Classical Genre
-- Identify artists in the 'Classical' genre whose tracks are missing composer information.
-- Classical music typically requires composer attribution; missing data here may indicate a metadata issue.

SELECT 
    ar.Name AS ArtistName,
    COUNT(*) AS MissingComposerCount
FROM Track t
JOIN Album al ON t.AlbumId = al.AlbumId
JOIN Artist ar ON al.ArtistId = ar.ArtistId
JOIN Genre g ON t.GenreId = g.GenreId
WHERE g.Name = 'Classical' AND t.Composer IS NULL
GROUP BY ar.Name
ORDER BY MissingComposerCount DESC;

-------------------------------------------------------------------------

-- Scenario : Data Quality Metric Table for Composer Completeness (SQL Server version)
-- Generates a snapshot of missing composer data quality for daily/weekly monitoring in BI tools or ETL checks.

SELECT 
    CONVERT(DATE, GETDATE()) AS SnapshotDate,
    COUNT(*) AS TotalTracks,
    SUM(CASE WHEN Composer IS NULL THEN 1 ELSE 0 END) AS MissingComposer,
    ROUND(100.0 * SUM(CASE WHEN Composer IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS MissingPercentage
FROM Track;



----------------------------------------------------------------------------

-- Scenario: Staging View with Composer Enrichment

-- Cleaned view for downstream use: composer filled from most common composer in the same album.

WITH AlbumComposerStats AS (
    SELECT 
        AlbumId,
        Composer,
        COUNT(*) AS ComposerCount,
        RANK() OVER (PARTITION BY AlbumId ORDER BY COUNT(*) DESC) AS rnk
    FROM Track
    WHERE Composer IS NOT NULL
    GROUP BY AlbumId, Composer
),
MostLikelyComposer AS (
    SELECT AlbumId, Composer AS InferredComposer
    FROM AlbumComposerStats
    WHERE rnk = 1
)
SELECT 
    t.TrackId,
    t.Name,
    ISNULL(t.Composer, mlc.InferredComposer) AS FinalComposer,
    t.AlbumId,
    t.GenreId,
    t.Milliseconds,
    t.UnitPrice
FROM Track t
LEFT JOIN MostLikelyComposer mlc ON t.AlbumId = mlc.AlbumId;

---------------------------------------------------------------------

-- Scenario: Revenue Impact of Missing Composer
-- Determine if incomplete metadata affects high-revenue items (a prioritization insight).

SELECT 
    CASE WHEN t.Composer IS NULL THEN 'Missing Composer' ELSE 'Has Composer' END AS ComposerStatus,
    SUM(il.UnitPrice * il.Quantity) AS Revenue
FROM InvoiceLine il
JOIN Track t ON il.TrackId = t.TrackId
GROUP BY CASE WHEN t.Composer IS NULL THEN 'Missing Composer' ELSE 'Has Composer' END;



---------------------------------------------------------------------
-- Scenario: Enrich Missing Composer Data During ETL Load  
-- In this ETL simulation, tracks missing a composer are enriched using the most frequently occurring composer from the same album.  
-- The cleaned data is then inserted into a staging table for downstream use in analytics or reporting.
 


WITH AlbumComposerStats AS (
    SELECT 
        AlbumId,
        Composer,
        COUNT(*) AS Count,
        RANK() OVER (PARTITION BY AlbumId ORDER BY COUNT(*) DESC) AS rnk
    FROM Track
    WHERE Composer IS NOT NULL
    GROUP BY AlbumId, Composer
),
MostCommonComposer AS (
    SELECT AlbumId, Composer AS InferredComposer
    FROM AlbumComposerStats
    WHERE rnk = 1
)
INSERT INTO Staging_Track_Cleaned (TrackId, Name, FinalComposer, AlbumId, GenreId, Milliseconds, UnitPrice)
SELECT 
    t.TrackId,
    t.Name,
    ISNULL(t.Composer, mcc.InferredComposer) AS FinalComposer,
    t.AlbumId,
    t.GenreId,
    t.Milliseconds,
    t.UnitPrice
FROM Track t
LEFT JOIN MostCommonComposer mcc ON t.AlbumId = mcc.AlbumId;

---------------------------------------------------------------------------------------------------

with AlbumComposerStats as (
    select 
        AlbumId,
        Composer,
        count(*) as ComposerCount,
        rank() over (partition by AlbumId order by count(*) desc) as rnk
    from Track
    where Composer is not null
    group by AlbumId, Composer
),
MostLikelyComposer as (
    select AlbumId, Composer as InferredComposer
    from AlbumComposerStats
    where rnk = 1
)
insert into Staging_Track_Cleaned (TrackId, Name, FinalComposer, AlbumId, GenreId, Milliseconds, UnitPrice)
select 
    t.TrackId,
    t.Name,
    isnull(t.Composer, mlc.InferredComposer) as FinalComposer,
    t.AlbumId,
    t.GenreId,
    t.Milliseconds,
    t.UnitPrice
from Track t
left join MostLikelyComposer mlc on t.AlbumId = mlc.AlbumId;


--------------------------------------------------------------------------------------------
-- Scenario: Album-Based Composer Consistency Audit
-- Identify albums that contain multiple distinct composers.
-- High variation in composer values within the same album may indicate a metadata inconsistency.

select 
    a.AlbumId,
    a.Title as AlbumTitle,
    ar.Name as ArtistName,
    count(distinct t.Composer) as DistinctComposerCount,
    case 
        when count(distinct t.Composer) = 1 then 'Highly Consistent'
        when count(distinct t.Composer) = 2 then 'Moderately Consistent'
        else 'Low Consistency'
    end as ConsistencyLevel
from Album a
join Track t on a.AlbumId = t.AlbumId
join Artist ar on a.ArtistId = ar.ArtistId
where t.Composer is not null
group by a.AlbumId, a.Title, ar.Name
order by DistinctComposerCount desc;

-------------------------------------------------------------------------------------------------------
-- Scenario: Composer Null Distribution by Track Length
-- Analyze how often composer data is missing across different track length categories.
-- Shorter tracks may have less metadata coverage due to their nature (e.g., intros, jingles).

with TrackLengthCategorized as (
    select 
        TrackId,
        Name,
        Composer,
        case 
            when Milliseconds < 120000 then 'Short'
            when Milliseconds between 120000 and 300000 then 'Medium'
            else 'Long'
        end as LengthCategory
    from Track
)
select 
    LengthCategory,
    count(*) as TotalTracks,
    sum(case when Composer is null then 1 else 0 end) as MissingComposerCount,
    round(100.0 * sum(case when Composer is null then 1 else 0 end) / count(*), 2) as MissingPercentage
from TrackLengthCategorized
group by LengthCategory
order by LengthCategory;
