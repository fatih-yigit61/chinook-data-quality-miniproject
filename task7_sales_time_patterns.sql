use Chinook;


-- scenario: hourly sales heatmap
-- description: shows the volume of sales by hour of the day.
-- insight: identifies high-traffic sales hours for potential targeted campaigns.

select 
    datepart(hour, i.InvoiceDate) as HourOfDay,
    count(*) as InvoiceCount,
    round(sum(il.UnitPrice * il.Quantity), 2) as Revenue
from Invoice i
join InvoiceLine il on i.InvoiceId = il.InvoiceId
group by datepart(hour, i.InvoiceDate)
order by HourOfDay;


-- scenario: day of week sales distribution
-- description: analyzes sales distribution across weekdays.
-- insight: helps understand which days customers are most active.

select 
    datename(weekday, i.InvoiceDate) as DayOfWeek,
    count(*) as InvoiceCount,
    sum(il.UnitPrice * il.Quantity) as TotalRevenue
from Invoice i
join InvoiceLine il on i.InvoiceId = il.InvoiceId
group by datename(weekday, i.InvoiceDate)
order by InvoiceCount desc;



-- scenario: rolling 3-month revenue trend
-- description: computes moving average of revenue over trailing 3 months.
-- insight: smooths short-term volatility and highlights consistent trends.

with MonthlyRevenue as (
    select 
        format(i.InvoiceDate, 'yyyy-MM') as RevenueMonth,
        datefromparts(year(i.InvoiceDate), month(i.InvoiceDate), 1) as RevenueDate,
        sum(il.UnitPrice * il.Quantity) as Revenue
    from Invoice i
    join InvoiceLine il on i.InvoiceId = il.InvoiceId
    group by format(i.InvoiceDate, 'yyyy-MM'), datefromparts(year(i.InvoiceDate), month(i.InvoiceDate), 1)
),
RollingTrend as (
    select 
        RevenueMonth,
        Revenue,
        avg(Revenue) over (order by RevenueDate rows between 2 preceding and current row) as Rolling3MonthAvg
    from MonthlyRevenue
)
select *
from RollingTrend
order by RevenueMonth;





-- scenario: revenue drop detection
-- description: detect months where revenue dropped more than 30% compared to the previous month.
-- insight: flags months with possible operational or market issues.

with MonthlyRevenue as (
    select 
        format(i.InvoiceDate, 'yyyy-MM') as RevenueMonth,
        sum(il.UnitPrice * il.Quantity) as Revenue
    from Invoice i
    join InvoiceLine il on i.InvoiceId = il.InvoiceId
    group by format(i.InvoiceDate, 'yyyy-MM')
),
Lagged as (
    select 
        RevenueMonth,
        Revenue,
        lag(Revenue) over (order by RevenueMonth) as PrevMonthRevenue
    from MonthlyRevenue
)
select *,
    round(100.0 * (Revenue - PrevMonthRevenue) / nullif(PrevMonthRevenue, 0), 2) as RevenueChangePct
from Lagged
where PrevMonthRevenue is not null
  and Revenue < 0.7 * PrevMonthRevenue
order by RevenueMonth;




-- scenario: peak hour contribution to daily revenue
-- description: determine how much revenue comes from top-performing hour of each day.
-- insight: shows concentration of revenue in short time windows.

with HourlyRevenue as (
    select 
        convert(date, i.InvoiceDate) as InvoiceDay,
        datepart(hour, i.InvoiceDate) as HourOfDay,
        sum(il.UnitPrice * il.Quantity) as HourlyTotal
    from Invoice i
    join InvoiceLine il on i.InvoiceId = il.InvoiceId
    group by convert(date, i.InvoiceDate), datepart(hour, i.InvoiceDate)
),
Ranked as (
    select *,
        sum(HourlyTotal) over (partition by InvoiceDay) as DailyTotal,
        rank() over (partition by InvoiceDay order by HourlyTotal desc) as HourRank
    from HourlyRevenue
)
select 
    InvoiceDay,
    HourOfDay,
    HourlyTotal,
    DailyTotal,
    round(100.0 * HourlyTotal / nullif(DailyTotal, 0), 2) as PeakHourPercentage
from Ranked
where HourRank = 1
order by InvoiceDay;




-- scenario: weekend vs weekday revenue split
-- description: compare total revenue generated on weekends vs weekdays.
-- insight: helps tailor marketing or campaigns toward high-performing days.

select 
    case 
        when datename(weekday, i.InvoiceDate) in ('Saturday', 'Sunday') then 'Weekend'
        else 'Weekday'
    end as DayType,
    count(distinct i.InvoiceId) as InvoiceCount,
    round(sum(il.UnitPrice * il.Quantity), 2) as TotalRevenue
from Invoice i
join InvoiceLine il on i.InvoiceId = il.InvoiceId
group by 
    case 
        when datename(weekday, i.InvoiceDate) in ('Saturday', 'Sunday') then 'Weekend'
        else 'Weekday'
    end;


    -- scenario: customer inactivity detection
-- description: measure how long each customer goes without making a purchase.
-- insight: identifies churn risk or dormant customers.

with InvoiceDates as (
    select 
        CustomerId,
        InvoiceDate,
        lag(InvoiceDate) over (partition by CustomerId order by InvoiceDate) as PrevInvoice
    from Invoice
),
Gaps as (
    select 
        CustomerId,
        InvoiceDate,
        PrevInvoice,
        datediff(day, PrevInvoice, InvoiceDate) as DaysInactive
    from InvoiceDates
    where PrevInvoice is not null
)
select *
from Gaps
where DaysInactive >= 60
order by DaysInactive desc;






-- scenario: revenue per invoice time window
-- description: bucket invoices into morning, afternoon, evening, night, and compare revenues.
-- insight: determines which time window contributes most to total sales.

select 
    case 
        when datepart(hour, i.InvoiceDate) between 6 and 11 then 'morning'
        when datepart(hour, i.InvoiceDate) between 12 and 17 then 'afternoon'
        when datepart(hour, i.InvoiceDate) between 18 and 22 then 'evening'
        else 'night'
    end as TimeWindow,
    count(*) as InvoiceCount,
    round(sum(il.UnitPrice * il.Quantity), 2) as TotalRevenue
from Invoice i
join InvoiceLine il on i.InvoiceId = il.InvoiceId
group by 
    case 
        when datepart(hour, i.InvoiceDate) between 6 and 11 then 'morning'
        when datepart(hour, i.InvoiceDate) between 12 and 17 then 'afternoon'
        when datepart(hour, i.InvoiceDate) between 18 and 22 then 'evening'
        else 'night'
    end
order by TotalRevenue desc;





-- scenario: time to first purchase per customer
-- description: measure how long after sign-up (or first invoice date) customers make their first purchase.
-- insight: identifies delays in conversion or potential onboarding friction.

with FirstInvoice as (
    select 
        CustomerId,
        min(InvoiceDate) as FirstPurchaseDate
    from Invoice
    group by CustomerId
),
CustomerActivity as (
    select 
        i.CustomerId,
        i.InvoiceDate,
        fi.FirstPurchaseDate,
        datediff(day, fi.FirstPurchaseDate, i.InvoiceDate) as DaysSinceFirst
    from Invoice i
    join FirstInvoice fi on i.CustomerId = fi.CustomerId
)
select CustomerId, 
       FirstPurchaseDate, 
       count(*) as TotalPurchases, 
       max(DaysSinceFirst) as MaxDaysAfterFirst,
       min(DaysSinceFirst) as MinDaysAfterFirst
from CustomerActivity
group by CustomerId, FirstPurchaseDate
order by MaxDaysAfterFirst desc;




-- scenario: sales volatility index per month
-- description: measure standard deviation of daily revenue per month to quantify sales volatility.
-- insight: identifies unstable revenue periods and helps in forecasting or capacity planning.

with DailyRevenue as (
    select 
        cast(i.InvoiceDate as date) as InvoiceDay,
        format(i.InvoiceDate, 'yyyy-MM') as RevenueMonth,
        sum(il.UnitPrice * il.Quantity) as DailyTotal
    from Invoice i
    join InvoiceLine il on i.InvoiceId = il.InvoiceId
    group by cast(i.InvoiceDate as date), format(i.InvoiceDate, 'yyyy-MM')
),
Volatility as (
    select 
        RevenueMonth,
        count(*) as ActiveDays,
        round(avg(DailyTotal), 2) as AvgDailyRevenue,
        round(stdevp(DailyTotal), 2) as StdDevRevenue
    from DailyRevenue
    group by RevenueMonth
)
select 
    RevenueMonth,
    ActiveDays,
    AvgDailyRevenue,
    StdDevRevenue,
    round(100.0 * StdDevRevenue / nullif(AvgDailyRevenue, 0), 2) as VolatilityIndexPct
from Volatility
order by VolatilityIndexPct desc;




-- scenario: cumulative revenue by week number
-- description: calculates how revenue accumulates throughout the year on a weekly basis.
-- insight: visualizes seasonal growth patterns and helps forecast cumulative performance targets.

with WeeklyRevenue as (
    select 
        year(i.InvoiceDate) as SalesYear,
        datepart(week, i.InvoiceDate) as WeekNumber,
        sum(il.UnitPrice * il.Quantity) as WeeklyTotal
    from Invoice i
    join InvoiceLine il on i.InvoiceId = il.InvoiceId
    group by year(i.InvoiceDate), datepart(week, i.InvoiceDate)
),
Cumulative as (
    select 
        SalesYear,
        WeekNumber,
        WeeklyTotal,
        sum(WeeklyTotal) over (
            partition by SalesYear 
            order by WeekNumber
            rows between unbounded preceding and current row
        ) as CumulativeRevenue
    from WeeklyRevenue
)
select *
from Cumulative
order by SalesYear, WeekNumber;
