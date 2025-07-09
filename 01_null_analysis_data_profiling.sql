use Chinook;

-- Create data quality summary table: total customers, number of missing Company and State, rates and estimated missing
create table MissingValuesCustomer(
    TotalCustomerCount int,
    MissingCompanyNumber int,
    MissingCompanyRatio float,
    UnknownStateNumber int,
    UnknownStateRatio float,
    PredictableStateNumber int,
    PredictableStateRatio float,
    
);

------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
-- Add the results of the missing data analysis to the Missing Values ​​Customer table
insert into  MissingValuesCustomer(TotalCustomerCount,MissingCompanyNumber,MissingCompanyRatio,UnknownStateNumber,UnknownStateRatio,PredictableStateNumber,PredictableStateRatio)
select 
    count(CustomerId),
    count(case when c.Company is null then 1 end),
    round(count(case when c.Company is null then 1 end) * 1.0 / count(CustomerId), 3),
    count(case when c.State is null then 1 end),
    round(count(case when c.State is null then 1 end) * 1.0 / count(CustomerId), 3),
    count(case when c.Country is not null and c.City is not null and c.State is null then 1 end),
    round(count(case when c.Country is not null and c.City is not null and c.State is null then 1 end) * 1.0 / count(CustomerId), 3)
from Customer c;
select * from MissingValuesCustomer

select * from customer;
------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
-- Number of customers by country, missing Company/State numbers and normalized MissingScore calculation
select c.Country,
       count(c.CustomerId)TotalCustomer,
       count(case when c.Company is null then 1 end)MissingCompanyName,
       count(case when c.State is null then 1 end)MissingCompanyState,
      MissingScore =  round(( count(case when c.Company is null then 1 end) + count(case when c.State is null then 1 end))*1.0 / (2*count(c.CustomerId)),2 )

from Customer c
group by Country
order by MissingScore desc,Country



------------------------------------------------------------------------------------------

-- Creating a table for customer records that can be corrected with the forecast

CREATE TABLE FixCandidateCustomers (
    CustomerId INT,
    FirstName NVARCHAR(100),
    LastName NVARCHAR(100),
    Company NVARCHAR(100),
    City NVARCHAR(100),
    Country NVARCHAR(100),
    State NVARCHAR(100)  
);


INSERT INTO FixCandidateCustomers (CustomerId, FirstName, LastName, Company, City, Country, State)
SELECT 
    CustomerId,
    FirstName,
    LastName,
    Company,
    City,
    Country,
    State
FROM Customer
WHERE State IS NULL
  AND City IS NOT NULL
  AND Country IS NOT NULL;
SELECT * FROM FixCandidateCustomers;


------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
-- insert into countryMissingRisk with calculated scores and labels 
------------------------------------------------------------------------------------------
CREATE TABLE countryMissingRisk (
    country NVARCHAR(100) PRIMARY KEY,
    totalCustomer INT,
    missingCompanyNumber INT,
    missingStateNumber INT,
    missingScore FLOAT,
    riskLevel VARCHAR(20)
);


INSERT INTO countryMissingRisk (
    country,
    totalCustomer,
    missingCompanyNumber,
    missingStateNumber,
    missingScore,
    riskLevel
)
select 
    c.country,
    count(c.customerId) as totalCustomer,
    count(case when c.company is null then 1 end) as missingCompanyNumber,
    count(case when c.state is null then 1 end) as missingStateNumber,
    round(
        (count(case when c.company is null then 1 end) + count(case when c.state is null then 1 end)) * 1.0 / (2 * count(c.customerId)),
        2
    ) as missingScore,
    case 
        when round((count(case when c.company is null then 1 end) + count(case when c.state is null then 1 end)) * 1.0 / (2 * count(c.customerId)), 2) >= 0.5 then 'high'
        when round((count(case when c.company is null then 1 end) + count(case when c.state is null then 1 end)) * 1.0 / (2 * count(c.customerId)), 2) >= 0.2 then 'medium'
        else 'low'
    end as riskLevel
from customer c
group by c.country
order by missingScore desc, c.country;




select * from  countryMissingRisk
