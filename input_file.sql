with

time_select (Year, Period) as (
  -- helper table
  -- specifies a specific Year, Period

  -- This query will get the previous period and its 
  -- corresponding year, which then will be used to 
  -- correctly retrieve the corresponding data.
  -- For example, if this query is run on 2018-03-01, 
  -- the period will be 02 and the year will be 2018.
  -- Another example, if the query is run on 2019-01-01, 
  -- the period will be 12 and the year will be 2018.
  select YEAR( CURRENT_DATE - DAY(CURRENT_DATE) DAYS ),
    MONTH( CURRENT_DATE - DAY(CURRENT_DATE) DAYS )
    FROM SYSIBM.SYSDUMMY1
),

summary as (
-- get the SCSummary data we need
-- limit to Year, Period in helper table time_select
-- trim variable length strings
select
  LoadTrackingUID,
  Year,
  Period,
  RTRIM(AccountCode) as AccountCode,
  UsageStartDate,
  UsageEndDate,
  RTRIM(RateCode) as RateCode,
  ResourceUnits,
  RateValue,
  MoneyValue,
  StartDate,
  EndDate,
  RTRIM(RateTable) as RateTable

from SCSummary
where (Year, Period) in (select Year, Period from time_select)
--  and AccountCode like '%TSAM2_AIX2%'
--  and AccountCode like '%ZZEDUC8_moptest1%'
--  and RateCode = 'SRVPBZYBAS'
),

detail as (
-- select the SCDetail fields we'll need
-- (selecting particular rows done later)
select
  LoadTrackingUID,
  DetailUID,
  DetailLine,
  RTRIM(AccountCode) as AccountCode,
  StartDate,
  EndDate,
  RTRIM(RateCode) as RateCode,
  ResourceUnits,
  MoneyValue,
  AccountingStartDate,
  AccountingEndDate
from SCDetail
),

combined as (
-- combine summary and detail records
-- NOTE: do not use this 'combined' for any totals
--  -- they will be duplicated, too big, and not right!
-- matches our desired summary records
-- to their exact detail records
SELECT
  summary.LoadTrackingUID as LoadTrackingUID,
  detail.LoadTrackingUID as dLoadTrackingUID,
  detail.DetailUID as DetailUID,
  detail.DetailLine as DetailLine,
  Year,
  Period,
  summary.AccountCode,
  summary.UsageStartDate,
  summary.UsageEndDate,
  detail.StartDate as dStartDate,
  detail.EndDate as dEndDate,
  summary.RateCode,
  summary.ResourceUnits,
  detail.ResourceUnits as dResourceUnits,
  summary.RateValue,
  summary.MoneyValue,
  detail.MoneyValue as dMoneyValue,
  summary.StartDate,
  summary.EndDate,
  detail.AccountingStartDate as dAccountingStartDate,
  detail.AccountingEndDate as dAccountingEndDate,
  summary.RateTable

from summary left outer join detail

on
    summary.AccountCode = detail.AccountCode
and (summary.StartDate, summary.EndDate) = (detail.AccountingStartDate, detail.AccountingEndDate)
and summary.RateCode = detail.RateCode
and DATE(summary.UsageStartDate) = DATE(detail.StartDate)
and DATE(summary.UsageEndDate) = DATE(detail.EndDate)

order by
  Year, Period,
  AccountCode,
  UsageStartDate, UsageEndDate,
  dStartDate, dEndDate,
  RateCode,
  StartDate, EndDate,
  RateTable
),

detailLine as
(
-- get the highest DetailLine number
-- for all the combined records
-- by Year, Period, AccountCode
-- detail by DetailUID, but we also should assume only one DetailUID?
select
  Year, Period,
  AccountCode,
  DetailUID,
  MAX(DetailLine) as DetailLine
from combined
group by
  Year, Period,
  AccountCode,
  DetailUID
),

aggSummaryRate as
(
-- summarize individual rate codes for each AccountCode
-- from the summary data
-- sum of ResourceUnits and MoneyValue
-- for entire Year/Period
-- max UsageEndDate
select
  Year, Period,
  AccountCode,
  RateCode,
  RateTable,
  SUM(ResourceUnits) as ResourceUnits,
  SUM(MoneyValue) as MoneyValue,
  AVG(RateValue) as RateValue
from summary
group by
  Year, Period,
  AccountCode,
  RateCode,
  RateTable
order by
  Year, Period,
  AccountCode,
  RateCode,
  RateTable
),

aggSummaryDate as
(
-- for each Year/Period/AccountCode,
-- get the min/max usage dates
select
  Year, Period,
  AccountCode,
  MIN(UsageStartDate) as UsageStartDate,
  MAX(UsageEndDate) as UsageEndDate
from summary
group by
  Year, Period,
  AccountCode
order by
  Year, Period,
  AccountCode
),

pivotSummaryRate as (
-- pivot the Rates information
-- giving numRUs and (comma-delimited) RUs
-- formatting:
--   RateValue w/6 decimal places
--   MoneyValue w/2 decimal places
select
  Year, Period,
  AccountCode,
  count(*) as numRUs,

  STRIP(
    LISTAGG(RateCode CONCAT ',' CONCAT
        ResourceUnits CONCAT ',' CONCAT
        CAST(RateValue as DECIMAL(18,8)) CONCAT ',' CONCAT
        CAST(MoneyValue as DECIMAL(18,2)) CONCAT ','),
    TRAILING,
    ',')
  as RUs
from aggSummaryRate
group by
  Year, Period,
  AccountCode
order by
  Year, Period,
  AccountCode
),

pivotSummary as (
-- combine pivoted RUs and min/max dates
select
    r.Year, r.Period,
    r.AccountCode,
    UsageStartDate, UsageEndDate,
    numRUs,
    RUs
from pivotSummaryRate r, aggSummaryDate d
where
   (r.Year, r.Period, r.AccountCode) = (d.Year, d.Period, d.AccountCode)
),

raw_idents as (
-- get identifier names and values
select
    di.LoadTrackingUID,
    di.DetailUID,
    di.DetailLine,
    RTRIM(i.IdentName) as IdentName,
    RTRIM(di.IdentValue) as IdentValue,
    di.IdentNumber
from CIMSDetailIdent di, CIMSIdent i
where di.IdentNumber = i.IdentNumber
),

idents as (
-- pivot the identifiers and values
-- by LoadTrackingUID, DetailUID, and DetailLine
-- quoting the values
--   excluding Account_Code identifier
select
    LoadTrackingUID,
    DetailUID,
    DetailLine,
    
    count(*) as numIdentifiers,

  STRIP(
    LISTAGG(IdentName CONCAT ',' CONCAT '"' CONCAT IdentValue CONCAT '"' CONCAT ','),
    TRAILING,
    ',' )
    as identifiers
    
from raw_idents
where IdentName not in ('ACCOUNT_CODE')
group by LoadTrackingUID, DetailUID, DetailLine
order by LoadTrackingUID, DetailUID, DetailLine
),

theData as (
-- combine the pivoted Summary and Identfier data
-- using the detailLine table to join them
-- pivotSummary -> detailLine -> idents
select
  ps.Year,
  ps.Period,
  '"' CONCAT RTRIM(ps.AccountCode) CONCAT '"' as AccountCode,
  DATE(UsageStartDate) as UsageStartDate,
  DATE(UsageEndDate) as UsageEndDate,
dl.DetailLine,
  numRUs,
  RUs,
  numIdentifiers,
  identifiers

from pivotSummary as ps, detailLine as dl, idents as i
where ps.Year = dl.Year
  and ps.Period = dl.Period
  and ps.AccountCode = dl.AccountCode
  and dl.DetailUID = i.DetailUID
  and dl.DetailLine = i.DetailLine
order by
  ps.Year, ps.Period,
  ps.AccountCode,
  UsageStartDate, UsageEndDate
)

select *
from theData
--where identifiers like '%emeastgdgzsccm%'
where identifiers like '%emeaprddgzsccm%'
;
