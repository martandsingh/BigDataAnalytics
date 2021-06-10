-- #####################################################################################;
-- AUTHOR: Martand Singh;
-- DATE: 07-June-2021;
-- SCOPE: WE WILL TRY TO FIND OUT MIN AND MAX PRICE OF BTC OVER LAST 5-6 YEARS.;
-- DATASOURCE: https://finance.yahoo.com/quote/BTC-USD/history?period1=1410912000&period2=1623024000&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true  ;
-- DATA REPO: https://github.com/martandsingh/BigDataAnalytics/tree/main/dataset/trading/BTC-USD.csv  ;
-- GOAL 1: calculate minimum price of a month for the year, sort it out and export it to local file system;
-- GOAL 2: calculate maximum price of a month for the year, sort it out and export it to local file system;
-- GOAL 3: calculate difference between min & max price of a month for the year, sort it out and export it to local file system;
-- FOLLOW US: https://www.facebook.com/codemakerz ;
-- LINKEDIN: https://www.linkedin.com/in/martandsays/ ;
-- CONTACT US IF YOU ARE INTERESTED TO PARTICIPATE.;
-- #####################################################################################;


-- STEP 0: DEFINED VARIABLES;
SET hivevar:localDataFilePath= '/home/cloudera/dataset/BTC-USD.csv';
SET hivevar:hdfsDataFileLocation = '/user/cloudera/msingh/btcusd';
SET hivevar:minFileName = '/home/cloudera/btcusd_output/minPrice';
SET hivevar:maxFileName = '/home/cloudera/btcusd_output/maxPrice';
SET hivevar:minMaxFileName = '/home/cloudera/btcusd_output/minMaxPrice';

SELECT ${localDataFilePath};
SELECT ${hdfsDataFileLocation};
SELECT ${minFileName};
SELECT ${maxFileName};
SELECT ${minMaxFileName};

-- STEP1: DOWNLOAD DATASET TO YOUR LOCAL SYSTEM AND COPY IT TO HDFS.;
-- hadoop fs -put ${localDataFilePath} ${hdfsDataFileLocation}  ;

-- STEP2: CREATE DATABASE cryptoanalysisBTC;

!echo "Creating database...";
create database if not exists cryptoanalysisBTC
comment "BTC-USD analysis database"
with DBPROPERTIES(
'Date'='07-June-2021',
'Author'='Martand Singh');

!echo "Database created.";
-- STEP3: SET ABOVE CREATED DATABASE AS CURRENT DATABASE;
USE cryptoanalysisBTC;

--STEP4: CREATE EXTERNAL TABLE WITH EXACT SAME COLUMN AS CSV. SKIPPING FIRST LINE TO AVOID HEADER ;

!echo "Creating table btc_analysis...";
create external table btc_price_analysis
(
pricedate date,
open float,
high float,
low float,
close float,
adj_close float,
volume bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

!echo "Table btc_analysis created.";

--STEP5: LOAD DATA FROM HDFS LOCATION DEFINED IN STEP 1 TO HIVE TABLE. ;

!echo "Loading data into btc_analysis...";
LOAD DATA INPATH ${hdfsDataFileLocation} OVERWRITE INTO TABLE btc_price_analysis;
!echo "Data loaded to btc_analysis.";

--STEP6: CREATE A MANAGED TABLE USING btc_analysis TABLE CREATED IN STEP 4. BUT THIS TIME WE WILL ADD NEW COLUMNS YEAR &  MONTH;
-- FOR EXTRACTING YEAR & MONTH WE WILL USE MONTH() YEAR() FUNCTIONS.;

!echo "Creating managed table btc_date...";

create  table btc_month_week_aggregate
row format delimited
fields terminated by ','
stored as textfile
AS
select pricedate,month(pricedate) as month,year(pricedate)
as year, date_format(pricedate, "W") as week_of_month,low,high,open,close,volume from btc_price_analysis;

create  table btc_week_day_aggregate2
row format delimited
fields terminated by ','
stored as textfile
AS
select pricedate,month(pricedate) as month,year(pricedate)
as year, date_format(pricedate, "W") as week_of_month, day(pricedate) as day, low, high, open, close, volume from btc_price_analysis
where day(pricedate) in ('1','7','14','21', 28 );

!echo "Table btc_date created.";


--STEP7: (OPTIONAL) ENABLE THIS TO PRINT COLUMN NAMES IN SELECT QUERY OUTPUT;
set hive.cli.print.header=true;


--STEP8: GOAL1: WE WILL GROUP BY TABLE WITH YEAR & MONTH AND CALCULATE THE MINIMUM PRICE FOR THE RESPECTIVE MONTH & YEAR.;
--OUR QUERY OUTPUT WILL BE SAVED AT THE GIVEN LOCAL FILE SYSTEM LOCATION ;

!echo "Exporting minimum price data to local storage...";

INSERT OVERWRITE LOCAL DIRECTORY ${minFileName}
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT a.year as year, a.month as month, week_of_month, a.lowest as lowest_price FROM 
( select month, year, date_format(pricedate, "W") as week_of_month, min(low) as lowest 
from btc_date_trans group by year,month ) AS a order by year,month, week_of_month;

!echo "Minimum price analysis exported.";

--STEP9: GOAL3: WE WILL GROUP BY TABLE WITH YEAR & MONTH AND CALCULATE THE MAXIMUM PRICE FOR THE RESPECTIVE MONTH & YEAR.;
--OUR QUERY OUTPUT WILL BE SAVED AT THE GIVEN LOCAL FILE SYSTEM LOCATION;

!echo "Exporting maximum price data to local storage...";

INSERT OVERWRITE LOCAL DIRECTORY ${maxFileName}
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT a.year as year, a.month as month, a.highest as highest_price FROM 
( select month, year, max(high) as highest 
from btc_date_trans group by year,month ) AS a order by year,month;

!echo "Maximum price analysis exported.";

--STEP10: GOAL3: WE WILL GROUP BY TABLE WITH YEAR & MONTH AND CALCULATE THE DiFFERENCE BETWEEN MINIMUM & MAXIMUM PRICE FOR THE RESPECTIVE MONTH & YEAR.;
--OUR QUERY OUTPUT WILL BE SAVED AT THE GIVEN LOCAL FILE SYSTEM LOCATION;

!echo "Exporting min-max difference data to local storage...";

INSERT OVERWRITE LOCAL DIRECTORY ${minMaxFileName}
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT a.year as year, a.month as month, a.lowest as lowest_price, a.highest as highest_price, a.minmaxdiff as min_max_diff FROM 
( select month, year, min(low) as lowest, max(high) as highest, max(high)-min(low) as minmaxdiff 
from btc_date_trans group by year,month ) AS a order by min_max_diff;

!echo "Exported min-max analysis data.";
!echo "Finished...";

-- FINISH. You can check you given local system path. there will be text files containing the result.;
-- THANK YOU.;