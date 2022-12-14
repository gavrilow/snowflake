use role sysadmin;
create database DEMO_DB;
create warehouse COMPUTE_WH;


alter user havrilov set default_role = 'SYSADMIN';
alter user havrilov set default_warehouse = 'COMPUTE_WH';
alter user havrilov set default_namespace = 'DEMO_DB.PUBLIC';

/*
alter user KISHOREK set default_role = 'SYSADMIN';
alter user KISHOREK set default_warehouse = 'COMPUTE_WH';
alter user KISHOREK set default_namespace = 'DEMO_DB.PUBLIC';
*/

use role accountadmin;

create or replace api integration dora_api_integration
api_provider = aws_api_gateway
api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole'
enabled = true
api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');

  
create or replace external function demo_db.public.grader(
      step varchar
    , passed boolean
    , actual integer
    , expected integer
    , description varchar)
returns variant
api_integration = dora_api_integration 
context_headers = (current_timestamp,current_account, current_statement) 
as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader'; 

select demo_db.public.grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123 ) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 


select current_account();


ALTER USER havrilov SET DEFAULT_ROLE = 'SYSADMIN';


use role sysadmin;
create database AGS_GAME_AUDIENCE;
drop schema public;
create schema raw;

--drop table AGS_GAME_AUDIENCE.RAW.GAME_LOGS;
--drop stage uni_kishore;
--drop file format FF_JSON_LOGS;

create or replace TABLE AGS_GAME_AUDIENCE.RAW.GAME_LOGS (
	RAW_LOG VARIANT
);

create or replace stage uni_kishore
    url = 's3://uni-kishore'
    -- credentials = (aws_secret_key = '<key>' aws_key_id = '<id>')
    ;


list @uni_kishore/kickoff;

create file format FF_JSON_LOGS
    type = JSON
    strip_outer_array = true;

select $1
from @uni_kishore/kickoff
(file_format => ff_json_logs);


copy into raw.game_logs
from @uni_kishore/kickoff
file_format = (format_name = ff_json_logs);

select raw_log:agent::text as agent,
raw_log:datetime_iso8601::timestamp_ntz as datetime,
raw_log:user_event::text as user_event,
raw_log:user_login::text as user_login,
*
from game_logs;

create or replace view logs as
select raw_log:agent::text as agent,
raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601,
raw_log:user_event::text as user_event,
raw_log:user_login::text as user_login,
*
from game_logs;

select * from logs;


use role accountadmin;
use database DEMO_DB;
use schema public;
use warehouse COMPUTE_WH;

-- DO NOT EDIT THIS CODE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DNGW01' as step
  ,(
      select count(*)  
      from ags_game_audience.raw.logs
      where is_timestamp_ntz(to_variant(datetime_iso8601))= TRUE 
   ) as actual
, 250 as expected
, 'Project DB and Log File Set Up Correctly' as description
); 


--what time zone is your account(and/or session) currently set to? Is it -0700?
select current_timestamp();

alter session set timezone = 'Europe/Sofia';
select current_timestamp();

--worksheets are sometimes called sessions -- we'll be changing the worksheet time zone
alter session set timezone = 'UTC';
select current_timestamp();

--how did the time differ after changing the time zone for the worksheet?
alter session set timezone = 'Africa/Nairobi';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

--show the account parameter called timezone
show parameters like 'timezone';
