// Snowflake Training Program for Data Engineers #17
-- https://learn.epam.com/api/file/get?id=58356&s=2

create database EPAM_LAB_NEW;
create schema CORE_DWH;
create schema DATA_MART;
drop schema public;


use schema CORE_DWH;

create or replace table CORE_DWH.region
(
  regionkey INTEGER,
  name      CHAR(25),
  comment   VARCHAR(152)
);


create or replace table CORE_DWH.nation
(
  nationkey INTEGER not null,
  name      CHAR(27),
  regionkey INTEGER,
  comment   VARCHAR(155)
);


create or replace table CORE_DWH.supplier
(
  suppkey   INTEGER not null,
  name      CHAR(25),
  address   VARCHAR(40),
  nationkey INTEGER,
  phone     CHAR(15),
  acctbal   FLOAT8,
  comment   VARCHAR(101)
);


create or replace table CORE_DWH.orders
(
  orderkey      INTEGER not null,
  custkey       INTEGER not null,
  orderstatus   CHAR(1),
  totalprice    FLOAT8,
  orderdate     DATE,
  orderpriority CHAR(15),
  clerk         CHAR(15),
  shippriority  INTEGER,
  comment       VARCHAR(79)
);


create or replace table CORE_DWH.partsupp
(
  partkey    INTEGER not null,
  suppkey    INTEGER not null,
  availqty   INTEGER,
  supplycost FLOAT8 not null,
  comment    VARCHAR(199)
);


create or replace table CORE_DWH.part
(
  partkey     INTEGER not null,
  name        VARCHAR(55),
  mfgr        CHAR(25),
  brand       CHAR(10),
  type        VARCHAR(25),
  size        INTEGER,
  container   CHAR(10),
  retailprice INTEGER,
  comment     VARCHAR(23)
);


create or replace table CORE_DWH.customer
(
  custkey    INTEGER not null,
  name       VARCHAR(25),
  address    VARCHAR(40),
  nationkey  INTEGER,
  phone      CHAR(15),
  acctbal    FLOAT8,
  mktsegment CHAR(10),
  comment    VARCHAR(117)
);


create or replace table CORE_DWH.lineitem
(
  orderkey      INTEGER not null,
  partkey       INTEGER not null,
  suppkey       INTEGER not null,
  linenumber    INTEGER not null,
  quantity      INTEGER not null,
  extendedprice FLOAT8 not null,
  discount      FLOAT8 not null,
  tax           FLOAT8 not null,
  returnflag    CHAR(1),
  linestatus    CHAR(1),
  shipdate      DATE,
  commitdate    DATE,
  receiptdate   DATE,
  shipinstruct  CHAR(25),
  shipmode      CHAR(10),
  comment       VARCHAR(44)
);

-- drop stage EPAM_LAB.CORE_DWH.int_stage;
-- CREATE STAGE EPAM_LAB.CORE_DWH.aws_stage URL = 's3://tpch' COMMENT = 'TPCH external stage';
--CREATE STAGE EPAM_LAB.CORE_DWH.int_stage COMMENT = 'Internal stage for CORE_DWH';
CREATE STAGE EPAM_LAB_NEW.CORE_DWH.int_stage COMMENT = 'Internal stage for CORE_DWH';

--create stage epam_lab_new.core_dwh.int_stage clone EPAM_LAB.CORE_DWH.int_stage;
-- 000002 (0A000): Unsupported feature 'Cloning internal and temporary stages'.

--list @EPAM_LAB.CORE_DWH.int_stage;
list @EPAM_LAB_NEW.CORE_DWH.int_stage;

--PUT data by using SNOWSQL
-- snowsql -a jp54772.eu-west-2.aws -u havrilov
-- PUT 'file://C://snowflake_epam_hands-on_lab\tcph2data\\*.*' @EPAM_LAB.CORE_DWH.int_stage;
-- PUT 'file://C:/snowflake_epam_hands-on_lab/tcph2data/*.*' @EPAM_LAB.CORE_DWH.int_stage;
-- For MAC
// PUT 'file:///Users/dmytro_havrilov/Documents/EPAM_snowflake_lab/tcph2data/*.*' @EPAM_LAB_NEW.CORE_DWH.int_stage;
--remove @EPAM_LAB.CORE_DWH.int_stage;

-- create file formats
// CSV
CREATE FILE FORMAT CORE_DWH.CSV TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\n' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\042' TRIM_SPACE = TRUE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N');

// DSV
CREATE FILE FORMAT CORE_DWH.DSV TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = '|' RECORD_DELIMITER = '\n' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\042' TRIM_SPACE = TRUE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = 'NONE' DATE_FORMAT = 'DD.MM.YY' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N');

--truncate table EPAM_LAB.CORE_DWH.supplier;

select top 10 $1 from @EPAM_LAB_NEW.CORE_DWH.int_stage/h_lineitem.dsv.gz;

select top 10 * from EPAM_LAB_NEW.CORE_DWH.lineitem;

-- drop task EPAM_LAB.CORE_DWH.customer;
-- truncate table EPAM_LAB.CORE_DWH.customer;

-- select * from EPAM_LAB.CORE_DWH.customer limit 3;

copy into CORE_DWH.customer(CUSTKEY,NAME,ADDRESS,NATIONKEY,PHONE,ACCTBAL,MKTSEGMENT,COMMENT)
from (select d.$1, d.$2, d.$3, d.$4, d.$5, replace(d.$6, ',','.') ,d.$7, d.$8
from @EPAM_LAB_NEW.CORE_DWH.int_stage/h_customer.dsv.gz d)
file_format = (format_name = CORE_DWH.DSV);

copy into CORE_DWH.lineitem(ORDERKEY,PARTKEY,SUPPKEY,LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX,RETURNFLAG,LINESTATUS,SHIPDATE,COMMITDATE,RECEIPTDATE,SHIPINSTRUCT,SHIPMODE,COMMENT)
from (select d.$1, d.$2, d.$3, d.$4, d.$5, replace(d.$6, ',','.'), replace(d.$7, ',','.'), replace(d.$8, ',','.'), d.$9, d.$10, d.$11, d.$12, d.$13, d.$14, d.$15, d.$16
from @EPAM_LAB_NEW.CORE_DWH.int_stage/h_lineitem.dsv.gz d)
file_format = (format_name = CORE_DWH.DSV);

copy into CORE_DWH.nation
from @EPAM_LAB_NEW.CORE_DWH.int_stage
files = ('h_nation.dsv.gz')
file_format = (format_name='CORE_DWH.DSV');


copy into CORE_DWH.orders(ORDERKEY,CUSTKEY,ORDERSTATUS,TOTALPRICE,ORDERDATE,ORDERPRIORITY,CLERK,SHIPPRIORITY,COMMENT)
from (select d.$1, d.$2, d.$3, replace(d.$4, ',','.'), d.$5, d.$6, d.$7, d.$8, d.$9
from @EPAM_LAB_NEW.CORE_DWH.int_stage/h_order.dsv.gz d)
file_format = (format_name = CORE_DWH.DSV);

copy into CORE_DWH.part
from @EPAM_LAB_NEW.CORE_DWH.int_stage
files = ('h_part.dsv.gz')
file_format = (format_name='CORE_DWH.DSV');

copy into CORE_DWH.partsupp(PARTKEY,SUPPKEY,AVAILQTY,SUPPLYCOST,COMMENT)
from (select d.$1, d.$2, d.$3, replace(d.$4, ',','.'), d.$5
from @EPAM_LAB_NEW.CORE_DWH.int_stage/h_partsupp.dsv.gz d)
file_format = (format_name = CORE_DWH.DSV);

copy into CORE_DWH.region
from @EPAM_LAB_NEW.CORE_DWH.int_stage
files = ('h_region.csv.gz')
file_format = (format_name='CORE_DWH.CSV');

copy into CORE_DWH.supplier(SUPPKEY, NAME, ADDRESS,NATIONKEY,PHONE,ACCTBAL,COMMENT)
from (select d.$1, d.$2, d.$3, d.$4, d.$5, replace(d.$6, ',','.'), d.$7
from @EPAM_LAB_NEW.CORE_DWH.int_stage/h_supplier.dsv.gz d)
file_format = (format_name = CORE_DWH.DSV);

select count(*) from EPAM_LAB.CORE_DWH.lineitem
union
select count(*) from EPAM_LAB.CORE_DWH.orders
union
select count(*) from EPAM_LAB.CORE_DWH.partsupp
union
select count(*) from EPAM_LAB.CORE_DWH.part
union
select count(*) from EPAM_LAB.CORE_DWH.customer
union
select count(*) from EPAM_LAB.CORE_DWH.supplier
union
select count(*) from EPAM_LAB.CORE_DWH.nation
union
select count(*) from EPAM_LAB.CORE_DWH.region;

-- CREATE OR REPLACE PIPE snowflake
-- AS COPY INTO EPAM_LAB.CORE_DWH.LINEITEM FROM @EPAM_LAB.CORE_DWH.INT_STAGE FILE_FORMAT = ( FORMAT_NAME =  EPAM_LAB.CORE_DWH.DSV )


// DATA_MART
-- create tables

create or replace table DATA_MART.orders
(
  orderkey      INTEGER not null,
  custkey       INTEGER,
  nationkey     INTEGER,
  regionkey     INTEGER,
  orderstatus   CHAR(1),
  totalprice    FLOAT8,
  orderdate     DATE,
  orderpriority CHAR(15),
  clerk         CHAR(15),
  shippriority  INTEGER,
  comment       VARCHAR(79)
);

create or replace table DATA_MART.customer
(
  custkey    INTEGER not null,
  name       VARCHAR(25),
  address    VARCHAR(40),
  phone      CHAR(15),
  acctbal    FLOAT8,
  mktsegment CHAR(10),
  comment    VARCHAR(117)
);

create or replace table DATA_MART.region
(
  regionkey INTEGER,
  name      CHAR(25),
  comment   VARCHAR(152)
);


create or replace table DATA_MART.nation
(
  nationkey INTEGER not null,
  name      CHAR(27),
  comment   VARCHAR(155)
);


alter table data_mart.orders
add constraint orderkey_pk primary key (orderkey);

alter table data_mart.customer
add constraint custkey_pk primary key (custkey);

alter table data_mart.nation
add constraint nationkey_pk primary key (nationkey);

alter table data_mart.region
add constraint regionkey_pk primary key (regionkey);

alter table data_mart.orders
add constraint custkey_fk foreign key (custkey)
references data_mart.customer(custkey);

alter table data_mart.orders
add constraint nationkey_fk foreign key (nationkey)
references data_mart.nation(nationkey);

alter table data_mart.orders
add constraint regionkey_fk foreign key (regionkey)
references data_mart.region(regionkey);

--- procedure to load data from CORE to MART

CREATE OR REPLACE PROCEDURE load_data_to_datamart()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
  BEGIN
    insert into data_mart.region
    select regionkey,
    name,
    comment
    from core_dwh.region;
    
    insert into data_mart.nation
    select nationkey,
    name,
    comment
    from core_dwh.nation;
    
    insert into data_mart.customer
    select custkey,
    name,
    address,
    phone,
    acctbal,
    mktsegment,
    comment
    from core_dwh.customer;
    
    insert into data_mart.orders
    select
    o.orderkey,
    o.custkey,
    n.nationkey,
    r.regionkey,
    o.orderstatus,
    o.totalprice,
    o.orderdate,
    o.orderpriority,
    o.clerk,
    o.shippriority,
    o.comment
    from core_dwh.orders o
    join core_dwh.customer c on c.custkey = o.custkey
    join core_dwh.nation n on n.nationkey = c.nationkey
    join core_dwh.region r on r.regionkey = n.regionkey;

END;
$$;

-- truncate table data_mart.customer;

use warehouse epam;
call load_data_to_datamart();

// STREAMS
create or replace stream region_stream on table epam_lab.core_dwh.region append_only=true;
create or replace stream nation_stream on table epam_lab.core_dwh.nation append_only=true;
create or replace stream supplier_stream on table epam_lab.core_dwh.supplier append_only=true;
create or replace stream customer_stream on table epam_lab.core_dwh.customer append_only=true;
create or replace stream orders_stream on table epam_lab.core_dwh.orders append_only=true;
create or replace stream part_stream on table epam_lab.core_dwh.part append_only=true;
create or replace stream partsupp_stream on table epam_lab.core_dwh.partsupp append_only=true;
create or replace stream lineitem_stream on table epam_lab.core_dwh.lineitem append_only=true;

// TASKS
create or replace task datamart_stream_procedure
warehouse = EPAM
schedule = '5 minutes'
when system$stream_has_data('region_stream')
and system$stream_has_data('nation_stream')
and system$stream_has_data('supplier_stream')
and system$stream_has_data('orders_stream')
and system$stream_has_data('orders_stream')
and system$stream_has_data('part_stream')
and system$stream_has_data('partsupp_stream')
and system$stream_has_data('lineitem_stream')
as
call load_data_to_datamart();

-- activate task
alter task datamart_stream_procedure resume;

show tasks;
show procedures;

create table CORE_DWH.supplier_backup clone CORE_DWH.supplier;


