create database EPAM_LAB;
create schema EPAM_LAB.CORE_DWH;
create schema EPAM_LAB.DATA_MART;
create schema EPAM_LAB.TPCH;

use schema EPAM_LAB.TPCH;

create or replace table EPAM_LAB.TPCH.region
(
  r_regionkey INTEGER,
  r_name      CHAR(25),
  r_comment   VARCHAR(152)
);


create or replace table EPAM_LAB.TPCH.nation
(
  n_nationkey INTEGER not null,
  n_name      CHAR(27),
  n_regionkey INTEGER,
  n_comment   VARCHAR(155)
);


create or replace table EPAM_LAB.TPCH.supplier
(
  s_suppkey   INTEGER not null,
  s_name      CHAR(25),
  s_address   VARCHAR(40),
  s_nationkey INTEGER,
  s_phone     CHAR(15),
  s_acctbal   FLOAT8,
  s_comment   VARCHAR(101)
);


create or replace table EPAM_LAB.TPCH.orders
(
  o_orderkey      INTEGER not null,
  o_custkey       INTEGER not null,
  o_orderstatus   CHAR(1),
  o_totalprice    FLOAT8,
  o_orderdate     DATE,
  o_orderpriority CHAR(15),
  o_clerk         CHAR(15),
  o_shippriority  INTEGER,
  o_comment       VARCHAR(79)
);


create or replace table EPAM_LAB.TPCH.partsupp
(
  ps_partkey    INTEGER not null,
  ps_suppkey    INTEGER not null,
  ps_availqty   INTEGER,
  ps_supplycost FLOAT8 not null,
  ps_comment    VARCHAR(199)
);


create or replace table EPAM_LAB.TPCH.part
(
  p_partkey     INTEGER not null,
  p_name        VARCHAR(55),
  p_mfgr        CHAR(25),
  p_brand       CHAR(10),
  p_type        VARCHAR(25),
  p_size        INTEGER,
  p_container   CHAR(10),
  p_retailprice INTEGER,
  p_comment     VARCHAR(23)
);


create or replace table EPAM_LAB.TPCH.customer
(
  c_custkey    INTEGER not null,
  c_name       VARCHAR(25),
  c_address    VARCHAR(40),
  c_nationkey  INTEGER,
  c_phone      CHAR(15),
  c_acctbal    FLOAT8,
  c_mktsegment CHAR(10),
  c_comment    VARCHAR(117)
);


create or replace table EPAM_LAB.TPCH.lineitem
(
  l_orderkey      INTEGER not null,
  l_partkey       INTEGER not null,
  l_suppkey       INTEGER not null,
  l_linenumber    INTEGER not null,
  l_quantity      INTEGER not null,
  l_extendedprice FLOAT8 not null,
  l_discount      FLOAT8 not null,
  l_tax           FLOAT8 not null,
  l_returnflag    CHAR(1),
  l_linestatus    CHAR(1),
  l_shipdate      DATE,
  l_commitdate    DATE,
  l_receiptdate   DATE,
  l_shipinstruct  CHAR(25),
  l_shipmode      CHAR(10),
  l_comment       VARCHAR(44)
);

-- drop stage epam_lab.tpch.int_stage;
CREATE STAGE EPAM_LAB.TPCH.aws_stage URL = 's3://tpch' COMMENT = 'TPCH external stage';
CREATE STAGE EPAM_LAB.TPCH.int_stage COMMENT = 'Internal stage for TPCH';

list @EPAM_LAB.TPCH.int_stage;

--PUT data by using SNOWSQL
-- snowsql -a no19089.ca-central-1.aws -u havrilov
-- snowsql -a hu69119.eu-west-2.aws -u havrilov
-- PUT 'file://C://snowflake_epam_hands-on_lab\tcph2data\\*.*' @EPAM_LAB.TPCH.int_stage;
-- PUT 'file://C:/snowflake_epam_hands-on_lab/tcph2data/*.*' @EPAM_LAB.TPCH.int_stage;

-- create file formats
CREATE FILE FORMAT EPAM_LAB.TPCH.CSV TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\n' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\042' TRIM_SPACE = TRUE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N');

--DSV
CREATE FILE FORMAT EPAM_LAB.TPCH.DSV TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = '|' RECORD_DELIMITER = '\n' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\042' TRIM_SPACE = TRUE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = 'NONE' DATE_FORMAT = 'DD.MM.YY' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N');

--truncate table EPAM_LAB.TPCH.supplier;

select $1 from @EPAM_LAB.TPCH.int_stage/h_lineitem.dsv.gz
limit 10;

select * from epam_lab.tpch.lineitem
limit 10;

-- truncate table epam_lab.tpch.region;
copy into epam_lab.tpch.customer(C_CUSTKEY,C_NAME,C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT)
from (select d.$1, d.$2, d.$3, d.$4, d.$5, replace(d.$6, ',','.') ,d.$7, d.$8
from @EPAM_LAB.TPCH.int_stage/h_customer.dsv.gz d)
file_format = (format_name = EPAM_LAB.TPCH.DSV);

copy into EPAM_LAB.TPCH.lineitem(L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT)
from (select d.$1, d.$2, d.$3, d.$4, d.$5, replace(d.$6, ',','.'), replace(d.$7, ',','.'), replace(d.$8, ',','.'), d.$9, d.$10, d.$11, d.$12, d.$13, d.$14, d.$15, d.$16
from @EPAM_LAB.TPCH.int_stage/h_lineitem.dsv.gz d)
file_format = (format_name = EPAM_LAB.TPCH.DSV);

copy into EPAM_LAB.TPCH.nation
from @EPAM_LAB.TPCH.int_stage
files = ('h_nation.dsv.gz')
file_format = (format_name='EPAM_LAB.TPCH.DSV');


copy into EPAM_LAB.TPCH.orders(O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT)
from (select d.$1, d.$2, d.$3, replace(d.$4, ',','.'), d.$5, d.$6, d.$7, d.$8, d.$9
from @EPAM_LAB.TPCH.int_stage/h_order.dsv.gz d)
file_format = (format_name = EPAM_LAB.TPCH.DSV);

copy into EPAM_LAB.TPCH.part
from @EPAM_LAB.TPCH.int_stage
files = ('h_part.dsv.gz')
file_format = (format_name='EPAM_LAB.TPCH.DSV');

copy into EPAM_LAB.TPCH.partsupp(PS_PARTKEY,PS_SUPPKEY,PS_AVAILQTY,PS_SUPPLYCOST,PS_COMMENT)
from (select d.$1, d.$2, d.$3, replace(d.$4, ',','.'), d.$5
from @EPAM_LAB.TPCH.int_stage/h_partsupp.dsv.gz d)
file_format = (format_name = EPAM_LAB.TPCH.DSV);

copy into EPAM_LAB.TPCH.region
from @EPAM_LAB.TPCH.int_stage
files = ('h_region.csv.gz')
file_format = (format_name='EPAM_LAB.TPCH.CSV');

copy into epam_lab.tpch.supplier(S_SUPPKEY, S_NAME, S_ADDRESS,S_NATIONKEY,S_PHONE,S_ACCTBAL,S_COMMENT)
from (select d.$1, d.$2, d.$3, d.$4, d.$5, replace(d.$6, ',','.'), d.$7
from @EPAM_LAB.TPCH.int_stage/h_supplier.dsv.gz d)
file_format = (format_name = EPAM_LAB.TPCH.DSV);

select count(*) from EPAM_LAB.TPCH.lineitem
union
select count(*) from EPAM_LAB.TPCH.orders
union
select count(*) from EPAM_LAB.TPCH.partsupp
union
select count(*) from EPAM_LAB.TPCH.part
union
select count(*) from EPAM_LAB.TPCH.customer
union
select count(*) from EPAM_LAB.TPCH.supplier
union
select count(*) from EPAM_LAB.TPCH.nation
union
select count(*) from EPAM_LAB.TPCH.region;

CREATE OR REPLACE PIPE snowflake
AS COPY INTO EPAM_LAB.TPCH.LINEITEM FROM @EPAM_LAB.TPCH.INT_STAGE FILE_FORMAT = ( FORMAT_NAME =  EPAM_LAB.TPCH.DSV )
