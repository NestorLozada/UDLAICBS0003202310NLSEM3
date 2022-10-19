Use nldbstg
;
CREATE TABLE CHANNELS 
    ( 
     CHANNEL_ID VARCHAR(10)  NOT NULL , 
     CHANNEL_DESC VARCHAR (20 )  NOT NULL , 
     CHANNEL_CLASS VARCHAR (20 )  NOT NULL , 
     CHANNEL_CLASS_ID VARCHAR (10)  NOT NULL 
    )
;
alter table CHANNELS 	
add constraint CHANNELS_PK Primary Key (CHANNEL_ID) 
;

CREATE TABLE COUNTRIES 
    ( 
     COUNTRY_ID VARCHAR (10)  NOT NULL , 
     COUNTRY_NAME VARCHAR (40 )  NOT NULL , 
     COUNTRY_REGION VARCHAR (20 )  NOT NULL , 
     COUNTRY_REGION_ID VARCHAR (10)  NOT NULL 
    ) 
;

ALTER TABLE COUNTRIES 
    ADD CONSTRAINT COUNTRIES_PK PRIMARY KEY ( COUNTRY_ID )  ;


CREATE TABLE CUSTOMERS 
    ( 
     CUST_ID VARCHAR (10)  NOT NULL , 
     CUST_FIRST_NAME VARCHAR (20 )  NOT NULL , 
     CUST_LAST_NAME VARCHAR (40 )  NOT NULL , 
     CUST_GENDER CHAR (1 )  NOT NULL , 
     CUST_YEAR_OF_BIRTH VARCHAR (4)  NOT NULL , 
     CUST_MARITAL_STATUS VARCHAR (20 ) , 
     CUST_STREET_ADDRESS VARCHAR(40 )  NOT NULL , 
     CUST_POSTAL_CODE VARCHAR (10 )  NOT NULL , 
     CUST_CITY VARCHAR (30 )  NOT NULL , 
     CUST_STATE_PROVINCE VARCHAR (40 )  NOT NULL , 
     COUNTRY_ID VARCHAR (10)  NOT NULL , 
     CUST_MAIN_PHONE_NUMBER VARCHAR (25 )  NOT NULL , 
     CUST_INCOME_LEVEL VARCHAR (30 ) , 
     CUST_CREDIT_LIMIT VARCHAR (10) , 
     CUST_EMAIL VARCHAR (30 )
    )
;

ALTER TABLE CUSTOMERS 
    ADD CONSTRAINT CUSTOMERS_PK PRIMARY KEY ( CUST_ID )  ;


CREATE TABLE PRODUCTS 
    ( 
     PROD_ID VARCHAR (6)  NOT NULL , 
     PROD_NAME VARCHAR (50 )  NOT NULL , 
     PROD_DESC VARCHAR (4000 )  NOT NULL , 
     PROD_CATEGORY VARCHAR (50 )  NOT NULL , 
     PROD_CATEGORY_ID VARCHAR (10)  NOT NULL , 
     PROD_CATEGORY_DESC VARCHAR (2000 )  NOT NULL , 
     PROD_WEIGHT_CLASS VARCHAR (3)  NOT NULL , 
     SUPPLIER_ID VARCHAR (6)  NOT NULL , 
     PROD_STATUS VARCHAR (20 )  NOT NULL , 
     PROD_LIST_PRICE VARCHAR (10)  NOT NULL , 
     PROD_MIN_PRICE VARCHAR (10) NOT NULL 
    )
;

ALTER TABLE PRODUCTS 
    ADD CONSTRAINT PRODUCTS_PK PRIMARY KEY ( PROD_ID )  ;


CREATE TABLE PROMOTIONS 
    ( 
     PROMO_ID VARCHAR (6)  NOT NULL , 
     PROMO_NAME VARCHAR (30 )  NOT NULL , 
     PROMO_COST VARCHAR (12)  NOT NULL , 
     PROMO_BEGIN_DATE VARCHAR (10)  NOT NULL , 
     PROMO_END_DATE VARCHAR (10)  NOT NULL 
    )
;


ALTER TABLE PROMOTIONS 
    ADD CONSTRAINT PROMO_PK PRIMARY KEY ( PROMO_ID )  ;


CREATE TABLE SALES 
    ( 
     PROD_ID VARCHAR (6)  NOT NULL , 
     CUST_ID VARCHAR (10)  NOT NULL , 
     TIME_ID VARCHAR (10)  NOT NULL , 
     CHANNEL_ID VARCHAR (10)  NOT NULL , 
     PROMO_ID VARCHAR (6)  NOT NULL , 
     QUANTITY_SOLD VARCHAR (12)  NOT NULL , 
     AMOUNT_SOLD VARCHAR (12)  NOT NULL 
    ) 
;

CREATE TABLE TIMES 
    ( 
     TIME_ID VARCHAR (10)  NOT NULL , 
     DAY_NAME VARCHAR (9 )  NOT NULL , 
     DAY_NUMBER_IN_WEEK VARCHAR (1)  NOT NULL , 
     DAY_NUMBER_IN_MONTH VARCHAR (2)  NOT NULL , 
     CALENDAR_WEEK_NUMBER VARCHAR (2)  NOT NULL , 
     CALENDAR_MONTH_NUMBER VARCHAR (2)  NOT NULL , 
     CALENDAR_MONTH_DESC VARCHAR (8 )  NOT NULL , 
     END_OF_CAL_MONTH VARCHAR(10)   NOT NULL , 
     CALENDAR_MONTH_NAME VARCHAR (9 )  NOT NULL , 
     CALENDAR_QUARTER_DESC CHAR (7 )  NOT NULL , 
     CALENDAR_YEAR VARCHAR (4)  NOT NULL 
    ) 
;

ALTER TABLE TIMES 
    ADD CONSTRAINT TIMES_PK PRIMARY KEY ( TIME_ID )  ;

