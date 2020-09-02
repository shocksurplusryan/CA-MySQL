/* TRACKING NUMBER PROCESSING */

/* SET GLOBALS */
set @lookback_days = 30 ;
set @shipwarning_days = 2 ;


/* PRE-CHECKS FOR DEV */
-- SELECT COLUMN_NAME,COLUMN_TYPE,CHARACTER_SET_NAME,COLLATION_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'carawdata' AND TABLE_NAME = 'ca_fulfillment' ;
-- SELECT COLUMN_NAME,COLUMN_TYPE,CHARACTER_SET_NAME,COLLATION_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'carawdata' AND TABLE_NAME = 'ca_order'  order by ordinal_position ;
-- select * from carawdata.ca_fulfillment order by updated_date_utc desc ;
-- select * from carawdata.ca_order order by id desc ;
-- /* DISPLAY DUPES IN FULF */
-- select * from carawdata.ca_fulfillment 
-- 	group by order_id, tracking_number
--     having count(*)>1
--     order by order_id desc
--     ;
-- /* DISPLAY DUPES IN ORDER */
-- select * from carawdata.ca_order 
-- 	group by id
--     having count(*)>1
--     order by id desc
--     ;



/* PULL IN NEW RAW FULFILLMENT DB, LIMIT TO VARS WE NEED */
/* IMPORTING FROM 'carawdata' PERMANENT DB */
drop table if exists carawdata.fulf_raw_clean_live ;
create table carawdata.fulf_raw_clean_live 
	(order_id bigint,
	created_date_utc datetime,
	updated_date_utc datetime,
	delivery_status varchar(100),
	tracking_number varchar(255),
	shipping_carrier varchar(100),
	shipping_class varchar(100),
	distribution_center_id varchar(50),
	external_fulfillment_status varchar(100),
	shipped_date_utc varchar(50),
	dc varchar(100))
    ;
insert ignore carawdata.fulf_raw_clean_live 
	select
		order_id,
        created_date_utc,
        updated_date_utc,
        delivery_status,
        tracking_number,
        shipping_carrier,
        shipping_class,
        distribution_center_id,
        external_fulfillment_status,
        shipped_date_utc,
        (case trim(distribution_center_id)
			when '-1000' then 'Transamerican'
			when '-1003' then 'Turn14'
			when '-1007' then 'Parts Authority'
			when '-1006' then 'Premier'
			when '-1004' then 'Turn14'
			when '-1001' then 'Keystone'
			when '-1002' then 'Turn14'
			when '-1005' then 'Meyer'
			when '3' then 'SOSS inFlow'
			end) as dc
	from carawdata.ca_fulfillment
    where lcase(delivery_status)!='canceled'
    order by order_id desc
    ;    
/* CREATE A FULFILLMENT DB AT ORDER LEVEL */
drop table if exists carawdata.fulf_order_level_live_setup ;
create table carawdata.fulf_order_level_live_setup 
	(order_id bigint,created_date_utc_min date,
    tracking_number_count float not null,tracking_number_list_setup varchar(250),delivery_status_list varchar(100),shipping_carrier_list varchar(100),
    external_fulfillment_status_list varchar(100),dc_list varchar(250))
    ;
insert ignore carawdata.fulf_order_level_live_setup 
	select
		order_id,
        min(created_date_utc) as created_date_utc_min,
		count(tracking_number) as tracking_number_count,
		group_concat(ifnull(tracking_number,'')) as tracking_number_list_setup,
		group_concat(ifnull(delivery_status,'')) as delivery_status_list,
		group_concat(ifnull(shipping_carrier,'')) as shipping_carrier_list,
        group_concat(ifnull(external_fulfillment_status,'')) as external_fulfillment_status_list,
        group_concat(ifnull(dc,'')) as dc_list
	from carawdata.fulf_raw_clean_live 
	group by order_id 
	;
drop table if exists carawdata.fulf_order_level_live ;
create table carawdata.fulf_order_level_live
	(order_id bigint,created_date_utc_min date,
    tracking_number_list text,
    tracking_number_count float,
    delivery_status_list varchar(100),shipping_carrier_list varchar(100),
    external_fulfillment_status_list text,dc_list varchar(250))
	;
insert ignore carawdata.fulf_order_level_live
    select 
		order_id,
        created_date_utc_min,
        /* CLEAN COMMA-SEPARATED LIST OF TRACKING NUMBERS */
		if(substring_index(tracking_number_list_setup,',',1)='',mid(tracking_number_list_setup,2,length(tracking_number_list_setup)-1),tracking_number_list_setup) as tracking_number_list,
        tracking_number_count,
        delivery_status_list,
        shipping_carrier_list,
        external_fulfillment_status_list,
        dc_list
	from carawdata.fulf_order_level_live_setup
    order by order_id desc
    ;    
    select * from carawdata.fulf_order_level_live ;
/* DUPE CHECK */
-- select distinct order_id from carawdata.fulf_order_level_live ;

/* END CLEANING OF FULFILLMENT DB */




/* CLEAN ORDER DB */
drop table if exists carawdata.order_db_raw_clean_live ;
create table carawdata.order_db_raw_clean_live 
	(id bigint,
    site_name varchar(50),
    site_order_id varchar(50),
    created_date_utc datetime,
    public_notes text,
    private_notes text,
    special_instructions text,
	total_price decimal(10,2),
	total_tax_price decimal(10,2),
	total_shipping_price decimal(10,2),
	total_shipping_tax_price decimal(10,2),
	requested_shipping_carrier varchar(50),
    requested_shipping_class varchar(50),
    flag_id varchar(10),
	flag_description text,
    order_tags text,
    distribution_center_type_rollup varchar(100),
    checkout_status varchar(25),
    payment_status varchar(25),
    shipping_status varchar(25),
    checkout_date_utc datetime,
    payment_date_utc datetime,
    shipping_date_utc datetime,
    buyer_user_id varchar(150),
    buyer_email_address varchar(150),
    payment_method varchar(50),
    payment_transaction_id varchar(100),
    payment_credit_card_last4 varchar(5),
    shipping_company_name varchar(150),
    shipping_daytime_phone varchar(50),
    shipping_address_line1 varchar(250),
    shipping_address_line2 varchar(250),
    shipping_city varchar(100),
    shipping_state_or_province varchar(50),
    shipping_postal_code varchar(15),
    shipping_country varchar(50),
    billing_company_name varchar(150),
    billing_daytime_phone varchar(50),
    billing_address_line1 varchar(250),
    billing_address_line2 varchar(250),
    billing_city varchar(100),
    billing_state_or_province_name varchar(50),
    billing_postal_code varchar(15),
    billing_country varchar(50),
    promotion_code varchar(30),
    promotion_amount decimal(10,2))
	;
insert ignore carawdata.order_db_raw_clean_live
	select
		id,
		site_name,
		site_order_id,
		created_date_utc,
		ifnull(public_notes,'') as public_notes,
		ifnull(private_notes,'') as private_notes,
		special_instructions,
		total_price,
		total_tax_price,
		total_shipping_price,
		total_shipping_tax_price,
		requested_shipping_carrier,
		requested_shipping_class,
		flag_id,
		ifnull(flag_description,'') as flag_description,
		ifnull(order_tags,'') as order_tags,
		distribution_center_type_rollup,
		checkout_status,
		payment_status,
		shipping_status,
		checkout_date_utc,
		ifnull(payment_date_utc,'') as payment_date_utc,
		shipping_date_utc,
		buyer_user_id,
		buyer_email_address,
		ifnull(payment_method,'') as payment_method,
		ifnull(payment_transaction_id,'') as payment_transaction_id,
		payment_credit_card_last4,
		ifnull(shipping_company_name,'') as shipping_company_name,
		shipping_daytime_phone,
		shipping_address_line1,
		shipping_address_line2,
		shipping_city,
		shipping_state_or_province,
		shipping_postal_code,
		shipping_country,
		ifnull(billing_company_name,'') as billing_company_name,
		billing_daytime_phone,
		billing_address_line1,
		billing_address_line2,
		billing_city,
		billing_state_or_province_name,
		billing_postal_code,
		billing_country,
		ifnull(promotion_code,'') as promotion_code,
		promotion_amount 
	from carawdata.ca_order
    order by id desc    
    ;
    
/* END CLEANING OF ORDER DB */




/* JOIN CUSTOMER DATA ONTO FULFILMENT DB FROM CA ORDERS DB */
drop table if exists carawdata.fulf_order_level_with_data ;
create table carawdata.fulf_order_level_with_data like carawdata.fulf_order_level_live ;
alter table carawdata.fulf_order_level_with_data 
	add site_name varchar(50),
	add site_order_id varchar(50),
	add created_date_utc datetime,
	add public_notes text,
	add private_notes text,
	add special_instructions text,
	add total_price decimal(10,2),
	add total_tax_price decimal(10,2),
	add total_shipping_price decimal(10,2),
	add total_shipping_tax_price decimal(10,2),
	add requested_shipping_carrier varchar(50),
	add requested_shipping_class varchar(50),
	add flag_id varchar(10),
	add order_tags text,
	add distribution_center_type_rollup varchar(100),
	add checkout_status varchar(25),
	add payment_status varchar(25),
	add shipping_status varchar(25),
	add checkout_date_utc datetime,
	add payment_date_utc datetime,
	add shipping_date_utc datetime,
	add buyer_user_id varchar(150),
	add buyer_email_address varchar(150),
	add payment_method varchar(50),
	add payment_transaction_id varchar(100),
	add payment_credit_card_last4 varchar(5),
	add shipping_company_name varchar(150),
	add shipping_daytime_phone varchar(50),
	add shipping_address_line1 varchar(250),
	add shipping_address_line2 varchar(250),
	add shipping_city varchar(100),
	add shipping_state_or_province varchar(50),
	add shipping_postal_code varchar(15),
	add shipping_country varchar(50),
	add billing_company_name varchar(150),
	add billing_daytime_phone varchar(50),
	add billing_address_line1 varchar(250),
	add billing_address_line2 varchar(250),
	add billing_city varchar(100),
	add billing_state_or_province_name varchar(50),
	add billing_postal_code varchar(15),
	add billing_country varchar(50),
	add promotion_code varchar(30),
	add promotion_amount decimal(10,2)
	;
insert ignore carawdata.fulf_order_level_with_data
	select 
		ff.*,
		site_name,
		site_order_id,
		created_date_utc,
		public_notes,
		private_notes,
		special_instructions,
		total_price,
		total_tax_price,
		total_shipping_price,
		total_shipping_tax_price,
		requested_shipping_carrier,
		requested_shipping_class,
		flag_id,
		flag_description,
		distribution_center_type_rollup,
		checkout_status,
		payment_status,
		shipping_status,
		checkout_date_utc,
		payment_date_utc,
		shipping_date_utc,
		buyer_user_id,
		buyer_email_address,
		payment_method,
		payment_transaction_id,
		payment_credit_card_last4,
		shipping_company_name,
		shipping_daytime_phone,
		shipping_address_line1,
		shipping_address_line2,
		shipping_city,
		shipping_state_or_province,
		shipping_postal_code,
		shipping_country,
		billing_company_name,
		billing_daytime_phone,
		billing_address_line1,
		billing_address_line2,
		billing_city,
		billing_state_or_province_name,
		billing_postal_code,
		billing_country,
		promotion_code,
		promotion_amount
	from carawdata.fulf_order_level_live as ff
    left join carawdata.order_db_raw_clean_live as oo
    on ff.order_id=oo.id
	;
--     select * from carawdata.fulf_order_level_with_data ;




/* JOIN NEW INCOMING ORDERS TO EXISTING DATA */
drop table if exists carawdata.fulf_new_vs_existing ;
create table carawdata.fulf_new_vs_existing like carawdata.fulf_order_level_with_data ;
alter table carawdata.fulf_new_vs_existing 
	add tracking_number_count_existing float,
    add tracking_number_list_existing text,
    add external_fulfillment_status_list_existing text
	;
insert ignore carawdata.fulf_new_vs_existing
	select
		newnew.*,
		ex.tracking_number_count as tracking_number_count_existing,
        ex.tracking_number_list as tracking_number_list_existing,
        ex.external_fulfillment_status_list as external_fulfillment_status_list_existing
	from carawdata.fulf_order_level_with_data as newnew
    left join carawdata.fulf_order_level_with_data_existing as ex
    on newnew.order_id=ex.order_id
	;
-- 	select * from carawdata.fulf_new_vs_existing ;







/* CREATE SUBSETS OF ORDER_ID THAT NEED UPDATE EMAIL TO CUSTOMER */
/* 11111 -- MULTIPLE PACAKGES */
/* ORDER IS WITHIN # LOOKBACK DAYS, AT LEAST 1 TRACKING NUMBER, AND TRACKING NUMBER COUNT INCREASED */
drop table if exists carawdata.fulf_detector_output_multitracks ;
create table carawdata.fulf_detector_output_multitracks like carawdata.fulf_new_vs_existing ;
insert ignore carawdata.fulf_detector_output_multitracks
	select *
	from carawdata.fulf_new_vs_existing
    where 
		tracking_number_count_existing >= 1 and
        tracking_number_count > tracking_number_count_existing and
        created_date_utc_min >= date(now() - interval @lookback_days day)
	order by order_id
	;
    select * from carawdata.fulf_detector_output_multitracks ;

/* END CUSTOMER EMAIL DATABASES */
/* END CUSTOMER EMAIL DATABASES */
/* END CUSTOMER EMAIL DATABASES */













/* CREATE SUBSETS OF ORDER_ID FOR INTERNAL EMAILS */
/* CREATE SUBSETS OF ORDER_ID FOR INTERNAL EMAILS */
/* CREATE SUBSETS OF ORDER_ID FOR INTERNAL EMAILS */
/* 11111 -- UNSHIPPED IN X # DAYS DEFINED BY SHIPWARNING_DAYS */
drop table if exists carawdata.fulf_detector_output_unshipped_warning ;
create table carawdata.fulf_detector_output_unshipped_warning like carawdata.fulf_new_vs_existing ;
insert ignore carawdata.fulf_detector_output_unshipped_warning
	select *
	from carawdata.fulf_new_vs_existing
    where 
		tracking_number_count=0 and	
        /* WITHIN SHIPWARNING_DAYS OF NOW, ACCOUNTING FOR BUSINESS DAYS */
        (
			-- mon
            if(dayofweek(now())=2,created_date_utc_min <= date(now() - interval (@shipwarning_days+2) day),
            -- tues
            if(dayofweek(now())=3,created_date_utc_min <= date(now() - interval (@shipwarning_days+3) day),
            -- wed,thu,fri
            if(dayofweek(now()) in (4,5,6),created_date_utc_min <= date(now() - interval (@shipwarning_days) day),FALSE)))
		) and
        /* ONLY FLAG THE NON-SHIPS WITHIN LOOKBACK_DAYS NUMBER OF DAYS */
        created_date_utc_min >= date(now() - interval @lookback_days day)
	;
-- 	select * from carawdata.fulf_detector_output_unshipped_warning ;
/* 22222 -- CHANGE OF FULFILLMENT STATUS TO FAIL FROM ANOTHER STATE, WITHIN PAST X # DAYS, SET BY SHIPWARNING_DAYS */
drop table if exists carawdata.fulf_detector_output_fail_warning ;
create table carawdata.fulf_detector_output_fail_warning like carawdata.fulf_new_vs_existing ;
insert ignore carawdata.fulf_detector_output_fail_warning
	select *
	from carawdata.fulf_new_vs_existing
    where 
		locate('fail',lcase(external_fulfillment_status_list))>0 and
        created_date_utc_min >= date(now() - interval @shipwarning_days day)
	;
-- 	select * from carawdata.fulf_detector_output_fail_warning ;
 /* 33333 -- WARN ON INTERNATIONAL ORDERS */
drop table if exists carawdata.fulf_detector_output_international_warning ;
create table carawdata.fulf_detector_output_international_warning like carawdata.fulf_new_vs_existing ;
	select *
	from carawdata.fulf_new_vs_existing
    where lcase(shipping_country)!='us' and lcase(shipping_country)!='united states' and lcase(shipping_country)!='united states of america'
	;
  /* 44444 -- WARN ON P.O. BOXES */
drop table if exists carawdata.fulf_detector_output_po_box_warning ;
create table carawdata.fulf_detector_output_po_box_warning like carawdata.fulf_new_vs_existing ;
	select *
	from carawdata.fulf_new_vs_existing
    where 
		locate('po box',lcase(shipping_address_line1))>0 or
		locate('p.o. box',lcase(shipping_address_line1))>0 or
		locate('post office box',lcase(shipping_address_line1))>0 or
		locate('pst off box',lcase(shipping_address_line1))>0 or
		locate('p.o box',lcase(shipping_address_line1))>0 or
		locate('usps box',lcase(shipping_address_line1))>0
	;
-- select * from carawdata.fulf_detector_output_po_box_warning ; 
  /* 55555 -- SOSS IN FLOW ORDERS */
drop table if exists carawdata.fulf_detector_output_sossinflow_order ;
create table carawdata.fulf_detector_output_sossinflow_order like carawdata.fulf_new_vs_existing ;
insert ignore carawdata.fulf_detector_output_sossinflow_order
	select *
	from carawdata.fulf_new_vs_existing
    where locate('soss',lcase(dc_list))>0
	;
-- 	select * from carawdata.fulf_detector_output_sossinflow_order ;
 
 /* END INTERNAL EMAIL SUBSETS */
 /* END INTERNAL EMAIL SUBSETS */
 /* END INTERNAL EMAIL SUBSETS */
 
 
 
 
 
 
 
 

/* SAVE UPDATED DB AS EXISTING TO CADATA PERMANENT DB */
-- drop table if exists carawdata.fulf_order_level_existing ;
-- create table carawdata.fulf_order_level_existing like carawdata.fulf_order_level_live ;
-- insert ignore carawdata.fulf_order_level_existing
-- 	select *
-- 	from carawdata.fulf_order_level_live
-- 	;






/* CLEAN UP TEMP TABLES */
-- drop table if exists carawdata.fulf_raw_clean_live ;
-- drop table if exists carawdata.fulf_order_level_live_setup ;
-- drop table if exists carawdata.fulf_new_vs_existing ;

