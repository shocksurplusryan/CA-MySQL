/* TRACKING NUMBER PROCESSING */

create database if not exists sp ;
set net_read_timeout = 300 ;
SET GLOBAL local_infile = 'ON' ;
TRUNCATE mysql.general_log ;
TRUNCATE mysql.slow_log ;
purge binary logs before DATE_SUB(NOW(), INTERVAL 3 hour) ;
SET group_concat_max_len = 100000000 ;



/* SET GLOBALS */
set @lookback_days = 30 ;
set @shipwarning_days = 2 ;



/* PULL IN NEW RAW FULFILLMENT DB , LIMIT TO VARS WE NEED */
drop table if exists sp.fulf_clean_1_live ;
create table sp.fulf_clean_1_live
	(dc varchar(50))
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
    select * from sp.fulf_clean_1_live ;



/* NEXT STEP FOR DEV & DATA CHECKS ONLY */
/* CREATE A WORKING DATABASE WITH RAW LEVEL JOINED TO AGGREGATED VARS, FOR DEV ONLY */
/* COUNT TRACKING NUMBERS PER ID */
-- drop table if exists sp.fulf_clean_2_with_count ;
-- create table sp.fulf_clean_2_with_count
-- 	(tracking_number_count float,tracking_number_list varchar(250),delivery_status_list varchar(100),shipping_carrier_list varchar(100))
--     select c1.*,
-- 		ifnull(cc.tracking_number_count,0) as tracking_number_count,
--         ifnull(cc.tracking_number_list,'') as tracking_number_list,
--         ifnull(cc.delivery_status_list,'') as delivery_status_list,
--         ifnull(cc.shipping_carrier_list,'') as shipping_carrier_list
-- 	from sp.fulf_clean_1_live as c1
--     left join
-- 		(select
-- 				order_id,
-- 				count(tracking_number) as tracking_number_count,
--                 group_concat(tracking_number) as tracking_number_list,
--                 group_concat(delivery_status) as delivery_status_list,
--                 group_concat(shipping_carrier) as shipping_carrier_list
-- 			from sp.fulf_clean_1_live 
-- 			where not isnull(tracking_number)
-- 			group by order_id
-- 		) as cc
-- 	on c1.order_id=cc.order_id
--     ;
    
    
    
/* CREATE A DB AT ORDER LEVEL */
drop table if exists sp.fulf_order_level_live_setup ;
create table sp.fulf_order_level_live_setup 
	(tracking_number_count float,tracking_number_list_setup varchar(250),delivery_status_list varchar(100),shipping_carrier_list varchar(100),created_date_utc_min date,
    external_fulfillment_status_list varchar(100),dc_list varchar(250))
    select
		order_id,
        min(created_date_utc) as created_date_utc_min,
		count(tracking_number) as tracking_number_count,
		replace(replace(group_concat(ifnull(tracking_number,'')),',,',','),',,',',') as tracking_number_list_setup,
		group_concat(ifnull(delivery_status,'')) as delivery_status_list,
		group_concat(ifnull(shipping_carrier,'')) as shipping_carrier_list,
        group_concat(ifnull(external_fulfillment_status,'')) as external_fulfillment_status_list,
        group_concat(ifnull(dc,'')) as dc_list
	from sp.fulf_clean_1_live 
	group by order_id 
	;
drop table if exists sp.fulf_order_level_live ;
create table sp.fulf_order_level_live
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
	from sp.fulf_order_level_live_setup
    order by order_id desc
    ;    
    select * from sp.fulf_order_level_live ;
    select distinct dc_list from sp.fulf_order_level_live order by dc_list ;



/* COMPARE NEW INCOMING RAW TO EXISTING */
drop table if exists sp.fulf_new_vs_existing ;
create table sp.fulf_new_vs_existing
	select
		newnew.*,
		ex.tracking_number_count as tracking_number_count_existing,
        ex.tracking_number_list as tracking_number_list_existing,
        ex.external_fulfillment_status_list as external_fulfillment_status_list_existing
	from sp.fulf_order_level_live as newnew
    left join cadata.fulf_order_level_existing as ex
    on newnew.order_id=ex.order_id
	;
    
	select * from sp.fulf_order_level_live ;
	select * from sp.fulf_new_vs_existing ;



/* CREATE SUBSET OF ORDER_ID THAT NEED UPDATE EMAIL TO CUSTOMER */
/* ORDER IS WITHIN 30 DAYS, AT LEAST 1 TRACKING NUMBER, AND TRACKING NUMBER COUNT INCREASED */
drop table if exists cadata.fulf_detector_output_multitracks ;
create table cadata.fulf_detector_output_multitracks
	select *
	from sp.fulf_new_vs_existing
    where 
		tracking_number_count_existing >= 1 and
        tracking_number_count > tracking_number_count_existing and
        created_date_utc_min >= date(now() - interval @lookback_days day)
	order by order_id
	;
    select * from cadata.fulf_detector_output_multitracks ;



/* CREATE SUBSET OF ORDER_ID FOR INTERNAL EMAIL */
/* UNSHIPPED IN X # DAYS DEFINED BY SHIPWARNING_DAYS */
drop table if exists cadata.fulf_detector_output_unshipped_warning ;
create table cadata.fulf_detector_output_unshipped_warning
	select *
	from sp.fulf_new_vs_existing
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
	select * from cadata.fulf_detector_output_unshipped_warning ;
/* CHANGE OF FULFILLMENT STATUS TO FAIL FROM ANOTHER STATE, WITHIN PAST X # DAYS, SET BY SHIPWARNING_DAYS */
drop table if exists cadata.fulf_detector_output_fail_warning ;
create table cadata.fulf_detector_output_fail_warning
	select *
	from sp.fulf_new_vs_existing
    where 
		locate('fail',lcase(external_fulfillment_status_list_existing))=0 and locate('fail',lcase(external_fulfillment_status_list))>0 and
        created_date_utc_min >= date(now() - interval @shipwarning_days day)
	;
	select * from cadata.fulf_detector_output_fail_warning ;
 

/* SAVE UPDATED DB AS EXISTING */
drop table if exists cadata.fulf_order_level_existing ;
create table cadata.fulf_order_level_existing
	select *
	from sp.fulf_order_level_live
    ;












-- drop database sp ;


