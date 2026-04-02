---sql1---
drop stream test_stb_sxny_cn_sbgjpt_index_drzfdl_v4 ;
drop table ctg_res_db.test_stb_sxny_cn_sbgjpt_index_drzfdl_v4;
---sql2---
drop stream str_tb_station_power_info ;
drop table ctg_res_db.stb_station_power_info;
---sql3---
drop stream str_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj1 ;
drop table ctg_res_db.stb_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj1;
---sql4---
drop stream str_hbny_sx_mint_jzzt2;
drop table ctg_res_db.stb_hbny_sx_mint_jzzt2;
---sql5---
drop stream stm_dwi_sxny_snestation_data_power;
drop table ctg_res_db.stb_dwi_sxny_snestation_data_power;
---sql6---
drop stream stm_dwi_hbny_sx_mint_power;
drop table ctg_res_db.stb_dwi_hbny_sx_mint_power;
---sql7---
drop stream stm_dwi_cjdl_rtems_power;
drop table ctg_res_db.stb_dwi_cjdl_rtems_power;
---sql8---
drop stream stm_dwi_hbny_sx_mint_unit_power;
drop table ctg_res_db.stb_dwi_hbny_sx_mint_unit_power;
---sql9---
drop stream stm_dwi_cjdl_rtems_unit_power;
drop table ctg_res_db.stb_dwi_cjdl_rtems_unit_power;
---sql10---
drop stream str_stb_sxny_cn_all_cz_yggl_base;
drop table ctg_res_db.stb_sxny_cn_all_cz_yggl_base;
---sql11---
drop stream str_stb_sxny_cn_all_cz_yggl_base1;
drop table ctg_res_db.stb_sxny_cn_all_cz_yggl_base1;
---sql12---
drop stream str_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj;
drop table ctg_res_db.stb_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj;
---sql13---
drop stream str_cjdl_rtdb_jzzt2;
drop table ctg_res_db.stb_cjdl_rtdb_jzzt2;
---sql14---
drop stream stb_sxny_cn_sbgjpt_index_blq_yjbj;
drop table ctg_res_db.stb_sxny_cn_sbgjpt_index_blq_yjbj;
---sql15---
drop stream stm_sxny_cn_power_all_cz_yggl;
drop table ctg_res_db.stb_sxny_cn_all_cz_yggl;






---sql1---
select * from ctg_res_db.test_stb_sxny_cn_sbgjpt_index_drzfdl_v4;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='test_stb_sxny_cn_sbgjpt_index_drzfdl_v4';
---sql2---
select * from ctg_res_db.stb_station_power_info;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='str_tb_station_power_info';
---sql3---
select * from ctg_res_db.stb_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj1;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='str_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj1';

---sql4---
select * from ctg_res_db.stb_hbny_sx_mint_jzzt2;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='str_hbny_sx_mint_jzzt2';
---sql5---
select * from ctg_res_db.stb_dwi_sxny_snestation_data_power;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='stm_dwi_sxny_snestation_data_power';
---sql6---
select * from ctg_res_db.stb_dwi_hbny_sx_mint_power;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='stm_dwi_hbny_sx_mint_power';
---sql7---
select * from ctg_res_db.stb_dwi_cjdl_rtems_power;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='stm_dwi_cjdl_rtems_power';
---sql8---
select * from ctg_res_db.stb_dwi_hbny_sx_mint_unit_power;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='stm_dwi_hbny_sx_mint_unit_power';
---sql9---
select * from ctg_res_db.stb_dwi_cjdl_rtems_unit_power;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='stm_dwi_cjdl_rtems_unit_power';
---sql10---
select * from ctg_res_db.stb_sxny_cn_all_cz_yggl_base;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='str_stb_sxny_cn_all_cz_yggl_base';
---sql11---
select * from ctg_res_db.stb_sxny_cn_all_cz_yggl_base1;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='str_stb_sxny_cn_all_cz_yggl_base1';
---sql12---
select * from ctg_res_db.stb_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='str_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj';
---sql13---
select * from ctg_res_db.stb_cjdl_rtdb_jzzt2;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='str_cjdl_rtdb_jzzt2';
---sql14---
select * from ctg_res_db.stb_sxny_cn_sbgjpt_index_blq_yjbj;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='stb_sxny_cn_sbgjpt_index_blq_yjbj';
---sql15---
select * from ctg_res_db.stb_sxny_cn_all_cz_yggl;
select status,checkpoint_time,start_time from information_schema.ins_stream_tasks where stream_name='stm_sxny_cn_power_all_cz_yggl';



---sql1---
select * from ctg_res_db.test_stb_sxny_cn_sbgjpt_index_drzfdl_v4 limit 1;
select * from ctg_res_db.stb_station_power_info limit 2;
select * from ctg_res_db.stb_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj1 limit 3;
select * from ctg_res_db.stb_hbny_sx_mint_jzzt2 limit 4;
select * from ctg_res_db.stb_dwi_sxny_snestation_data_power limit 5;
select * from ctg_res_db.stb_dwi_hbny_sx_mint_power limit 6;
select * from ctg_res_db.stb_dwi_cjdl_rtems_power limit 7;
select * from ctg_res_db.stb_dwi_hbny_sx_mint_unit_power limit 8;
select * from ctg_res_db.stb_dwi_cjdl_rtems_unit_power limit 9;
select * from ctg_res_db.stb_sxny_cn_all_cz_yggl_base limit 10;
select * from ctg_res_db.stb_sxny_cn_all_cz_yggl_base1 limit 11;
select * from ctg_res_db.stb_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj limit 12;
select * from ctg_res_db.stb_cjdl_rtdb_jzzt2 limit 13;
select * from ctg_res_db.stb_sxny_cn_sbgjpt_index_blq_yjbj limit 14;
select * from ctg_res_db.stb_sxny_cn_all_cz_yggl limit 15;


select count(*) from ctg_res_db.test_stb_sxny_cn_sbgjpt_index_drzfdl_v4 limit 1;
select count(*) from ctg_res_db.stb_station_power_info limit 2;
select count(*) from ctg_res_db.stb_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj1 limit 3;
select count(*) from ctg_res_db.stb_hbny_sx_mint_jzzt2 limit 4;
select count(*) from ctg_res_db.stb_dwi_sxny_snestation_data_power limit 5;
select count(*) from ctg_res_db.stb_dwi_hbny_sx_mint_power limit 6;
select count(*) from ctg_res_db.stb_dwi_cjdl_rtems_power limit 7;
select count(*) from ctg_res_db.stb_dwi_hbny_sx_mint_unit_power limit 8;
select count(*) from ctg_res_db.stb_dwi_cjdl_rtems_unit_power limit 9;
select count(*) from ctg_res_db.stb_sxny_cn_all_cz_yggl_base limit 10;
select count(*) from ctg_res_db.stb_sxny_cn_all_cz_yggl_base1 limit 11;
select count(*) from ctg_res_db.stb_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj limit 12;
select count(*) from ctg_res_db.stb_cjdl_rtdb_jzzt2 limit 13;
select count(*) from ctg_res_db.stb_sxny_cn_sbgjpt_index_blq_yjbj limit 14;
select count(*) from ctg_res_db.stb_sxny_cn_all_cz_yggl limit 15;



select count(*) from ctg_res_db.stb_dwi_cjdl_rtems_power limit 7;
select count(*) from ctg_res_db.stb_sxny_cn_all_cz_yggl_base limit 10;
select count(*) from ctg_res_db.stb_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj limit 12;
select count(*) from ctg_res_db.stb_cjdl_rtdb_jzzt2 limit 13;

select count(*) from ctg_tsdb.stb_sxny_cn;
select count(*) from ctg_tsdb.stb_sxny_cn_sbgjpt_stationmsg_cnstationstatus_bj1;
select count(*) from ctg_tsdb.tb_station_power_info;
select count(*) from ctg_tsdb.stb_sxny_snestation_data;
select count(*) from ctg_tsdb.stb_cjdl_rtdb;
select count(*) from ctg_tsdb.stb_cjdl_rtems;
select count(*) from ctg_tsdb.stb_sxny_cn_all_cz_yggl_base1;
select count(*) from ctg_tsdb.stb_hbny_sx_mint;

select status,info,in_queue, task_id, stream_name from information_schema.ins_stream_tasks;