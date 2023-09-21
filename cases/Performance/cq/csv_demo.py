###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import os
from taostest.util.common import TDCom
from taostest import TDCase
from taostest.util.remote import Remote
import random
import pandas as pd
from dateutil import parser
import shutil

class BugTS3311(TDCase):
    def init(self):
        self._remote: Remote = Remote(self.logger)
        self.tdCom = TDCom(self.tdSql)
        self.dbname = "history_quote"
        self.source_csv_path = "/root/2022/source"
        self.ns_csv_path = "/root/2022/ns"
        self.new_csv_path = "/root/2022/new"
        self.csv_list = os.listdir(self.source_csv_path)
        self.stbname = "t_tick_order_template"
        self.thread_count = 16
        self.need_generate_ns_file = False
        self.clean_db = True
        self.continue_insert = False
        
    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def prepare(self):
        self.tdSql.execute('drop database if exists history_quote;')
        self.tdSql.execute('create database if not exists history_quote precision "ns" KEEP 18000 DAYS 10 replica 2  BLOCKS 12 UPDATE 1;')
        self.tdSql.execute('CREATE STABLE history_quote.t_tick_order_template(orig_time TIMESTAMP , hash BIGINT , appl_seq_num BIGINT , channel_no INT, order_time BIGINT , order_price BIGINT , order_volume BIGINT , side BINARY(1) , order_type BINARY(1) , md_stream_id BINARY(6) , orig_order_no BIGINT, product_status BINARY(8), biz_index BIGINT , biz_type INT) TAGS(market int,security_code binary(16));  ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_level2_snapshot_template (orig_time TIMESTAMP,hash BIGINT, trading_phase_code BINARY (8), channel_no INT, md_stream_id BINARY (6), pre_close_price BIGINT, open_price BIGINT, high_price BIGINT, low_price BIGINT, last_price BIGINT, close_price BIGINT, bid_price BINARY(168), bid_volume BINARY(168), offer_price BINARY(168), offer_volume BINARY(168), num_trades BIGINT, total_volume_trade BIGINT, total_value_trade BIGINT, total_bid_volume BIGINT, total_offer_volume BIGINT, weighted_avg_bid_price BIGINT, weighted_avg_offer_price BIGINT,  iopv BIGINT, high_limited BIGINT, low_limited BIGINT, last_trade_time BIGINT, biz_type INT) TAGS(market int, security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_index_snapshot_template (orig_time TIMESTAMP,  hash BIGINT,channel_no INT, md_stream_id BINARY(6), trading_phase_code BINARY(8), pre_close_index BIGINT, open_index BIGINT, high_index BIGINT, low_index BIGINT, last_index BIGINT, close_index BIGINT, total_volume_trade BIGINT, total_value_trade BIGINT,biz_type INT) TAGS(market int,security_code binary(16));')
        self.tdSql.execute('CREATE STABLE history_quote.t_option_snapshot_template(orig_time TIMESTAMP , hash BIGINT, channel_no INT, md_stream_id BINARY(6), trading_phase_code BINARY(8) , total_long_position BIGINT , total_volume_trade BIGINT , total_value_trade BIGINT , pre_settle_price BIGINT , pre_close_price BIGINT , open_price BIGINT , auction_price BIGINT , auction_volume BIGINT , high_price BIGINT , low_price BIGINT , last_price BIGINT , close_price BIGINT , high_limited BIGINT , low_limited BIGINT , bid_price BINARY(168) , bid_volume BINARY(168), offer_price BINARY(168) , offer_volume BINARY(168) , settle_price BIGINT , last_trade_time BIGINT, ref_price BIGINT,biz_type INT) TAGS(market int,security_code binary(16));')
        self.tdSql.execute('CREATE STABLE history_quote.t_hkt_snapshot_template (orig_time TIMESTAMP, hash BIGINT, total_volume_trade BIGINT, total_value_trade BIGINT, pre_close_price BIGINT, nominal_price BIGINT, high_price BIGINT, low_price BIGINT, last_price BIGINT, bid_price BINARY(168), bid_volume BINARY(168), offer_price BINARY(168), offer_volume BINARY(168), trading_phase_code BINARY(8), channel_no INT, ref_price BIGINT, high_limited BIGINT, low_limited BIGINT, bid_price_limit_up BIGINT, bid_price_limit_down BIGINT, offer_price_limit_up BIGINT, offer_price_limit_down BIGINT, md_stream_id BINARY(6) ,biz_type INT) TAGS(market int,security_code binary(16));  ')
        self.tdSql.execute('CREATE STABLE history_quote.t_tick_execution_template(orig_time TIMESTAMP , hash BIGINT ,  appl_seq_num BIGINT , channel_no INT , exec_time BIGINT, exec_price BIGINT , exec_volume BIGINT , value_trade BIGINT , bid_appl_seq_num BIGINT , offer_appl_seq_num BIGINT , side BINARY(1) , exec_type BINARY(1) , md_stream_id BINARY(6) , biz_index BIGINT ,biz_type INT) TAGS(market int,security_code binary(16));  ')
        self.tdSql.execute('CREATE STABLE history_quote.t_tick_orderqueue_template(orig_time TIMESTAMP , hash BIGINT, order_time BIGINT , side BINARY(1) , order_price BIGINT , order_volume BIGINT , num_of_orders BIGINT , items BIGINT , item_volume BINARY(1024), channel_no INT , md_stream_id BINARY(6)) TAGS(market int,security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_future_level1_snapshot_template(orig_time TIMESTAMP , hash BIGINT, action_day INT,  exchange_inst_id BINARY(32) , last_price BIGINT , pre_settle_price BIGINT , pre_close_price BIGINT , pre_open_interest BIGINT ,  open_price BIGINT , high_price BIGINT , low_price  BIGINT , total_volume_trade BIGINT , total_value_trade BIGINT , open_interest BIGINT , close_price BIGINT , settle_price BIGINT , high_limited BIGINT , low_limited BIGINT , pre_delta BIGINT , curr_delta BIGINT , bid_price BINARY(168) , bid_volume BINARY(168) , offer_price BINARY(168) , offer_volume BINARY(168) , average_price BIGINT , trading_day INT ,biz_type INT) TAGS(market int,security_code binary(16));  ')
        self.tdSql.execute('CREATE STABLE history_quote.t_future_level2_snapshot_template(orig_time TIMESTAMP , hash BIGINT, action_day INT,  exchange_inst_id BINARY(32) , last_price BIGINT , pre_settle_price BIGINT , pre_close_price BIGINT , pre_open_interest BIGINT ,  open_price BIGINT , high_price BIGINT , low_price  BIGINT , total_volume_trade BIGINT , total_value_trade BIGINT , open_interest BIGINT , close_price BIGINT , settle_price BIGINT , high_limited BIGINT , low_limited BIGINT , pre_delta BIGINT , curr_delta BIGINT , bid_price BINARY(168) , bid_volume BINARY(168) , offer_price BINARY(168) , offer_volume BINARY(168) , average_price BIGINT , trading_day INT,biz_type INT) TAGS(market int,security_code binary(16));  ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_1min_kline_template(orig_time TIMESTAMP ,  kline_time BIGINT , open_price BIGINT , high_price BIGINT , low_price BIGINT , close_price BIGINT , volume_trade BIGINT, value_trade BIGINT, total_volume_trade BIGINT, total_value_trade BIGINT ) TAGS(market int,security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_3min_kline_template(orig_time TIMESTAMP ,  kline_time BIGINT , open_price BIGINT , high_price BIGINT , low_price BIGINT , close_price BIGINT , volume_trade BIGINT, value_trade BIGINT, total_volume_trade BIGINT, total_value_trade BIGINT ) TAGS(market int,security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_5min_kline_template(orig_time TIMESTAMP ,  kline_time BIGINT , open_price BIGINT , high_price BIGINT , low_price BIGINT , close_price BIGINT , volume_trade BIGINT, value_trade BIGINT, total_volume_trade BIGINT, total_value_trade BIGINT ) TAGS(market int,security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_10min_kline_template(orig_time TIMESTAMP ,  kline_time BIGINT , open_price BIGINT , high_price BIGINT , low_price BIGINT , close_price BIGINT , volume_trade BIGINT, value_trade BIGINT, total_volume_trade BIGINT, total_value_trade BIGINT ) TAGS(market int,security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_15min_kline_template(orig_time TIMESTAMP ,  kline_time BIGINT , open_price BIGINT , high_price BIGINT , low_price BIGINT , close_price BIGINT , volume_trade BIGINT, value_trade BIGINT, total_volume_trade BIGINT, total_value_trade BIGINT ) TAGS(market int,security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_30min_kline_template(orig_time TIMESTAMP ,  kline_time BIGINT , open_price BIGINT , high_price BIGINT , low_price BIGINT , close_price BIGINT , volume_trade BIGINT, value_trade BIGINT, total_volume_trade BIGINT, total_value_trade BIGINT ) TAGS(market int,security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_60min_kline_template(orig_time TIMESTAMP ,  kline_time BIGINT , open_price BIGINT , high_price BIGINT , low_price BIGINT , close_price BIGINT , volume_trade BIGINT, value_trade BIGINT, total_volume_trade BIGINT, total_value_trade BIGINT ) TAGS(market int,security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_120min_kline_template(orig_time TIMESTAMP ,  kline_time BIGINT , open_price BIGINT , high_price BIGINT , low_price BIGINT , close_price BIGINT , volume_trade BIGINT, value_trade BIGINT, total_volume_trade BIGINT, total_value_trade BIGINT ) TAGS(market int,security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_day_kline_template(orig_time TIMESTAMP ,  kline_time BIGINT , open_price BIGINT , high_price BIGINT , low_price BIGINT , close_price BIGINT , volume_trade BIGINT, value_trade BIGINT ) TAGS(market int,security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_week_kline(orig_time TIMESTAMP ,  kline_time BIGINT , open_price BIGINT , high_price BIGINT , low_price BIGINT , close_price BIGINT , volume_trade BIGINT, value_trade BIGINT ) TAGS(market int,security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_month_kline(orig_time TIMESTAMP ,  kline_time BIGINT , open_price BIGINT , high_price BIGINT , low_price BIGINT , close_price BIGINT , volume_trade BIGINT, value_trade BIGINT ) TAGS(market int,security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_season_kline(orig_time TIMESTAMP ,  kline_time BIGINT , open_price BIGINT , high_price BIGINT , low_price BIGINT , close_price BIGINT , volume_trade BIGINT, value_trade BIGINT ) TAGS(market int,security_code binary(16)); ')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_year_kline(orig_time TIMESTAMP ,  kline_time BIGINT , open_price BIGINT , high_price BIGINT , low_price BIGINT , close_price BIGINT , volume_trade BIGINT, value_trade BIGINT ) TAGS(market int,security_code binary(16));')
        self.tdSql.execute('CREATE STABLE history_quote.t_stock_factor_template (orig_time TIMESTAMP ,  factor_name BINARY(64) , key1 BINARY(64) , key2 BINARY(64) , seq_num BIGINT , data NCHAR(1024) ) TAGS(actor_type BINARY(64),factor_sub_type BINARY(64));')
        
    def remove_first_line(self, file_path):
        with open(file_path, "r") as f:
            lines = f.readlines()
        lines = lines[1:]
        with open(file_path, "w") as f:
            f.writelines(lines)
        
    def trans_date_to_ns(self, date_list):
        n_l = list()
        for date in date_list:
            d0 = date.split(".")[0]
            d1 = date.split(".")[1].replace("'", "")
            ns_ts = int(parser.parse(d0).timestamp()) * 10**9 + int(f"{d1}".lstrip("0"))
            n_l.append(ns_ts)
        return n_l
    
    def gen_ns_file(self, csv_file):
        # for csv_file in self.csv_list:
        target_file = f'{self.ns_csv_path}/{csv_file}'
        update_target_file = f'{self.new_csv_path}/{csv_file}'
        if not os.path.exists(target_file):
            self._remote._logger.info(f'********** Generating {csv_file} **********')
            df = pd.read_csv(f'{self.source_csv_path}/{csv_file}', header=None)
            od = df.iloc[:, 0]
            nd = self.trans_date_to_ns(od)
            df.iloc[:, 0] = nd
            df.to_csv(target_file, header=False, index=False)
            df.to_csv(update_target_file, header=False, index=False)
    
    def gen_new_insert_file(self):
        for csv_file in self.csv_list:
            target_file = f'{self.new_csv_path}/{csv_file}'
            self._remote._logger.info(f'********** Generating {csv_file} ********** ')
            df = pd.read_csv(target_file, header=None)
            od = df.iloc[:, 0]
            ts_offset = df.iloc[0, 0] - df.iloc[-1, 0]
            nd = list(map(lambda x:x + ts_offset, od))
            df.iloc[:, 0] = nd
            df.to_csv(target_file, header=False, index=False)
    
    def insert_base(self, csv_name):
        self._remote._logger.info(f'********** Inserting base data into {csv_name} **********')
        tbname = csv_name.split(".")[0]
        self.tdSql.execute(f'insert into {self.dbname}.{tbname} using {self.dbname}.{self.stbname} tags (1, "test") file "{self.ns_csv_path}/{csv_name}";')
    
    def append_insert(self, csv_name):
        self._remote._logger.info(f'********** Inserting append data into {csv_name} **********')
        tbname = csv_name.split(".")[0]
        self.tdSql.execute(f'insert into {self.dbname}.{tbname} using {self.dbname}.{self.stbname} tags (1, "test") file "{self.new_csv_path}/{csv_name}";')

    def delete_rows(self, csv_name):
        tbname = csv_name.split(".")[0]
        self.tdSql.execute(f'delete from {self.dbname}.{tbname};')

    def thread_gen_ns_file(self, thread_count):
        self._remote._logger.info('********** Threading gen ns file **********')
        self.tdCom.thread_pool(self.csv_list, self.gen_ns_file, thread_count)

    def thread_delete_rows(self, thread_count):
        delete_list = random.sample(self.csv_list, int(len(self.csv_list)/2))
        self.tdCom.thread_pool(self.csv_list, self.delete_rows, thread_count)

    def thread_insert_base(self, thread_count):
        self._remote._logger.info('********** Threading insert base data **********')
        self.tdCom.thread_pool(self.csv_list, self.insert_base, thread_count)
        
    def thread_append_insert(self, thread_count):
        self._remote._logger.info('********** Threading insert append data **********')
        self.tdCom.thread_pool(self.csv_list, self.append_insert, thread_count)

    def init_test(self):
        if not os.path.exists(self.ns_csv_path):
            os.makedirs(self.ns_csv_path)
        if not self.continue_insert:
            if os.path.exists(self.new_csv_path):
                shutil.rmtree(self.new_csv_path)
        if self.need_generate_ns_file:
            self.thread_gen_ns_file(self.thread_count)
        if self.clean_db:
            self.prepare()
            self.thread_insert_base(self.thread_count)
        
    def run(self):
        self.init_test()
        if not self.continue_insert:
            self._remote._logger.info(f'********** copy {self.ns_csv_path} to {self.new_csv_path} **********')
            shutil.copytree(self.ns_csv_path, self.new_csv_path)
        for i in range(200):
            self._remote._logger.info(f'********** Appending insert range: {i} **********')
            self.gen_new_insert_file()
            self.thread_append_insert(self.thread_count)