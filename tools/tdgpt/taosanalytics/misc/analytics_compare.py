# encoding:utf-8
# pylint: disable=c0103,c0413

""" do perform analysis comparsion based on data in TDengine """

import sys, os

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

import configparser
import json
import time
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error
import xlsxwriter

import taos
from taosanalytics.servicemgmt import loader


def symmetric_mean_absolute_percentage_error(y_true, y_pred) -> float:
    y_true_np, y_pred_np = np.array(y_true), np.array(y_pred)
    return 2 * 100 * np.mean(np.abs((y_true_np - y_pred_np) / (np.abs(y_true_np) + np.abs(y_pred_np))))


class AlgoParams:
    """ the parameter class for execute forecast """

    def __init__(self, skey, ekey, fc_rows=10):
        self.skey = skey
        self.ekey = ekey
        self.fc_rows = fc_rows


class DataLoader:
    """ load data from TDengine """

    def __init__(self):
        self.host = None
        self.user = None
        self.passwd = None
        self.conf = None
        self.tz = None
        self.db = None
        self.table_name = None
        self.col_name = None
        self.res_start_ts = 0
        self.fc_algos = None

        self.ad_algos = None
        self.anno_res = None

        self.fc_start_time = None
        self.fc_end_time = None
        self.ad_start_time = None
        self.ad_end_time = None

        self.workbook = None
        self.worksheet = None

        conf = configparser.ConfigParser()
        conf.read('./analytics.ini')

        if conf.has_option('taosd', 'host'):
            self.host = conf.get('taosd', 'host')

        if conf.has_option('taosd', 'user'):
            self.user = conf.get('taosd', 'user')

        if conf.has_option('taosd', 'password'):
            self.passwd = conf.get('taosd', 'password')

        if conf.has_option('taosd', 'conf'):
            self.conf = conf.get('taosd', 'conf')

        if conf.has_option('input_data', 'db_name'):
            self.db = conf.get('input_data', 'db_name')

        if conf.has_option('input_data', 'table_name'):
            self.table_name = conf.get('input_data', 'table_name')

        if conf.has_option('input_data', 'column_name'):
            self.col_name = conf.get('input_data', 'column_name')

        if conf.has_option('forecast', 'rows'):
            self.rows = int(conf.get('forecast', 'rows'))

        if conf.has_option('forecast', 'res_start_time'):
            self.res_start_ts = conf.get('forecast', 'res_start_time')

        if conf.has_option('forecast', 'start_time'):
            self.fc_start_time = conf.get('forecast', 'start_time')

        if conf.has_option('forecast', 'end_time'):
            self.fc_end_time = conf.get('forecast', 'end_time')

        if conf.has_option('forecast', 'period'):
            self.period = conf.get('forecast', 'period')

        if conf.has_option('ad', 'start_time'):
            self.ad_start_time = conf.get('ad', 'start_time')

        if conf.has_option('ad', 'end_time'):
            self.ad_end_time = conf.get('ad', 'end_time')

        if conf.has_option('ad', 'anno_res'):
            res = conf.get('ad', 'anno_res')
            res = res.replace('[', '').replace(']', '').split(',')
            self.anno_res = [int(item) for item in res]

        if conf.has_section('forecast.algos'):
            self.fc_algos = conf['forecast.algos']

        if conf.has_section('ad.algos'):
            self.ad_algos = conf['ad.algos']

    def set_conn_params(self, host, user, passwd, conf, tz):
        """
        Set the connection parameters for the database.

        Parameters:
        - host: The hostname or IP address of the database server.
        - user: The username used to authenticate with the database server.
        - passwd: The password used to authenticate with the database server.
        - conf: Additional configuration options for the database connection.
        - tz: The timezone to be used for the database connection.

        This method sets the instance variables `host`, `user`, `passwd`, `conf`, and `tz` with the provided parameters.
        """
        self.host = host
        self.user = user

        self.passwd = passwd
        self.conf = conf
        self.tz = tz

    def load_n_forecast(self):
        """ load data from TDengine """
        conn = taos.connect(
            host=self.host, user=self.user, password=self.passwd, config=self.conf, timezone=self.tz
        )

        cursor = conn.cursor()

        sql = "use " + self.db
        cursor.execute(sql)

        sql = ("select {} from {} where '{}' <= _c0 and _c0 <= '{}'".
               format(self.col_name, self.table_name, self.fc_start_time, self.fc_end_time))

        cursor.execute(sql)

        input_list = []
        for row in cursor:
            print(row)
            input_list.append(row[0])

        if len(input_list) < self.rows:
            raise ValueError(f"the number of input data is less than {self.rows}")

        conn.close()

        loader.load_all_service()

        self.open_fc_file()
        rowIndex = 1

        for key, value in self.fc_algos.items():
            s = loader.get_service(key)
            if s is None:
                print(f"{key} algorithm is not valid, ignore and continue")
                continue

            # the last fc_num is used to validate the forecast results
            eval_list = input_list[-self.rows:]

            try:
                params = json.loads(value)
            except ValueError as e:
                print(f"failed to parse params for algo:{key}, {e}")
                continue

            s.set_input_list(input_list[:-self.rows], None)

            p = {"rows": self.rows, "start_ts": self.res_start_ts, "period": self.period, "time_step": 86400 * 30}
            p.update(params)

            try:
                s.set_params(p)
            except ValueError as e:
                print(f"failed to set params for algo:{key}, {e}")
                continue

            start = time.time()

            try:
                r = s.execute()
            except Exception as e:
                print(f"failed to execute algo:{key}, {e}")
                continue

            end = time.time()
            print("elapsed time: ", (end - start) * 1000)

            # Calculate MSE of predicate
            mse, mape, smape = (mean_squared_error(eval_list, r["res"][1]),
                                mean_absolute_percentage_error(eval_list, r["res"][1]),
                                symmetric_mean_absolute_percentage_error(eval_list, r["res"][1]));

            print(f"The mse, mape, smape of predicate is: {mse} {mape} {smape}")

            self.do_draw_fc_results(input_list, len(r["res"]) > 2, r["res"], self.rows, key)
            self.write_fc_results(rowIndex, key, value, mse, mape, smape,f'./{key}.png', elapsed_time=(end - start) * 1000)
            rowIndex += 1

        self.close_fc_file()

    def load_n_ad(self):
        """ load data from TDengine """
        conn = taos.connect(
            host=self.host, user=self.user, password=self.passwd, config=self.conf, timezone=self.tz
        )

        cursor = conn.cursor()

        sql = "use " + self.db
        cursor.execute(sql)

        sql = ("select {} from {} where '{}' <= _c0 and _c0 <= '{}'".format(self.col_name, self.table_name,
                                                                            self.ad_start_time, self.ad_end_time))

        cursor.execute(sql)

        input_list = []
        for row in cursor:
            input_list.append(row[0])

        conn.close()

        loader.load_all_service()

        self.open_ad_file()

        rowIndex = 1
        for key, value in self.ad_algos.items():
            s = loader.get_service(key)
            if s is None:
                print(f"{key} algorithm is not valid, ignore and continue")
                continue

            try:
                params = json.loads(value)
            except ValueError as e:
                print(f"failed to parse params for algo:{key}, {e}")
                continue

            s.set_input_list(input_list, None)

            start = time.time()
            try:
                s.set_params(params)
            except ValueError as e:
                print(f"failed to set params for algo:{key}, {e}")
                continue

            try:
                r = s.execute()
            except Exception as e:
                print(f"failed to execute algo:{key}, {e}")
                continue

            end = time.time()

            self.do_draw_ad_results(input_list, r, key)
            pre, recall = self.do_calculate_precision_recall(self.anno_res, r, key)

            self.write_ad_results(rowIndex, key, value, pre, recall, f'./{key}.png', elapsed_time=(end - start) * 1000)
            rowIndex += 1

        self.close_ad_file()

    def open_ad_file(self):

        self.workbook = xlsxwriter.Workbook('ad_results.xlsx')
        self.worksheet = self.workbook.add_worksheet()

        col_title = ['algorithm', 'params', 'precision(%)', 'recall(%)',
                     'elapsed_time(ms.)']

        for col_num, data in enumerate(col_title):
            self.worksheet.write(0, col_num, data)


    def write_ad_results(self, row, name, param, precision, recall, result_img,
                         elapsed_time):
        self.worksheet.write(row, 0, name)
        self.worksheet.write(row, 1, param)
        self.worksheet.write(row, 2, precision)
        self.worksheet.write(row, 3, recall)
        self.worksheet.write(row, 4, elapsed_time)

        ws = self.workbook.add_worksheet(name)
        ws.insert_image(0, 0, result_img)

    def close_ad_file(self):
        self.workbook.close()

    def do_draw_fc_results(self, input_list, return_conf, fc, n_rows, fig_name):
        """ visualize the forecast results """
        plt.clf()

        training_list = input_list[:-n_rows]
        x = np.arange(len(training_list), len(training_list) + n_rows, 1)

        # draw the range of conf
        if return_conf:
            lower_series = pd.Series(fc[2], index=x)
            upper_series = pd.Series(fc[3], index=x)

            plt.fill_between(lower_series.index, lower_series, upper_series, color='k', alpha=.15)

        # draw the training data
        plt.plot(input_list[:-n_rows], c='green')

        # draw the true data for evaluating
        plt.plot(x, input_list[-n_rows:], c='blue')

        # draw the predicate value
        plt.plot(x, fc[1], c='red')
        plt.savefig(fig_name)

    def do_draw_ad_results(self, input_list, res, fig_name):
        """ visualize the anomaly detection results """
        plt.clf()

        # draw the original data
        plt.plot(input_list, c='green')

        for index, val in enumerate(res):
            if val != -1:
                continue
            plt.scatter(index, input_list[index], marker='o', color='r', alpha=0.5, s=100, zorder=3)

        plt.savefig(fig_name)

    def do_calculate_precision_recall(self, annotated_res, res, name):
        hit = 0
        total = 0

        for index, val in enumerate(res):
            if val != -1:
                continue

            total += 1
            if index in annotated_res:
                hit += 1

        p = 0
        if total == 0:
            p = 0.0
        else:
            p = hit * 100.0 / total
        recall = hit * 100.0 / len(annotated_res)

        return p, recall

    def open_fc_file(self):
        self.workbook = xlsxwriter.Workbook('fc_results.xlsx')
        self.worksheet = self.workbook.add_worksheet()
        col_title = ['algorithm', 'params', 'mse', 'mape', 'smape', 'elapsed_time(ms.)']
        for col_num, data in enumerate(col_title):
            self.worksheet.write(0, col_num, data)


    def write_fc_results(self, row, name, param, mse, mape, smape, result_img, elapsed_time):
        self.worksheet.write(row, 0, name)
        self.worksheet.write(row, 1, param)
        self.worksheet.write(row, 2, mse)
        self.worksheet.write(row, 3, mape)
        self.worksheet.write(row, 4, smape)
        self.worksheet.write(row, 5, elapsed_time)

        ws = self.workbook.add_worksheet(name)
        ws.insert_image(0, 0, result_img)

    def close_fc_file(self):
        self.workbook.close()

if __name__ == "__main__":
    d_loader = DataLoader()

    if len(sys.argv) < 2:
        print("please input the mode, forecast or anomaly-detection")
        exit(1)

    if sys.argv[1] == 'forecast':
        d_loader.load_n_forecast()
    elif sys.argv[1] == 'anomaly-detection':
        d_loader.load_n_ad()
    else:
        print("please input the mode, forecast or anomaly-detection")
