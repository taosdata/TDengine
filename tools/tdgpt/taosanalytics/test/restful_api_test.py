# encoding:utf-8
# pylint: disable=c0103
"""flask restful api test module"""
import math
import sys, os.path

import numpy as np

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../")

from flask_testing import TestCase
from taosanalytics.app import app
from taosanalytics.conf import setup_log_info


class RestfulTest(TestCase):
    """ restful api test class """

    def create_app(self):
        app.testing = True
        setup_log_info("restfull_test.log")
        return app

    def test_access_main_page(self):
        """ test asscess default main page """
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content_length,
                         len("TDgpt - TDengine TSDB© Time-Series Data Analytics Platform (ver 3.3.7.1)") + 1)

    def test_load_status(self):
        """ test load the server status """
        response = self.client.get('/status')
        self.assertEqual(response.status_code, 200)
        res = response.json

        self.assertEqual(res['protocol'], 1.0)
        self.assertEqual(res['status'], 'ready')

    def test_load_algos(self):
        """ test load provided algos"""
        response = self.client.get('/list')
        self.assertEqual(response.status_code, 200)

        res = response.json
        self.assertEqual(res['version'], 0.1)
        self.assertEqual(res['protocol'], 1.0)

        d = res['details']
        self.assertEqual(len(d), 4)

    def test_forecast(self):
        """test forecast api"""
        response = self.client.post("/forecast", json={
            "schema": [
                ["ts", "TIMESTAMP", 8],
                ["val", "INT", 4]
            ],
            "data": [
                [
                    1577808000000, 1577808001000, 1577808002000, 1577808003000, 1577808004000,
                    1577808005000, 1577808006000, 1577808007000, 1577808008000, 1577808009000,
                    1577808010000, 1577808011000, 1577808012000, 1577808013000, 1577808014000,
                    1577808015000, 1577808016000, 1577808017000, 1577808018000, 1577808019000,
                    1577808020000, 1577808021000, 1577808022000, 1577808023000, 1577808024000,
                    1577808025000, 1577808026000, 1577808027000, 1577808028000, 1577808029000,
                    1577808030000, 1577808031000, 1577808032000, 1577808033000, 1577808034000,
                    1577808035000, 1577808036000, 1577808037000, 1577808038000, 1577808039000,
                    1577808040000, 1577808041000, 1577808042000, 1577808043000, 1577808044000,
                    1577808045000, 1577808046000, 1577808047000, 1577808048000, 1577808049000,
                    1577808050000, 1577808051000, 1577808052000, 1577808053000, 1577808054000,
                    1577808055000, 1577808056000, 1577808057000, 1577808058000, 1577808059000,
                    1577808060000, 1577808061000, 1577808062000, 1577808063000, 1577808064000,
                    1577808065000, 1577808066000, 1577808067000, 1577808068000, 1577808069000,
                    1577808070000, 1577808071000, 1577808072000, 1577808073000, 1577808074000,
                    1577808075000, 1577808076000, 1577808077000, 1577808078000, 1577808079000,
                    1577808080000, 1577808081000, 1577808082000, 1577808083000, 1577808084000,
                    1577808085000, 1577808086000, 1577808087000, 1577808088000, 1577808089000,
                    1577808090000, 1577808091000, 1577808092000, 1577808093000, 1577808094000,
                    1577808095000
                ],
                [
                    13, 14, 8, 10, 16, 26, 32, 27, 18, 32, 36, 24, 22, 23, 22, 18, 25, 21, 21,
                    14, 8, 11, 14, 23, 18, 17, 19, 20, 22, 19, 13, 26, 13, 14, 22, 24, 21, 22,
                    26, 21, 23, 24, 27, 41, 31, 27, 35, 26, 28, 36, 39, 21, 17, 22, 17, 19, 15,
                    34, 10, 15, 22, 18, 15, 20, 15, 22, 19, 16, 30, 27, 29, 23, 20, 16, 21, 21,
                    25, 16, 18, 15, 18, 14, 10, 15, 8, 15, 6, 11, 8, 7, 13, 10, 23, 16, 15, 25
                ]
            ],
            "option": "algo=holtwinters",
            "algo": "holtwinters",
            "prec": "ms",
            "wncheck": 1,
            "return_conf": 1,
            "forecast_rows": 10,
            "conf": 0.95,
            "start": 1577808096000,
            "every": 1000,
            "rows": 96,
            "protocol": 1.0
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["algo"], "holtwinters")
        self.assertEqual(response.json["rows"], 10)
        self.assertEqual(response.json["period"], 0)
        self.assertEqual(response.json["res"][0][0], 1577808096000)
        self.assertEqual(response.json["res"][0][-1], 1577808105000)
        self.assertEqual(len(response.json["res"][0]), response.json["rows"])
        self.assertEqual(len(response.json["res"]), 4)

    def test_prophet_forecast_model(self):
        """test forecast api for prophet model"""
        response = self.client.post("/forecast", json={
            "schema": [
                ["ts", "TIMESTAMP", 8],
                ["val", "INT", 4]
            ],
            "data": [
                [
                    1577808000000, 1577808001000, 1577808002000, 1577808003000, 1577808004000,
                    1577808005000, 1577808006000, 1577808007000, 1577808008000, 1577808009000,
                    1577808010000, 1577808011000, 1577808012000, 1577808013000, 1577808014000,
                    1577808015000, 1577808016000, 1577808017000, 1577808018000, 1577808019000,
                    1577808020000, 1577808021000, 1577808022000, 1577808023000, 1577808024000,
                    1577808025000, 1577808026000, 1577808027000, 1577808028000, 1577808029000,
                    1577808030000, 1577808031000, 1577808032000, 1577808033000, 1577808034000,
                    1577808035000, 1577808036000, 1577808037000, 1577808038000, 1577808039000,
                    1577808040000, 1577808041000, 1577808042000, 1577808043000, 1577808044000,
                    1577808045000, 1577808046000, 1577808047000, 1577808048000, 1577808049000,
                    1577808050000, 1577808051000, 1577808052000, 1577808053000, 1577808054000,
                    1577808055000, 1577808056000, 1577808057000, 1577808058000, 1577808059000,
                    1577808060000, 1577808061000, 1577808062000, 1577808063000, 1577808064000,
                    1577808065000, 1577808066000, 1577808067000, 1577808068000, 1577808069000,
                    1577808070000, 1577808071000, 1577808072000, 1577808073000, 1577808074000,
                    1577808075000, 1577808076000, 1577808077000, 1577808078000, 1577808079000,
                    1577808080000, 1577808081000, 1577808082000, 1577808083000, 1577808084000,
                    1577808085000, 1577808086000, 1577808087000, 1577808088000, 1577808089000,
                    1577808090000, 1577808091000, 1577808092000, 1577808093000, 1577808094000,
                    1577808095000
                ],
                [
                    13, 14, 8, 10, 16, 26, 32, 27, 18, 32, 36, 24, 22, 23, 22, 18, 25, 21, 21,
                    14, 8, 11, 14, 23, 18, 17, 19, 20, 22, 19, 13, 26, 13, 14, 22, 24, 21, 22,
                    26, 21, 23, 24, 27, 41, 31, 27, 35, 26, 28, 36, 39, 21, 17, 22, 17, 19, 15,
                    34, 10, 15, 22, 18, 15, 20, 15, 22, 19, 16, 30, 27, 29, 23, 20, 16, 21, 21,
                    25, 16, 18, 15, 18, 14, 10, 15, 8, 15, 6, 11, 8, 7, 13, 10, 23, 16, 15, 25
                ]
            ],
            "option": "algo=prophet",
            "algo": "prophet",
            "prec": "ms",
            "wncheck": 1,
            "return_conf": 1,
            "forecast_rows": 10,
            "conf": 0.95,
            "start": 1577808096000,
            "every": 1000,
            "rows": 96,
            "protocol": 1.0
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["algo"], "prophet")
        self.assertEqual(response.json["rows"], 10)
        self.assertEqual(response.json["period"], 0)
        self.assertEqual(response.json["res"][0][0], 1577808096000)
        self.assertEqual(response.json["res"][0][-1], 1577808105000)
        self.assertEqual(len(response.json["res"][0]), response.json["rows"])
        self.assertEqual(len(response.json["res"]), 4)

    def test_ad(self):
        """test anomaly detect api"""
        response = self.client.post("/anomaly-detect", json={
            "schema": [
                ["ts", "TIMESTAMP", 8],
                ["val", "INT", 4]
            ],
            "data": [
                [1577808000000, 1577808001000, 1577808002000, 1577808003000, 1577808004000,
                 1577808005000, 1577808006000, 1577808007000, 1577808008000, 1577808009000,
                 1577808010000, 1577808011000, 1577808012000, 1577808013000, 1577808014000,
                 1577808015000, 1577808016000],
                [5, 14, 15, 15, 14, 19, 17, 16, 20, 22, 8, 21, 28, 11, 9, 29, 40]
            ],
            "rows": 17,
            "algo": "iqr"
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["rows"], 1)
        self.assertEqual(response.json["algo"], "iqr")

    def test_ad_error_get(self):
        """1. invalid http method"""
        response = self.client.get("/anomaly-detect", json={
            "schema": [
                ["ts", "TIMESTAMP", 8],
                ["val", "INT", 4]
            ],
            "data": [
                [1577808000000, 1577808001000, 1577808002000, 1577808003000, 1577808004000,
                 1577808005000, 1577808006000, 1577808007000, 1577808008000, 1577808009000,
                 1577808010000, 1577808011000, 1577808012000, 1577808013000, 1577808014000,
                 1577808015000, 1577808016000],
                [5, 14, 15, 15, 14, 19, 17, 16, 20, 22, 8, 21, 28, 11, 9, 29, 40]
            ],
            "rows": 17,
            "algo": "iqr"
        })

        self.assertEqual(response.status_code, 405)

    def test_ad_error_empty_payload(self):
        """2. list that is going to apply anomaly detection is empty or less value than the threshold
        , which is [10, 100000]"""
        response = self.client.post("/anomaly-detect", json={
            "schema": [
                ["ts", "TIMESTAMP", 8],
                ["val", "INT", 4]
            ],
            "data": [
                [1577808000000, 1577808001000, 1577808002000, 1577808003000, 1577808004000,
                 1577808005000, 1577808006000, 1577808007000, 1577808008000],
                [5, 14, 15, 15, 14, 19, 17, 16, 20]
            ],
            "rows": 9,
            "algo": "iqr"
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["rows"], -1)

    def test_ad_error_single_col(self):
        """3. only one column"""
        response = self.client.post("/anomaly-detect", json={
            "schema": [
                ["ts", "TIMESTAMP", 8],
                ["val", "INT", 4]
            ],
            "data": [
                [1577808000000, 1577808001000, 1577808002000, 1577808003000, 1577808004000,
                 1577808005000, 1577808006000, 1577808007000, 1577808008000]
            ],
            "rows": 9,
            "algo": "iqr"
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["rows"], -1)

    def test_ad_error_three_cols(self):
        """4. there are three input columns """
        response = self.client.post("/anomaly-detect", json={
            "schema": [
                ["ts", "TIMESTAMP", 8],
                ["val", "INT", 4],
                ["val1", "INT", 4]
            ],
            "data": [
                [1577808000000, 1577808001000, 1577808002000, 1577808003000, 1577808004000,
                 1577808005000, 1577808006000, 1577808007000, 1577808008000, 1577808009000],
                [5, 14, 15, 15, 14, 19, 17, 16, 20, 44],
                [5, 14, 15, 15, 14, 19, 17, 16, 20, 44]
            ],
            "rows": 10,
            "algo": "iqr"
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["rows"], -1)

    def test_ad_disorder_cols(self):
        """5. disorder two columns """
        response = self.client.post("/anomaly-detect", json={
            "schema": [
                ["val", "INT", 4],
                ["ts", "TIMESTAMP", 8]
            ],
            "data": [
                [5, 14, 15, 15, 14, 19, 17, 16, 20, 44],
                [1577808000000, 1577808001000, 1577808002000, 1577808003000, 1577808004000,
                 1577808005000, 1577808006000, 1577808007000, 1577808008000, 1577808009000],
            ],
            "rows": 10,
            "algo": "iqr"
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["rows"], 2)

    def test_missing_schema(self):
        """6. missing schema info"""
        response = self.client.post("/anomaly-detect", json={
            "data": [
                [5, 14, 15, 15, 14, 19, 17, 16, 20, 44],
                [1577808000000, 1577808001000, 1577808002000, 1577808003000, 1577808004000,
                 1577808005000, 1577808006000, 1577808007000, 1577808008000, 1577808009000],
            ],
            "rows": 10,
            "algo": "iqr"
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["rows"], -1)

    def test_invalid_schema_info(self):
        """7. invalid schema info"""
        response = self.client.post("/anomaly-detect", json={
            "schema": [
                ["ts", "TIMESTAMP", 8]
            ],
            "data": [
                [1577808000000, 1577808001000, 1577808002000, 1577808003000, 1577808004000,
                 1577808005000, 1577808006000, 1577808007000, 1577808008000, 1577808009000],
            ],
            "rows": 10,
            "algo": "iqr"
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["rows"], -1)


    def test_gpt_restful_service(self):
        response = self.client.post('/forecast', json={
            "schema": [
                ["ts", "TIMESTAMP", 8],
                ["val", "INT", 4]
            ],
            "data": [
                [
                    1577808000000, 1577808001000, 1577808002000, 1577808003000, 1577808004000,
                    1577808005000, 1577808006000, 1577808007000, 1577808008000, 1577808009000,
                    1577808010000, 1577808011000, 1577808012000, 1577808013000, 1577808014000,
                    1577808015000, 1577808016000, 1577808017000, 1577808018000, 1577808019000,
                    1577808020000, 1577808021000, 1577808022000, 1577808023000, 1577808024000,
                    1577808025000, 1577808026000, 1577808027000, 1577808028000, 1577808029000,
                    1577808030000, 1577808031000, 1577808032000, 1577808033000, 1577808034000,
                    1577808035000, 1577808036000, 1577808037000, 1577808038000, 1577808039000,
                    1577808040000, 1577808041000, 1577808042000, 1577808043000, 1577808044000,
                    1577808045000, 1577808046000, 1577808047000, 1577808048000, 1577808049000,
                    1577808050000, 1577808051000, 1577808052000, 1577808053000, 1577808054000,
                    1577808055000, 1577808056000, 1577808057000, 1577808058000, 1577808059000,
                    1577808060000, 1577808061000, 1577808062000, 1577808063000, 1577808064000,
                    1577808065000, 1577808066000, 1577808067000, 1577808068000, 1577808069000,
                    1577808070000, 1577808071000, 1577808072000, 1577808073000, 1577808074000,
                    1577808075000, 1577808076000, 1577808077000, 1577808078000, 1577808079000,
                    1577808080000, 1577808081000, 1577808082000, 1577808083000, 1577808084000,
                    1577808085000, 1577808086000, 1577808087000, 1577808088000, 1577808089000,
                    1577808090000, 1577808091000, 1577808092000, 1577808093000, 1577808094000,
                    1577808095000
                ],
                [
                    13, 14, 8, 10, 16, 26, 32, 27, 18, 32, 36, 24, 22, 23, 22, 18, 25, 21, 21,
                    14, 8, 11, 14, 23, 18, 17, 19, 20, 22, 19, 13, 26, 13, 14, 22, 24, 21, 22,
                    26, 21, 23, 24, 27, 41, 31, 27, 35, 26, 28, 36, 39, 21, 17, 22, 17, 19, 15,
                    34, 10, 15, 22, 18, 15, 20, 15, 22, 19, 16, 30, 27, 29, 23, 20, 16, 21, 21,
                    25, 16, 18, 15, 18, 14, 10, 15, 8, 15, 6, 11, 8, 7, 13, 10, 23, 16, 15, 25
                ]
            ],
            "option": "algo=td_gpt_fc",
            "algo": "td_gpt_fc",
            "prec": "ms",
            "wncheck": 0,
            "return_conf": 0,
            "forecast_rows": 10,
            "conf": 95,
            "start": 1577808096000,
            "every": 1000,
            "rows": 21,
            "protocol": 1.0
        })


    def test_dtw_service(self):
        response = self.client.post('/correlation', json={
            "schema": [
                ["ts", "TIMESTAMP", 8],
                ["val", "INT", 4],
                ["val1", "INT", 4]
            ],
            "data": [
                [1577808000000, 1577808001000, 1577808002000, 1577808003000, 1577808004000],
                [1, 1.1, 1.0, 1.2, 1.1],
                [1, 1.5, 1.3, 1.8, 1.6]
            ],
            "option": "algo=dtw,radius=2",
            "algo": "dtw",
            "prec": "ms",
            "wncheck": 0,
            "protocol": 1.0
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["rows"], 7)
        self.assertTrue(math.isclose(response.json["distance"], 1.6))

    def test_tlcc_service(self):
        req = {
            "schema": [
                ["ts", "TIMESTAMP", 8],
                ["val", "INT", 4],
                ["val1", "INT", 4]
            ],
            "data": [],
            "option": "algo=tlcc,lag_start=-20,lag_end=20",
            "algo": "tlcc",
            "prec": "ms",
            "wncheck": 0,
            "protocol": 1.0
        }

        np.random.seed(42)
        n = 100

        # x_t:
        x = np.sin(np.linspace(0, 10, n)) + np.random.normal(0, 0.2, n)

        # y_t: delay 3 steps
        y = np.roll(x, 3) + np.random.normal(0, 0.2, n)

        ts = [i for i in range(0, 100)]

        req["data"].append(ts)
        req["data"].append(x.tolist())
        req["data"].append(y.tolist())

        response = self.client.post('/correlation', json=req)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["rows"], 41)
        self.assertEqual(np.argmax(response.json["ccf_vals"]), 23)
