# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import unittest

from stream_deploy_cli import HealthyHeartbeats, SnodeHealthScan


class HeartbeatResultTest(unittest.TestCase):
    def test_only_matching_successful_response_counts(self):
        reports = HealthyHeartbeats('aa')

        def report(text, tid='101'):
            return reports.completed(f'11/19 09:00:00 {tid} E MST DEBUG {text}')

        def start():
            report('start to process stream hb req msg')
            report('start to process grp 3 stream-hb from dnode:1, snodeId:1')
            report(' aa Trigger TASK:1 task status Running got')

        start()
        self.assertFalse(report('end to process stream hb req msg, gid:3, response code:-1'))
        self.assertFalse(report('end to process stream hb req msg, gid:3, response code:0'))
        start()
        self.assertFalse(report('end to process stream hb req msg, gid:4, response code:0'))
        start()
        self.assertFalse(report('end to process stream hb req msg, gid:3, response code:0', '102'))
        self.assertTrue(report('end to process stream hb req msg, gid:3, response code:0'))


class HealthScanTest(unittest.TestCase):
    def test_requires_matching_online_scan_after_threshold(self):
        scan = SnodeHealthScan(41000)
        online = 'snode 1 online, try to check tasks status, currTs:{}, lastUpTs:999'
        end = 'end health check, soltIdx:1, checkStartTs:{}'
        self.assertFalse(scan.completed(end.format(42000)))
        self.assertFalse(scan.completed(online.format(40999)))
        self.assertFalse(scan.completed(end.format(40999)))
        self.assertFalse(scan.completed(end.format(42000)))
        self.assertFalse(scan.completed(online.format(42000)))
        self.assertTrue(scan.completed(end.format(42000)))


if __name__ == '__main__':
    unittest.main()
