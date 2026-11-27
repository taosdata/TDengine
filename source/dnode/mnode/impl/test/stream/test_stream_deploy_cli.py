# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import unittest
from pathlib import Path
import subprocess
import tempfile
from unittest.mock import Mock, patch

from stream_deploy_cli import HealthyHeartbeats, SnodeHealthScan, run_case, wait_dnode_ready


class DnodeReadinessTest(unittest.TestCase):
    @staticmethod
    def status(state, vnodes, support):
        return f'| status | vnodes | support_vnodes |\n| {state} | {vnodes} | {support} |\n'

    def test_waits_for_online_status_and_two_available_vnodes(self):
        sql = Mock(side_effect=[
            '',
            self.status('offline', 0, 0),
            self.status('offline', 0, 4),
            self.status('ready', 0, 0),
            self.status('ready', 3, 4),
            self.status('ready', 2, 4),
        ])
        with patch('stream_deploy_cli.time.sleep') as sleep:
            wait_dnode_ready(sql)
        self.assertEqual(sql.call_count, 6)
        sql.assert_called_with(
            'select status,`vnodes`,`support_vnodes` from information_schema.ins_dnodes where id=1')
        self.assertEqual(sleep.call_count, 5)

    def test_ready_node_does_not_sleep(self):
        with patch('stream_deploy_cli.time.sleep') as sleep:
            wait_dnode_ready(Mock(return_value=self.status('ready', 0, 2)))
        sleep.assert_not_called()

    def test_timeout_reports_last_status(self):
        last = self.status('offline', 0, 0)
        with patch('stream_deploy_cli.time.monotonic', side_effect=[0, 0, 1]), \
                patch('stream_deploy_cli.time.sleep'):
            with self.assertRaises(AssertionError) as error:
                wait_dnode_ready(Mock(return_value=last), timeout=1)
        self.assertIn('dnode 1 ready with capacity for two vgroups', str(error.exception))
        self.assertIn(last, str(error.exception))

    def test_sql_failure_is_not_retried(self):
        sql = Mock(side_effect=RuntimeError('test daemon exited with 1'))
        with self.assertRaisesRegex(RuntimeError, 'test daemon exited with 1'):
            wait_dnode_ready(sql)
        sql.assert_called_once()

    def test_fixture_waits_before_snode_and_database_creation(self):
        # The single capacity check must reject offline and full nodes
        # before allowing DDL with two spare vnodes.
        states = [('offline', 0, 0), ('ready', 3, 4), ('ready', 2, 4)]
        statements = []

        def execute(command, **kwargs):
            statement = command[-1]
            statements.append(statement)
            if statement == 'select 1':
                text = '| 1 |\n| 1 |\n'
            elif statement.startswith('show dnode 1 variables'):
                text = '| dnode_id | name | value |\n| 1 | logDir | ./log |\n'
            elif 'information_schema.ins_dnodes' in statement:
                state, vnodes, support = states.pop(0)
                text = self.status(state, vnodes, support)
            else:
                self.assertFalse(states, f'DDL ran before dnode readiness: {statement}')
                if statement == 'create database testdb vgroups 2':
                    raise RuntimeError('database creation reached')
                text = ''
            return subprocess.CompletedProcess(command, 0, text.encode())

        server = Mock()
        server.poll.return_value = None
        with tempfile.TemporaryDirectory() as tmp, \
                patch('stream_deploy_cli.available_ports', return_value=(16030, 16031)), \
                patch('stream_deploy_cli.subprocess.Popen', return_value=server), \
                patch('stream_deploy_cli.subprocess.run', side_effect=execute), \
                patch('stream_deploy_cli.time.sleep'):
            with self.assertRaisesRegex(RuntimeError, 'database creation reached'):
                run_case(Path(tmp), Path('daemon'), Path('taos'))
        self.assertIn('create snode on dnode 1', statements)
        self.assertEqual(statements[-1], 'create database testdb vgroups 2')
        server.terminate.assert_called_once()
        server.wait.assert_called_once_with(timeout=12)


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
