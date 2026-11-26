# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import configparser
import io
import pathlib
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from medusa.config import (
    CassandraConfig, MedusaConfig, SSHConfig, SSMConfig, _namedtuple_from_dict
)
from medusa.orchestration import Orchestration, OrchestrationError, SshOrchestration
from medusa.ssm_orchestration import (
    INVOCATION_NOT_FOUND_GRACE_SECONDS,
    InstanceResolver,
    SsmOrchestration,
    _format_command,
    _parse_s3_url,
)


def _client_error(code, message='error', operation='TestOp'):
    return ClientError({'Error': {'Code': code, 'Message': message}}, operation)


def _build_medusa_config(ssm_overrides=None, use_sudo='True'):
    config = configparser.ConfigParser(interpolation=None)
    config['cassandra'] = {'use_sudo': use_sudo}
    config['ssh'] = {
        'username': 'guest',
        'key_file': '',
        'port': '22',
        'cert_file': '',
        'keepalive_seconds': '60',
        'use_pty': 'False',
        'login_shell': 'False',
        'known_hosts': '',
        'forward_agent': 'False',
    }
    ssm_defaults = {
        'enabled': 'True',
        'region': '',
        'document_name': 'AWS-RunShellScript',
        'execution_timeout': '86400',
        'delivery_timeout': '600',
        'poll_interval_min': '2',
        'poll_interval_max': '30',
        'run_as_user': '',
        'login_shell': 'False',
        'host_instance_map': '',
        'output_s3_bucket': '',
        'output_s3_prefix': '',
    }
    if ssm_overrides:
        ssm_defaults.update(ssm_overrides)
    config['ssm'] = ssm_defaults

    return MedusaConfig(
        file_path=None,
        storage=None,
        monitoring={},
        cassandra=_namedtuple_from_dict(CassandraConfig, config['cassandra']),
        ssh=_namedtuple_from_dict(SSHConfig, config['ssh']),
        checks=None,
        logging=None,
        grpc=None,
        kubernetes=None,
        ssm=_namedtuple_from_dict(SSMConfig, config['ssm']),
    )


def _make_ec2_paginator(instances):
    paginator = MagicMock()
    paginator.paginate.return_value = [{'Reservations': [{'Instances': instances}]}]
    ec2_client = MagicMock()
    ec2_client.get_paginator.return_value = paginator
    return ec2_client


def _make_ssm_paginator(info_list):
    paginator = MagicMock()
    paginator.paginate.return_value = [{'InstanceInformationList': info_list}]
    ssm_client = MagicMock()
    ssm_client.get_paginator.return_value = paginator
    return ssm_client


class SsmOrchestrationTest(unittest.TestCase):

    def test_orchestration_facade_dispatch(self):
        """Orchestration selects SshOrchestration by default and SsmOrchestration when ssm.enabled=True."""
        cfg_no_ssm = _build_medusa_config()._replace(ssm=None)
        self.assertIsInstance(Orchestration(cfg_no_ssm)._backend, SshOrchestration)

        cfg_ssm_off = _build_medusa_config({'enabled': 'False'})
        self.assertIsInstance(Orchestration(cfg_ssm_off)._backend, SshOrchestration)

        cfg_ssm_on = _build_medusa_config({'enabled': 'True'})
        self.assertIsInstance(Orchestration(cfg_ssm_on)._backend, SsmOrchestration)

    def test_format_command(self):
        """Verify command formatting for root vs run_as_user, login_shell, and use_sudo."""
        self.assertEqual(
            _format_command('nodetool status', '', False, True),
            'nodetool status',
        )
        self.assertEqual(
            _format_command("echo 'hi' && nodetool status", '', True, True),
            "bash -lc 'echo '\"'\"'hi'\"'\"' && nodetool status'",
        )
        self.assertEqual(
            _format_command('nodetool status', 'cassandra', True, True),
            "su - cassandra -c 'sudo nodetool status'",
        )
        self.assertEqual(
            _format_command('nodetool status', 'cassandra', False, False),
            "su cassandra -c 'nodetool status'",
        )

    def test_parse_s3_url(self):
        self.assertEqual(_parse_s3_url('s3://my-bucket/prefix/out.txt'), ('my-bucket', 'prefix/out.txt'))
        self.assertEqual(
            _parse_s3_url('https://my-bucket.s3.us-west-2.amazonaws.com/a/b%20c/stdout.txt'),
            ('my-bucket', 'a/b c/stdout.txt'),
        )
        self.assertEqual(
            _parse_s3_url('https://s3.us-west-2.amazonaws.com/my-bucket/a/b/stderr.txt'),
            ('my-bucket', 'a/b/stderr.txt'),
        )
        self.assertIsNone(_parse_s3_url(''))
        self.assertIsNone(_parse_s3_url('https://example.com/foo'))

    def test_resolver_direct_instance_ids(self):
        """EC2 i-* and managed mi-* IDs pass through without calling AWS APIs."""
        ssm_client = MagicMock()
        ec2_client = MagicMock()
        resolver = InstanceResolver(ssm_client, ec2_client)

        hosts = ['i-0123456789abcdef0', 'mi-0fedcba9876543210']
        resolved, errors = resolver.resolve(hosts)
        self.assertEqual(resolved, {h: h for h in hosts})
        self.assertEqual(errors, {})
        ssm_client.get_paginator.assert_not_called()
        ec2_client.get_paginator.assert_not_called()

    def test_resolver_static_map_precedence_and_errors(self):
        """Static map takes precedence over EC2/SSM; missing/malformed files raise OrchestrationError."""
        with tempfile.NamedTemporaryFile('w', delete=False) as tmp:
            tmp.write("# comment\n\n10.0.0.1,i-111111\nnode2.internal = i-222222\n")
            map_path = pathlib.Path(tmp.name)

        try:
            ssm_client = MagicMock()
            ec2_client = MagicMock()
            resolver = InstanceResolver(ssm_client, ec2_client, host_instance_map_path=str(map_path))
            resolved, errors = resolver.resolve(['10.0.0.1', 'node2.internal'])
            self.assertEqual(resolved, {'10.0.0.1': 'i-111111', 'node2.internal': 'i-222222'})
            self.assertEqual(errors, {})
            ec2_client.get_paginator.assert_not_called()
            ssm_client.get_paginator.assert_not_called()
        finally:
            map_path.unlink(missing_ok=True)

        with self.assertRaises(OrchestrationError):
            InstanceResolver(MagicMock(), host_instance_map_path='/nonexistent/path/map.txt')

        with tempfile.NamedTemporaryFile('w', delete=False) as bad_tmp:
            bad_tmp.write("invalid_line_without_separator\n")
            bad_path = pathlib.Path(bad_tmp.name)
        try:
            with self.assertRaises(OrchestrationError):
                InstanceResolver(MagicMock(), host_instance_map_path=str(bad_path))
        finally:
            bad_path.unlink(missing_ok=True)

    def test_resolver_ec2_and_cache(self):
        """EC2 DescribeInstances resolves IPs/DNS names and caches results across calls."""
        ec2_client = _make_ec2_paginator([
            {
                'InstanceId': 'i-aaa111',
                'PrivateIpAddress': '10.0.1.10',
                'PublicIpAddress': '54.1.2.10',
                'PrivateDnsName': 'ip-10-0-1-10.ec2.internal',
                'PublicDnsName': 'ec2-54-1-2-10.compute-1.amazonaws.com',
            }
        ])
        ssm_client = MagicMock()
        resolver = InstanceResolver(ssm_client, ec2_client)

        hosts = ['10.0.1.10', '54.1.2.10', 'ip-10-0-1-10.ec2.internal', 'ec2-54-1-2-10.compute-1.amazonaws.com']
        resolved, errors = resolver.resolve(hosts)
        self.assertEqual(errors, {})
        for h in hosts:
            self.assertEqual(resolved[h], 'i-aaa111')
        ssm_client.get_paginator.assert_not_called()

        # Second call hits cache without another describe_instances call
        resolved2, errors2 = resolver.resolve(['10.0.1.10'])
        self.assertEqual(resolved2, {'10.0.1.10': 'i-aaa111'})
        self.assertEqual(errors2, {})
        self.assertEqual(ec2_client.get_paginator.call_count, 1)

    def test_resolver_ssm_fallback_on_ec2_unauthorized_and_dns(self):
        """Falls back to SSM DescribeInstanceInformation when EC2 raises UnauthorizedOperation."""
        ec2_client = MagicMock()
        ec2_client.get_paginator.side_effect = _client_error('UnauthorizedOperation', 'Access denied')

        ssm_client = _make_ssm_paginator([
            {'InstanceId': 'mi-999abc', 'IPAddress': '10.0.2.20', 'ComputerName': 'cass-node-2'}
        ])
        resolver = InstanceResolver(ssm_client, ec2_client)

        def _fake_gethostbyname(hostname):
            return {'cass-node-2.example.com': '10.0.2.20'}.get(hostname, hostname)

        with patch('medusa.ssm_orchestration.socket.gethostbyname', side_effect=_fake_gethostbyname):
            resolved, errors = resolver.resolve(['cass-node-2', 'cass-node-2.example.com', '10.0.9.99'])

        self.assertEqual(resolved['cass-node-2'], 'mi-999abc')
        self.assertEqual(resolved['cass-node-2.example.com'], 'mi-999abc')
        self.assertIn('10.0.9.99', errors)
        self.assertIsInstance(errors['10.0.9.99'], OrchestrationError)

    @patch('medusa.ssm_orchestration.time.sleep')
    def test_uniform_batching_chunks_at_50(self, _mock_sleep):
        """Uniform commands across 75 hosts with pool_size=100 split into 50 + 25 SendCommand calls."""
        hosts = [f'i-{idx:017x}' for idx in range(75)]
        ssm_client = MagicMock()
        ssm_client.send_command.side_effect = [
            {'Command': {'CommandId': 'cmd-batch-1'}},
            {'Command': {'CommandId': 'cmd-batch-2'}},
        ]
        ssm_client.get_command_invocation.return_value = {
            'Status': 'Success',
            'ResponseCode': 0,
            'StandardOutputContent': 'ok',
            'StandardErrorContent': '',
        }

        cfg = _build_medusa_config({
            'output_s3_bucket': 'ssm-logs-bkt',
            'output_s3_prefix': 'medusa',
        })
        orch = SsmOrchestration(cfg, pool_size=100, ssm_client=ssm_client, ec2_client=MagicMock())
        result = orch.pssh_run(hosts, 'nodetool snapshot', hosts_variables={})

        self.assertTrue(result)
        self.assertEqual(ssm_client.send_command.call_count, 2)
        first_call = ssm_client.send_command.call_args_list[0].kwargs
        second_call = ssm_client.send_command.call_args_list[1].kwargs
        self.assertEqual(len(first_call['InstanceIds']), 50)
        self.assertEqual(len(second_call['InstanceIds']), 25)
        self.assertEqual(first_call['MaxConcurrency'], '50')
        self.assertEqual(first_call['MaxErrors'], '100%')
        self.assertEqual(first_call['Parameters']['executionTimeout'], ['86400'])
        self.assertEqual(first_call['TimeoutSeconds'], 600)
        self.assertEqual(first_call['OutputS3BucketName'], 'ssm-logs-bkt')
        self.assertEqual(first_call['OutputS3KeyPrefix'], 'medusa')

    @patch('medusa.ssm_orchestration.time.sleep')
    def test_hosts_variables_per_host_and_unresolved_alignment(self, _mock_sleep):
        """Per-host hosts_variables preserve host-to-variable alignment even when a host fails resolution."""
        ec2_client = _make_ec2_paginator([
            {'InstanceId': 'i-0001', 'PrivateIpAddress': '10.0.0.1'},
            # 10.0.0.2 intentionally missing from EC2 and SSM
            {'InstanceId': 'i-0003', 'PrivateIpAddress': '10.0.0.3'},
        ])
        ssm_client = _make_ssm_paginator([])
        ssm_client.send_command.side_effect = [
            {'Command': {'CommandId': 'cmd-1'}},
            {'Command': {'CommandId': 'cmd-3'}},
        ]
        ssm_client.get_command_invocation.return_value = {
            'Status': 'Success',
            'ResponseCode': 0,
            'StandardOutputContent': 'ok',
            'StandardErrorContent': '',
        }

        cfg = _build_medusa_config()
        orch = SsmOrchestration(cfg, pool_size=2, ssm_client=ssm_client, ec2_client=ec2_client)
        result = orch.pssh_run(
            ['10.0.0.1', '10.0.0.2', '10.0.0.3'],
            'medusa --fqdn=%s restore-node %s',
            hosts_variables=[
                ('10.0.0.1', '--seeds 10.0.0.9'),
                ('10.0.0.2', '--seeds 10.0.0.9'),
                ('10.0.0.3', ''),
            ],
        )

        self.assertFalse(result)
        self.assertEqual(ssm_client.send_command.call_count, 2)
        sent_1 = ssm_client.send_command.call_args_list[0].kwargs
        sent_3 = ssm_client.send_command.call_args_list[1].kwargs
        self.assertEqual(sent_1['InstanceIds'], ['i-0001'])
        self.assertEqual(sent_1['Parameters']['commands'], ['medusa --fqdn=10.0.0.1 restore-node --seeds 10.0.0.9'])
        self.assertEqual(sent_3['InstanceIds'], ['i-0003'])
        self.assertEqual(sent_3['Parameters']['commands'], ['medusa --fqdn=10.0.0.3 restore-node '])

    @patch('medusa.ssm_orchestration.time.sleep')
    @patch('medusa.ssm_orchestration.time.monotonic')
    def test_polling_grace_period_backoff_and_partial_failure(self, mock_monotonic, mock_sleep):
        """Handles InvocationDoesNotExist grace period, exponential backoff, and waits for all hosts."""
        mock_monotonic.side_effect = [0.0, 5.0, 5.0, 5.0, 7.0, 7.0, 7.0]
        ssm_client = MagicMock()
        ssm_client.send_command.return_value = {'Command': {'CommandId': 'cmd-abc'}}

        # 3 hosts: i-1 (InvocationDoesNotExist -> Success), i-2 (InProgress -> Failed), i-3 (Success)
        invocations = {
            'i-001': [
                _client_error('InvocationDoesNotExist'),
                {'Status': 'Success', 'ResponseCode': 0, 'StandardOutputContent': 'ok', 'StandardErrorContent': ''},
            ],
            'i-002': [
                {'Status': 'InProgress'},
                {'Status': 'Failed', 'ResponseCode': 1, 'StandardOutputContent': '', 'StandardErrorContent': 'boom'},
            ],
            'i-003': [
                {'Status': 'Success', 'ResponseCode': 0, 'StandardOutputContent': 'ok', 'StandardErrorContent': ''},
            ],
        }

        def _get_inv(CommandId, InstanceId):
            nxt = invocations[InstanceId].pop(0)
            if isinstance(nxt, Exception):
                raise nxt
            return nxt

        ssm_client.get_command_invocation.side_effect = _get_inv

        cfg = _build_medusa_config({'poll_interval_min': '2', 'poll_interval_max': '5'})
        orch = SsmOrchestration(cfg, pool_size=10, ssm_client=ssm_client, ec2_client=MagicMock())
        result = orch.pssh_run(['i-001', 'i-002', 'i-003'], 'medusa backup-node')

        self.assertFalse(result)
        self.assertEqual(invocations['i-001'], [])
        self.assertEqual(invocations['i-002'], [])
        self.assertEqual(invocations['i-003'], [])
        mock_sleep.assert_called_once_with(2.0)

    @patch('medusa.ssm_orchestration.time.sleep')
    @patch('medusa.ssm_orchestration.time.monotonic')
    def test_polling_invocation_not_found_past_grace_period(self, mock_monotonic, _mock_sleep):
        """InvocationDoesNotExist past INVOCATION_NOT_FOUND_GRACE_SECONDS fails the host."""
        mock_monotonic.side_effect = [0.0, float(INVOCATION_NOT_FOUND_GRACE_SECONDS + 1)]
        ssm_client = MagicMock()
        ssm_client.send_command.return_value = {'Command': {'CommandId': 'cmd-late'}}
        ssm_client.get_command_invocation.side_effect = _client_error('InvocationDoesNotExist')

        cfg = _build_medusa_config()
        orch = SsmOrchestration(cfg, pool_size=10, ssm_client=ssm_client, ec2_client=MagicMock())
        result = orch.pssh_run(['i-001'], 'medusa backup-node')

        self.assertFalse(result)

    @patch('medusa.ssm_orchestration.time.sleep')
    def test_s3_output_retrieval_and_fallback(self, _mock_sleep):
        """Fetches full logs from S3 on failure and falls back to inline content if S3 read fails."""
        ssm_client = MagicMock()
        ssm_client.send_command.return_value = {'Command': {'CommandId': 'cmd-s3'}}
        ssm_client.get_command_invocation.side_effect = [
            {
                'Status': 'Failed',
                'ResponseCode': 1,
                'StandardOutputContent': 'inline-out',
                'StandardErrorContent': 'inline-err',
                'StandardOutputUrl': 'https://my-logs.s3.us-west-2.amazonaws.com/prefix/cmd-s3/i-001/stdout',
                'StandardErrorUrl': 'https://my-logs.s3.us-west-2.amazonaws.com/prefix/cmd-s3/i-001/stderr',
            },
            {
                'Status': 'TimedOut',
                'ResponseCode': -1,
                'StandardOutputContent': 'inline-out-2',
                'StandardErrorContent': 'inline-err-2',
                'StandardOutputUrl': 'https://my-logs.s3.us-west-2.amazonaws.com/prefix/cmd-s3/i-002/stdout',
                'StandardErrorUrl': 'https://my-logs.s3.us-west-2.amazonaws.com/prefix/cmd-s3/i-002/stderr',
            },
        ]

        s3_client = MagicMock()
        s3_client.get_object.side_effect = [
            {'Body': io.BytesIO(b'full-s3-stdout')},
            {'Body': io.BytesIO(b'full-s3-stderr')},
            _client_error('NoSuchKey', 'missing'),
            _client_error('NoSuchKey', 'missing'),
        ]

        cfg = _build_medusa_config({'output_s3_bucket': 'my-logs', 'output_s3_prefix': 'prefix'})
        orch = SsmOrchestration(cfg, pool_size=10, ssm_client=ssm_client, ec2_client=MagicMock(), s3_client=s3_client)

        with patch('medusa.ssm_orchestration.BaseOrchestration._report_results', return_value=False) as mock_report:
            result = orch.pssh_run(['i-001', 'i-002'], 'medusa backup-node')

        self.assertFalse(result)
        error_list = mock_report.call_args.args[3]
        self.assertEqual(error_list[0][1].stdout, 'full-s3-stdout')
        self.assertEqual(error_list[0][1].stderr, 'full-s3-stderr')
        self.assertEqual(error_list[1][1].stdout, 'inline-out-2')
        self.assertEqual(error_list[1][1].stderr, 'inline-err-2')

    @patch('medusa.ssm_orchestration.time.sleep')
    def test_keyboard_interrupt_cancels_in_flight_commands(self, mock_sleep):
        """KeyboardInterrupt during polling calls cancel_command on all remaining commands and re-raises."""
        ssm_client = MagicMock()
        ssm_client.send_command.side_effect = [
            {'Command': {'CommandId': 'cmd-1'}},
            {'Command': {'CommandId': 'cmd-2'}},
        ]
        ssm_client.get_command_invocation.return_value = {'Status': 'InProgress'}
        ssm_client.cancel_command.side_effect = [
            _client_error('InvalidCommandId', 'already finished'),
            {},
        ]
        mock_sleep.side_effect = KeyboardInterrupt()

        cfg = _build_medusa_config()
        orch = SsmOrchestration(cfg, pool_size=2, ssm_client=ssm_client, ec2_client=MagicMock())

        with self.assertRaises(KeyboardInterrupt):
            orch.pssh_run(
                ['i-001', 'i-002'],
                'medusa --fqdn=%s restore-node',
                hosts_variables=[('10.0.0.1',), ('10.0.0.2',)],
            )

        self.assertEqual(ssm_client.cancel_command.call_count, 2)
        ssm_client.cancel_command.assert_any_call(CommandId='cmd-1', InstanceIds=['i-001'])
        ssm_client.cancel_command.assert_any_call(CommandId='cmd-2', InstanceIds=['i-002'])

    def test_empty_hosts_returns_true(self):
        cfg = _build_medusa_config()
        orch = SsmOrchestration(cfg, pool_size=10, ssm_client=MagicMock(), ec2_client=MagicMock())
        self.assertTrue(orch.pssh_run([], 'nodetool status'))


if __name__ == '__main__':
    unittest.main()
