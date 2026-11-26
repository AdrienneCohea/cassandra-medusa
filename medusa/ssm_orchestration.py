# -*- coding: utf-8 -*-
# Copyright 2020- Datastax, Inc. All rights reserved.
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

import logging
import pathlib
import re
import shlex
import socket
import time
from urllib.parse import unquote, urlparse

import boto3
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import BotoCoreError, ClientError

import medusa.utils
from medusa.orchestration import BaseOrchestration, CommandResult, OrchestrationError
from medusa.storage import divide_chunks

SSM_MAX_TARGETS_PER_COMMAND = 50
INVOCATION_NOT_FOUND_GRACE_SECONDS = 60
_INLINE_OUTPUT_TRUNCATION_THRESHOLD = 24000
_INSTANCE_ID_RE = re.compile(r'^(i|mi)-[0-9a-f]+$')
_NON_TERMINAL_STATUSES = {'Pending', 'InProgress', 'Delayed', 'Cancelling'}


def _format_command(command, run_as_user, login_shell, use_sudo):
    """Wrap command for AWS-RunShellScript respecting user, login shell, and sudo settings."""
    if not run_as_user:
        # SSM agent runs as root by default; do not prepend sudo.
        if login_shell:
            return f'bash -lc {shlex.quote(command)}'
        return command

    inner = f'sudo {command}' if use_sudo else command
    if login_shell:
        return f'su - {shlex.quote(run_as_user)} -c {shlex.quote(inner)}'
    return f'su {shlex.quote(run_as_user)} -c {shlex.quote(inner)}'


def _parse_s3_url(url):
    """Parse s3:// or AWS S3 HTTPS URL into (bucket, key), or return None if unrecognized."""
    if not url:
        return None
    parsed = urlparse(url)
    if parsed.scheme == 's3':
        bucket = parsed.netloc
        key = unquote(parsed.path.lstrip('/'))
        if bucket and key:
            return bucket, key
        return None

    if parsed.scheme in ('http', 'https'):
        host = parsed.netloc.split(':')[0]
        path = unquote(parsed.path.lstrip('/'))
        # Virtual-hosted style: <bucket>.s3.<region>.amazonaws.com/<key> or <bucket>.s3.amazonaws.com/<key>
        vh_match = re.match(r'^(.+?)\.s3(?:[.-][a-z0-9-]+)?\.amazonaws\.com$', host)
        if vh_match and path:
            return vh_match.group(1), path
        # Path style: s3.<region>.amazonaws.com/<bucket>/<key> or s3.amazonaws.com/<bucket>/<key>
        ps_match = re.match(r'^s3(?:[.-][a-z0-9-]+)?\.amazonaws\.com$', host)
        if ps_match and '/' in path:
            bucket, key = path.split('/', 1)
            if bucket and key:
                return bucket, key

    return None


class InstanceResolver:
    """Resolve Cassandra node hosts (IPs, FQDNs, or IDs) to EC2/SSM instance IDs."""

    def __init__(self, ssm_client, ec2_client=None, host_instance_map_path=None):
        self._ssm_client = ssm_client
        self._ec2_client = ec2_client
        self._cache = {}
        self._static_map = (
            self._load_static_map(host_instance_map_path)
            if host_instance_map_path
            else {}
        )

    @staticmethod
    def _load_static_map(path):
        map_path = pathlib.Path(path)
        if not map_path.is_file():
            raise OrchestrationError(f'SSM host_instance_map file not found: {path}')

        mapping = {}
        try:
            with map_path.open('r', encoding='utf-8') as f:
                for line_no, raw_line in enumerate(f, start=1):
                    line = raw_line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if ',' in line:
                        host_part, id_part = line.split(',', 1)
                    elif '=' in line:
                        host_part, id_part = line.split('=', 1)
                    else:
                        raise OrchestrationError(
                            f'Malformed line {line_no} in SSM host_instance_map {path}: expected host,instance_id'
                        )
                    host_key = host_part.strip()
                    inst_id = id_part.strip()
                    if not host_key or not inst_id:
                        raise OrchestrationError(
                            f'Malformed line {line_no} in SSM host_instance_map {path}: empty host or instance_id'
                        )
                    mapping[host_key] = inst_id
        except OSError as exc:
            raise OrchestrationError(f'Failed to read SSM host_instance_map {path}: {exc}') from exc

        return mapping

    @staticmethod
    def _lookup_in_index(host, index):
        if host in index:
            return index[host]
        lower = host.lower()
        if lower in index:
            return index[lower]
        return None

    @staticmethod
    def _dns_lookup(host):
        try:
            return socket.gethostbyname(host)
        except OSError:
            return None

    def _index_ec2_instances(self):
        if self._ec2_client is None:
            return {}
        index = {}
        try:
            paginator = self._ec2_client.get_paginator('describe_instances')
            pages = paginator.paginate(
                Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]
            )
            for page in pages:
                for reservation in page.get('Reservations', []):
                    for inst in reservation.get('Instances', []):
                        inst_id = inst.get('InstanceId')
                        if not inst_id:
                            continue
                        for key in ('PrivateIpAddress', 'PublicIpAddress', 'PrivateDnsName', 'PublicDnsName'):
                            val = (inst.get(key) or '').strip()
                            if val:
                                index.setdefault(val, inst_id)
                                index.setdefault(val.lower(), inst_id)
        except (ClientError, BotoCoreError) as exc:
            logging.warning('EC2 DescribeInstances failed (%s); falling back to SSM DescribeInstanceInformation', exc)
        return index

    def _index_ssm_instances(self):
        index = {}
        try:
            paginator = self._ssm_client.get_paginator('describe_instance_information')
            for page in paginator.paginate():
                for info in page.get('InstanceInformationList', []):
                    inst_id = info.get('InstanceId')
                    if not inst_id:
                        continue
                    for key in ('IPAddress', 'ComputerName'):
                        val = (info.get(key) or '').strip()
                        if val:
                            index.setdefault(val, inst_id)
                            index.setdefault(val.lower(), inst_id)
        except (ClientError, BotoCoreError) as exc:
            logging.warning('SSM DescribeInstanceInformation failed: %s', exc)
        return index

    def resolve(self, hosts):
        """Resolve hosts to instance IDs. Returns (resolved_map, error_map)."""
        resolved = {}
        errors = {}
        unresolved = []

        for host in hosts:
            if host in resolved:
                continue
            if _INSTANCE_ID_RE.match(host):
                resolved[host] = host
            elif host in self._static_map:
                resolved[host] = self._static_map[host]
            elif host in self._cache:
                resolved[host] = self._cache[host]
            else:
                unresolved.append(host)

        if not unresolved:
            return resolved, errors

        discovered = self._index_ec2_instances()
        still_unresolved = []
        dns_ips = {}
        for host in unresolved:
            inst_id = self._lookup_in_index(host, discovered)
            if inst_id is None:
                ip = self._dns_lookup(host)
                if ip:
                    dns_ips[host] = ip
                    inst_id = self._lookup_in_index(ip, discovered)
            if inst_id is not None:
                resolved[host] = inst_id
                self._cache[host] = inst_id
            else:
                still_unresolved.append(host)

        if still_unresolved:
            ssm_index = self._index_ssm_instances()
            for host in still_unresolved:
                inst_id = self._lookup_in_index(host, ssm_index)
                if inst_id is None:
                    ip = dns_ips.get(host) or self._dns_lookup(host)
                    if ip:
                        inst_id = self._lookup_in_index(ip, ssm_index)
                if inst_id is not None:
                    resolved[host] = inst_id
                    self._cache[host] = inst_id
                else:
                    errors[host] = OrchestrationError(
                        f"Could not resolve host '{host}' to an SSM instance ID"
                    )

        return resolved, errors


class SsmOrchestration(BaseOrchestration):
    """Run commands on managed EC2/SSM nodes in parallel via AWS Systems Manager."""

    def __init__(self, config, pool_size=10, ssm_client=None, ec2_client=None, s3_client=None):
        super().__init__(config, pool_size=pool_size)
        self._ssm_client = ssm_client
        self._ec2_client = ec2_client
        self._s3_client = s3_client
        self._session = None
        self._boto_config = BotocoreConfig(retries={'mode': 'adaptive', 'max_attempts': 10})
        self._resolver = None

    def _get_session(self):
        if self._session is None:
            region = (self.config.ssm.region or '').strip()
            session_kwargs = {'region_name': region} if region else {}
            self._session = boto3.Session(**session_kwargs)
        return self._session

    def _ensure_clients(self):
        if self._ssm_client is None:
            self._ssm_client = self._get_session().client('ssm', config=self._boto_config)
        if self._ec2_client is None:
            self._ec2_client = self._get_session().client('ec2', config=self._boto_config)
        if self._resolver is None:
            map_path = (self.config.ssm.host_instance_map or '').strip() or None
            self._resolver = InstanceResolver(
                self._ssm_client,
                ec2_client=self._ec2_client,
                host_instance_map_path=map_path,
            )

    def _get_s3_client(self):
        if self._s3_client is None:
            self._s3_client = self._get_session().client('s3', config=self._boto_config)
        return self._s3_client

    def _send_command(self, instance_ids, wrapped_cmd, document_name, execution_timeout,
                      delivery_timeout, output_s3_bucket, output_s3_prefix):
        kwargs = {
            'InstanceIds': instance_ids,
            'DocumentName': document_name,
            'Parameters': {
                'commands': [wrapped_cmd],
                'executionTimeout': [str(execution_timeout)],
            },
            'TimeoutSeconds': delivery_timeout,
            'MaxConcurrency': str(len(instance_ids)),
            'MaxErrors': '100%',
        }
        if output_s3_bucket:
            kwargs['OutputS3BucketName'] = output_s3_bucket
            if output_s3_prefix:
                kwargs['OutputS3KeyPrefix'] = output_s3_prefix
        response = self._ssm_client.send_command(**kwargs)
        return response['Command']['CommandId']

    def _fetch_s3_output(self, url):
        parsed = _parse_s3_url(url)
        if parsed is None:
            return None
        bucket, key = parsed
        try:
            obj = self._get_s3_client().get_object(Bucket=bucket, Key=key)
            return obj['Body'].read().decode('utf-8', errors='replace')
        except (ClientError, BotoCoreError) as exc:
            logging.warning('Failed to fetch SSM S3 output from %s: %s', url, exc)
            return None

    def _cancel_commands(self, remaining_by_cmd):
        for cmd_id, instance_ids in remaining_by_cmd.items():
            try:
                logging.warning('Cancelling in-flight SSM command %s on instances %s', cmd_id, instance_ids)
                self._ssm_client.cancel_command(CommandId=cmd_id, InstanceIds=instance_ids)
            except (ClientError, BotoCoreError) as exc:
                logging.warning('Failed to cancel SSM command %s: %s', cmd_id, exc)

    def _poll_batch(self, in_flight, send_time, poll_interval_min, poll_interval_max,
                    output_s3_bucket, success, error):
        interval = poll_interval_min
        try:
            while in_flight:
                for key in list(in_flight.keys()):
                    cmd_id, inst_id = key
                    host = in_flight[key]
                    try:
                        resp = self._ssm_client.get_command_invocation(
                            CommandId=cmd_id,
                            InstanceId=inst_id,
                        )
                    except ClientError as exc:
                        code = exc.response.get('Error', {}).get('Code', '')
                        elapsed = time.monotonic() - send_time
                        if code == 'InvocationDoesNotExist' and elapsed < INVOCATION_NOT_FOUND_GRACE_SECONDS:
                            continue
                        in_flight.pop(key)
                        logging.error('%s (%s): SSM invocation error: %s', host, inst_id, exc)
                        error.append((host, exc))
                        continue
                    except BotoCoreError as exc:
                        in_flight.pop(key)
                        logging.error('%s (%s): SSM invocation error: %s', host, inst_id, exc)
                        error.append((host, exc))
                        continue

                    status = resp.get('Status', '')
                    if status in _NON_TERMINAL_STATUSES:
                        continue

                    in_flight.pop(key)
                    stdout = resp.get('StandardOutputContent', '') or ''
                    stderr = resp.get('StandardErrorContent', '') or ''
                    response_code = resp.get('ResponseCode')

                    needs_full_output = (
                        bool(output_s3_bucket)
                        and (
                            status != 'Success'
                            or response_code != 0
                            or len(stdout) >= _INLINE_OUTPUT_TRUNCATION_THRESHOLD
                            or len(stderr) >= _INLINE_OUTPUT_TRUNCATION_THRESHOLD
                        )
                    )
                    if needs_full_output:
                        s3_stdout = self._fetch_s3_output(resp.get('StandardOutputUrl'))
                        if s3_stdout is not None:
                            stdout = s3_stdout
                        s3_stderr = self._fetch_s3_output(resp.get('StandardErrorUrl'))
                        if s3_stderr is not None:
                            stderr = s3_stderr

                    if response_code is None:
                        returncode = 0 if status == 'Success' else 1
                    else:
                        returncode = int(response_code)

                    result = CommandResult(returncode=returncode, stdout=stdout, stderr=stderr)
                    if status == 'Success' and returncode == 0:
                        success.append((host, result))
                    else:
                        logging.error(
                            '%s (%s): SSM command %s finished with status=%s returncode=%s',
                            host, inst_id, cmd_id, status, returncode
                        )
                        error.append((host, result))

                if in_flight:
                    time.sleep(interval)
                    interval = min(interval * 1.5, poll_interval_max)
        except KeyboardInterrupt:
            remaining_by_cmd = {}
            for cmd_id, inst_id in in_flight.keys():
                remaining_by_cmd.setdefault(cmd_id, []).append(inst_id)
            self._cancel_commands(remaining_by_cmd)
            raise

    def pssh_run(self, hosts, command, hosts_variables=None, ssh_client=None):
        """Run command on hosts in parallel via AWS SSM. Returns True on full success, False on any error."""
        if not hosts:
            return self._report_results(command, hosts, [], [], backend_name='SSM')

        self._ensure_clients()

        ssm_cfg = self.config.ssm
        document_name = (ssm_cfg.document_name or 'AWS-RunShellScript').strip()
        execution_timeout = int(ssm_cfg.execution_timeout)
        delivery_timeout = int(ssm_cfg.delivery_timeout)
        poll_interval_min = float(ssm_cfg.poll_interval_min)
        poll_interval_max = float(ssm_cfg.poll_interval_max)
        run_as_user = (ssm_cfg.run_as_user or '').strip()
        login_shell = medusa.utils.evaluate_boolean(ssm_cfg.login_shell)
        use_sudo = medusa.utils.evaluate_boolean(self.config.cassandra.use_sudo)
        output_s3_bucket = (ssm_cfg.output_s3_bucket or '').strip()
        output_s3_prefix = (ssm_cfg.output_s3_prefix or '').strip()
        pool_size = int(self.pool_size) if self.pool_size else 10

        logging.info(
            'Executing "%s" via AWS SSM on nodes %s with a parallelism/pool size of %s',
            command, hosts, pool_size
        )

        # Pair hosts with their substituted commands before filtering unresolved hosts.
        raw_commands = self._substitute_commands(hosts, command, hosts_variables)
        host_cmd_pairs = list(zip(hosts, raw_commands))

        resolved, resolve_errors = self._resolver.resolve(hosts)
        success = []
        error = []
        for host, _ in host_cmd_pairs:
            if host in resolve_errors:
                exc = resolve_errors[host]
                logging.error('%s: instance resolution error: %s', host, exc)
                error.append((host, exc))

        resolved_pairs = [
            (host, resolved[host], _format_command(raw_cmd, run_as_user, login_shell, use_sudo))
            for host, raw_cmd in host_cmd_pairs
            if host in resolved
        ]

        is_per_host = bool(hosts_variables and isinstance(hosts_variables, list))

        if not is_per_host:
            batch_size = min(pool_size, SSM_MAX_TARGETS_PER_COMMAND)
            for batch in divide_chunks(resolved_pairs, batch_size):
                instance_ids = [inst_id for _, inst_id, _ in batch]
                wrapped_cmd = batch[0][2]
                in_flight = {}
                send_time = time.monotonic()
                try:
                    cmd_id = self._send_command(
                        instance_ids, wrapped_cmd, document_name, execution_timeout,
                        delivery_timeout, output_s3_bucket, output_s3_prefix
                    )
                    for host, inst_id, _ in batch:
                        in_flight[(cmd_id, inst_id)] = host
                except (ClientError, BotoCoreError) as exc:
                    for host, inst_id, _ in batch:
                        logging.error('%s (%s): SSM send_command error: %s', host, inst_id, exc)
                        error.append((host, exc))
                    continue

                self._poll_batch(
                    in_flight, send_time, poll_interval_min, poll_interval_max,
                    output_s3_bucket, success, error
                )
        else:
            for batch in divide_chunks(resolved_pairs, pool_size):
                in_flight = {}
                send_time = time.monotonic()
                for host, inst_id, wrapped_cmd in batch:
                    try:
                        cmd_id = self._send_command(
                            [inst_id], wrapped_cmd, document_name, execution_timeout,
                            delivery_timeout, output_s3_bucket, output_s3_prefix
                        )
                        in_flight[(cmd_id, inst_id)] = host
                    except (ClientError, BotoCoreError) as exc:
                        logging.error('%s (%s): SSM send_command error: %s', host, inst_id, exc)
                        error.append((host, exc))

                if in_flight:
                    self._poll_batch(
                        in_flight, send_time, poll_interval_min, poll_interval_max,
                        output_s3_bucket, success, error
                    )

        return self._report_results(command, hosts, success, error, backend_name='SSM')
