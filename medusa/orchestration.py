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

from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass
import logging

import asyncssh

import medusa.utils
from medusa.storage import divide_chunks


def display_output(host_results):
    """Log stdout/stderr for a list of (host, CommandResult | SSHCompletedProcess) pairs."""
    for host, result in host_results:
        if isinstance(result, BaseException):
            logging.info("{}-error: {}".format(host, result))
            continue
        for line in result.stdout.splitlines():
            logging.info("{}-stdout: {}".format(host, line))
        for line in result.stderr.splitlines():
            logging.info("{}-stderr: {}".format(host, line))


class OrchestrationError(RuntimeError):
    """Raised when an unexpected error occurs during orchestration of commands across nodes."""
    pass


@dataclass
class CommandResult:
    """Normalized command execution result compatible with display_output()."""
    returncode: int
    stdout: str = ''
    stderr: str = ''


async def _run_one(host, command, connect_kwargs):
    """Connect to a single host and run one command. Returns SSHCompletedProcess."""
    async with asyncssh.connect(host, **connect_kwargs) as conn:
        # check=False: non-zero exit returns SSHCompletedProcess; we inspect returncode ourselves.
        return await conn.run(command, check=False)


async def _run_on_hosts(hosts, commands, connect_kwargs):
    """Fan out _run_one across all hosts in a batch concurrently.

    return_exceptions=True means a single unreachable host does not abort the batch.
    Returns a list of (host, SSHCompletedProcess | Exception) pairs.
    """
    coros = [_run_one(host, cmd, connect_kwargs) for host, cmd in zip(hosts, commands)]
    results = await asyncio.gather(*coros, return_exceptions=True)
    return list(zip(hosts, results))


class BaseOrchestration(ABC):
    """Base class for cluster command orchestration backends."""

    def __init__(self, config, pool_size=10):
        self.pool_size = pool_size
        self.config = config

    @abstractmethod
    def pssh_run(self, hosts, command, hosts_variables=None, ssh_client=None):
        """Execute a command across target hosts. Returns True on full success, False on any error."""

    @staticmethod
    def _substitute_commands(hosts, command, hosts_variables=None):
        """Return per-host commands with %s placeholders substituted when hosts_variables is a list."""
        # `{}` (empty dict, backup path) and None both mean no substitution.
        if hosts_variables and isinstance(hosts_variables, list):
            return [command % tuple(hosts_variables[idx]) for idx in range(len(hosts))]
        return [command] * len(hosts)

    @staticmethod
    def _report_results(command, hosts, success, error, backend_name='SSH'):
        """Log execution outcomes and return True if all hosts succeeded, False otherwise."""
        if len(success) == len(hosts):
            logging.info('Job executing "{}" ran and finished Successfully on all nodes.'
                         .format(command))
            return True
        elif len(error) > 0:
            logging.error('Job executing "{}" ran and finished with errors on following nodes: {}'
                          .format(command, sorted({host for host, _ in error})))
            display_output(error)
            return False
        else:
            err_msg = 'Something unexpected happened while running {} command'.format(backend_name)
            logging.error(err_msg)
            raise OrchestrationError(err_msg)


class SshOrchestration(BaseOrchestration):
    """Run commands on remote hosts in parallel over SSH via asyncssh."""

    def pssh_run(self, hosts, command, hosts_variables=None, ssh_client=None):
        """Run command on hosts in parallel via asyncssh. Returns True on full success, False on any error.

        hosts_variables: list of tuples for per-host %s substitution, or None/empty for a uniform command.
        ssh_client: accepted for API compatibility but ignored.
        Must be called from a synchronous context — asyncio.run() is used internally.
        """
        username = self.config.ssh.username if self.config.ssh.username != '' else None
        port = int(self.config.ssh.port)
        pkey = self.config.ssh.key_file if self.config.ssh.key_file != '' else None
        cert_file = self.config.ssh.cert_file if self.config.ssh.cert_file != '' else None
        keepalive_seconds = int(self.config.ssh.keepalive_seconds)
        use_pty = medusa.utils.evaluate_boolean(self.config.ssh.use_pty)
        use_login_shell = medusa.utils.evaluate_boolean(self.config.ssh.login_shell)
        use_sudo = medusa.utils.evaluate_boolean(self.config.cassandra.use_sudo)
        forward_agent = medusa.utils.evaluate_boolean(self.config.ssh.forward_agent)

        # asyncssh connect options
        connect_kwargs = {
            'port': port,
            'agent_forwarding': forward_agent,
            'keepalive_interval': keepalive_seconds,
            'request_pty': use_pty,
            'connect_timeout': 30,
        }
        kh = (self.config.ssh.known_hosts or '').strip()
        if kh and kh.lower() != 'none':
            connect_kwargs['known_hosts'] = kh
            logging.info('SSH host-key verification using file: %s', kh)
        else:
            connect_kwargs['known_hosts'] = None
            logging.info('SSH host-key verification is disabled (no known_hosts file configured)')
        if username is not None:
            connect_kwargs['username'] = username
        if pkey is not None:
            connect_kwargs['client_keys'] = [(pkey, cert_file)] if cert_file is not None else [pkey]

        success = []
        error = []

        logging.info('Executing "{command}" on following nodes {hosts} with a parallelism/pool size of {pool_size}'
                     .format(command=command, hosts=hosts, pool_size=self.pool_size))

        all_commands = self._substitute_commands(hosts, command, hosts_variables)
        host_command_pairs = list(zip(hosts, all_commands))

        for i, pair_batch in enumerate(divide_chunks(host_command_pairs, self.pool_size), start=1):
            parallel_hosts = [host for host, _ in pair_batch]
            commands = [cmd for _, cmd in pair_batch]

            # sudo wraps the command; login shell wraps everything (sudo is inside the shell).
            processed_commands = [
                "$SHELL -cl '{}'".format(('sudo ' if use_sudo else '') + cmd) if use_login_shell
                else ('sudo ' if use_sudo else '') + cmd
                for cmd in commands
            ]

            logging.debug(f'Batch #{i}: Running on nodes={parallel_hosts} parallelism={len(parallel_hosts)} '
                          f'login_shell={use_login_shell} sudo={use_sudo}')

            batch_results = asyncio.run(
                _run_on_hosts(parallel_hosts, processed_commands, connect_kwargs)
            )

            for host, result in batch_results:
                if isinstance(result, BaseException):
                    logging.error("{}: connection/execution error: {}".format(host, result))
                    error.append((host, result))
                elif result.returncode == 0:
                    success.append((host, result))
                else:
                    error.append((host, result))

        return self._report_results(command, hosts, success, error, backend_name='SSH')


class Orchestration(object):
    """Facade selecting SSH or AWS SSM orchestration based on configuration."""

    def __init__(self, config, pool_size=10):
        self.pool_size = pool_size
        self.config = config
        self._backend = self._create_backend(config, pool_size)

    @staticmethod
    def _create_backend(config, pool_size):
        ssm_cfg = getattr(config, 'ssm', None)
        if ssm_cfg is not None and medusa.utils.evaluate_boolean(ssm_cfg.enabled):
            from medusa.ssm_orchestration import SsmOrchestration
            return SsmOrchestration(config, pool_size=pool_size)
        return SshOrchestration(config, pool_size=pool_size)

    def pssh_run(self, hosts, command, hosts_variables=None, ssh_client=None):
        """Run command on hosts via the configured orchestration backend."""
        return self._backend.pssh_run(
            hosts, command, hosts_variables=hosts_variables, ssh_client=ssh_client
        )
