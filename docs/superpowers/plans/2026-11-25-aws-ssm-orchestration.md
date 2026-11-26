# AWS SSM Orchestration Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add AWS Systems Manager (SSM) (`AWS-RunShellScript`) as a first-class alternative orchestration backend to SSH (`asyncssh`) in `cassandra-medusa`, keeping SSH as the default and preserving the existing `Orchestration.pssh_run` interface for `backup_cluster` and `restore_cluster`.

**Architecture:**
- Add an `[ssm]` configuration section in `medusa.ini` (`enabled = False` by default) plus a `--ssm-enabled` CLI override in `medusacli.py`.
- Reuse the standard `boto3` credential/region chain (with an optional `[ssm] region` override; no multi-region auto-discovery).
- Refactor `medusa/orchestration.py` into `BaseOrchestration` (abstract base + shared batch substitution and reporting helpers), `SshOrchestration`, and an `Orchestration` facade class that delegates to `SsmOrchestration` or `SshOrchestration` based on `config.ssm.enabled`.
- Implement `SsmOrchestration` and `InstanceResolver` in `medusa/ssm_orchestration.py` to handle host-to-instance-ID resolution, `AWS-RunShellScript` batching (chunked by `min(pool_size, 50)` for uniform commands and per-host bounded by `pool_size` for `hosts_variables`), bounded backoff polling with `InvocationDoesNotExist` grace handling, optional S3 output retrieval beyond SSM's 24KB inline limit, and `ssm.cancel_command` cleanup on `KeyboardInterrupt`.

**Tech Stack:** Python 3.10+, `boto3` / `botocore` (`1.40.41` in `pyproject.toml`), `click`, `unittest` / `pytest`, `flake8`.

---

## Global Constraints & Pitfalls

- **Backward-compatible `MedusaConfig`:** `MedusaConfig` in `medusa/config.py` is instantiated directly with positional/keyword arguments across ~20 unit test files (`tests/backup_cluster_test.py`, `tests/restore_cluster_test.py`, `tests/orchestration_test.py`, `tests/cassandra_utils_test.py`, etc.) without passing `ssm`. Add `'ssm'` as the **last** field on `MedusaConfig` with `defaults=(None,)` so all existing callers continue to work without modification.
- **`tests/orchestration_test.py` monkey-patching:** `OrchestrationTest.setUp()` monkey-patches `medusa.orchestration._run_one`. Keep `_run_one` and `_run_on_hosts` at module scope in `medusa/orchestration.py`, and ensure `SshOrchestration` invokes `_run_one` through module scope (via `_run_on_hosts`) so the test patch stays effective.
- **`create_autospec(Orchestration)` in `tests/backup_cluster_test.py`:** Keep `Orchestration` as a concrete class with `pssh_run(self, hosts, command, hosts_variables=None, ssh_client=None)` rather than replacing `Orchestration` with a plain factory function.
- **CLI flag collision in `_override_config_with_args`:** `medusa/config.py`'s `_override_config_with_args` matches Click kwargs by unprefixed field name across all `CONFIG_SECTIONS` (`enabled` already collides across `grpc`, `kubernetes`, `logging`, and would collide with `ssm`). Name the CLI option `--ssm-enabled` (`ssm_enabled`, `is_flag=True, default=None`) and apply it in a dedicated `_handle_ssm_settings(config, args)` helper so omitting `--ssm-enabled` never overrides `medusa.ini`.
- **Flake8 rules (`tox.ini`):** `max-line-length = 120`, `select = E,W,F`, ignore `W503,E402,E713,E231,E201,E241,E702`.

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `medusa/config.py` | Modify | Add `SSMConfig`, `ssm` field on `MedusaConfig` (`defaults=(None,)`), `_build_default_config()` defaults, `_handle_ssm_settings()`, conditional `[ssh]` validation, and SSM timeout bounds validation |
| `medusa/medusacli.py` | Modify | Add `--ssm-enabled` top-level CLI option |
| `medusa/orchestration.py` | Modify | Extract `CommandResult`, `BaseOrchestration`, `SshOrchestration`, and keep `Orchestration` as delegating facade |
| `medusa/ssm_orchestration.py` | Create | Implement `InstanceResolver` and `SsmOrchestration` |
| `tests/ssm_orchestration_test.py` | Create | Unit tests for SSM resolution, command formatting, batching, polling, S3 output, and cancellation |
| `tests/config_test.py` | Modify | Unit tests for `[ssm]` config defaults, `--ssm-enabled` override, conditional `[ssh]` checks, and timeout validation |
| `medusa-example.ini` | Modify | Document commented `[ssm]` section and defaults |
| `docs/Configuration.md` | Modify | Document `[ssm]` options, `--ssm-enabled`, and required AWS IAM permissions |

---

### Task 1: Configuration & CLI Support

**Files:**
- Modify: `medusa/config.py`
- Modify: `medusa/medusacli.py`

**Interfaces:**
- Produces: `SSMConfig` namedtuple with fields:
  - `enabled` (`'False'`)
  - `region` (`''`)
  - `document_name` (`'AWS-RunShellScript'`)
  - `execution_timeout` (`'86400'`)
  - `delivery_timeout` (`'600'`)
  - `poll_interval_min` (`'2'`)
  - `poll_interval_max` (`'30'`)
  - `run_as_user` (`''`)
  - `login_shell` (`'False'`)
  - `host_instance_map` (`''`)
  - `output_s3_bucket` (`''`)
  - `output_s3_prefix` (`''`)
- Produces: `MedusaConfig` with `'ssm'` appended and `defaults=(None,)`.
- Produces: `--ssm-enabled` CLI flag (`is_flag=True, default=None`) on `cli` in `medusa/medusacli.py`.

- [ ] **Step 1: Add `SSMConfig`, update `MedusaConfig` and `CONFIG_SECTIONS` in `medusa/config.py`**

```python
SSMConfig = collections.namedtuple(
    'SSMConfig',
    [
        'enabled', 'region', 'document_name', 'execution_timeout', 'delivery_timeout',
        'poll_interval_min', 'poll_interval_max', 'run_as_user', 'login_shell',
        'host_instance_map', 'output_s3_bucket', 'output_s3_prefix'
    ]
)

MedusaConfig = collections.namedtuple(
    'MedusaConfig',
    [
        'file_path',  # Store a specific config file path, None for default file
        'storage', 'cassandra', 'ssh', 'checks', 'monitoring', 'logging', 'grpc', 'kubernetes', 'ssm'
    ],
    defaults=(None,)
)
```

Register `'ssm': SSMConfig` in `CONFIG_SECTIONS` and add defaults in `_build_default_config()`:

```python
    config['ssm'] = {
        'enabled': 'False',
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
```

- [ ] **Step 2: Add `_handle_ssm_settings(config, args)` and wire into `parse_config()`**

Call `_handle_ssm_settings(config, args)` right after `_handle_k8s_and_grpc_settings(config, args)`:

```python
def _handle_ssm_settings(config, args):
    """Apply CLI --ssm-enabled override without colliding with other sections' 'enabled' fields."""
    if args.get('ssm_enabled') is True:
        config.set('ssm', 'enabled', 'True')
```

- [ ] **Step 3: Populate `ssm` and update validation in `load_config()`**

Pass `ssm=_namedtuple_from_dict(SSMConfig, config['ssm'])` when constructing `MedusaConfig`.
Skip required `[ssh]` field validation (`username`, `key_file`) when SSM is enabled, and validate SSM timeout bounds when SSM is enabled:

```python
    ssm_enabled = evaluate_boolean(medusa_config.ssm.enabled) if medusa_config.ssm else False

    if not ssm_enabled:
        for field in ['username', 'key_file']:
            if getattr(medusa_config.ssh, field) is None:
                logging.error('Required configuration "{}" is missing in [ssh] section.'.format(field))
                sys.exit(2)
    else:
        try:
            exec_timeout = int(medusa_config.ssm.execution_timeout)
            delivery_timeout = int(medusa_config.ssm.delivery_timeout)
        except (TypeError, ValueError):
            logging.error('SSM execution_timeout and delivery_timeout must be integers.')
            sys.exit(2)
        if not (30 <= exec_timeout <= 172800):
            logging.error('SSM execution_timeout must be between 30 and 172800 seconds.')
            sys.exit(2)
        if not (30 <= delivery_timeout <= 3600):
            logging.error('SSM delivery_timeout must be between 30 and 3600 seconds.')
            sys.exit(2)
```

- [ ] **Step 4: Add `--ssm-enabled` option to `medusa/medusacli.py`**

On the top-level `@click.group` `cli` function (alongside `--ssh-username` and `--ssh-key-file`):

```python
@click.option('--ssm-enabled', help='Use AWS SSM instead of SSH for cluster orchestration',
              is_flag=True, default=None)
```

---

### Task 2: Refactor `Orchestration` into Backend Interface + Facade

**File:** `medusa/orchestration.py`

**Interfaces:**
- Produces: `CommandResult(returncode: int, stdout: str, stderr: str)` dataclass
- Produces: `BaseOrchestration(ABC)` with `_prepare_batch_commands(...)`, `_report_results(...)`, and abstract `pssh_run(self, hosts, command, hosts_variables=None, ssh_client=None)`
- Produces: `SshOrchestration(BaseOrchestration)`
- Preserves: `Orchestration(config, pool_size=10)` with `pssh_run(...) -> bool`

- [ ] **Step 1: Define `CommandResult` and `BaseOrchestration` in `medusa/orchestration.py`**

```python
@dataclass
class CommandResult:
    """Normalized command execution result compatible with display_output()."""
    returncode: int
    stdout: str = ''
    stderr: str = ''
```

Extract shared logic into `BaseOrchestration(ABC)`:
- `_prepare_batch_commands(self, command, parallel_hosts, hosts_variables, batch_index)`: performs the per-host `command % tuple(hosts_variables[batch_start + j])` substitution when `hosts_variables` is a non-empty `list`, or `[command] * len(parallel_hosts)` otherwise.
- `_report_results(self, command, hosts, success, error)`: logs success/error summary, calls `display_output(error)`, raises `OrchestrationError` if neither `success` nor `error` recorded anything (when `hosts` is non-empty), and returns `bool`. (Also handle empty `hosts` cleanly if passed.)

- [ ] **Step 2: Extract `SshOrchestration(BaseOrchestration)`**

Move the `asyncssh` option extraction and batch execution loop into `SshOrchestration.pssh_run(...)`. Keep module-level `async def _run_one(...)` and `async def _run_on_hosts(...)` in `medusa/orchestration.py` so `tests/orchestration_test.py` continues to intercept `_run_one`.

- [ ] **Step 3: Turn `Orchestration` into a delegating facade**

```python
class Orchestration(object):
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
        return self._backend.pssh_run(
            hosts, command, hosts_variables=hosts_variables, ssh_client=ssh_client
        )
```

Using a deferred import for `SsmOrchestration` avoids circular imports (`ssm_orchestration.py` imports `BaseOrchestration`, `CommandResult`, `OrchestrationError`, `display_output` from `medusa.orchestration`).

---

### Task 3: Host-to-Instance-ID Resolution (`InstanceResolver`)

**File:** `medusa/ssm_orchestration.py` (Create)

**Interfaces:**
- Produces: `InstanceResolver(ssm_client, ec2_client=None, host_instance_map_path=None)`
- Produces: `resolve(hosts: list[str]) -> tuple[dict[str, str], dict[str, Exception]]` returning `(resolved_map, unresolved_errors)`

- [ ] **Step 1: Implement `InstanceResolver` with precedence & caching**

Resolution order per host:
1. **Direct ID passthrough:** Regex `^(i|mi)-[0-9a-f]+$` matches immediately; skip API lookup.
2. **Static mapping file (`host_instance_map`):**
   - Loaded once in `__init__` (or lazily before resolution) if `host_instance_map_path` is non-empty.
   - Raise `OrchestrationError` immediately if the configured path does not exist or cannot be read.
   - Parse lines stripped of whitespace, ignoring empty lines and `#` comments; support both `host,instance_id` and `host=instance_id`.
3. **In-memory cache:** Reuse cached `host -> instance_id` entries across batches/calls on the same resolver instance.
4. **Dynamic EC2 `DescribeInstances` (for any still-unresolved hosts):**
   - Paginate `ec2_client.get_paginator('describe_instances').paginate(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])`.
   - Index `PrivateIpAddress`, `PublicIpAddress`, `PrivateDnsName`, `PublicDnsName` -> `InstanceId`.
   - Catch `botocore.exceptions.ClientError` (`UnauthorizedOperation`, `AccessDenied`, etc.): log a warning and continue to SSM fallback rather than failing hard.
5. **Dynamic SSM `DescribeInstanceInformation` fallback (for any still-unresolved hosts):**
   - Paginate `ssm_client.get_paginator('describe_instance_information').paginate()`.
   - Index `IPAddress` and `ComputerName` (exact and lowercase) -> `InstanceId` (handles `mi-*` hybrid activations and IAM policies without `ec2:DescribeInstances`).
6. **DNS fallback:** For any remaining hostname, attempt `socket.gethostbyname(host)` and check against the discovered IP index (suppressing `socket.gaierror`).
7. **Unresolved hosts:** Return per-host `OrchestrationError(f"Could not resolve host '{host}' to an SSM instance ID")` in `unresolved_errors` so `SsmOrchestration.pssh_run` records them as per-host errors and returns `False` instead of aborting the rest of the batch.

---

### Task 4: `SsmOrchestration` Execution, Polling, Output & Cancellation

**File:** `medusa/ssm_orchestration.py`

**Interfaces:**
- Produces: `SsmOrchestration(BaseOrchestration)`
  - `__init__(self, config, pool_size=10, ssm_client=None, ec2_client=None, s3_client=None)` (optional injected clients simplify unit testing)
  - `pssh_run(self, hosts, command, hosts_variables=None, ssh_client=None) -> bool`

- [ ] **Step 1: Boto3 session/client initialization with adaptive retries**

- Build `boto3.Session()` using standard credential/region chain (`region_name=ssm_cfg.region` only when `ssm_cfg.region` is non-empty).
- Apply `botocore.config.Config(retries={'mode': 'adaptive', 'max_attempts': 10})` to handle SSM `ThrottlingException`.
- Lazily create `s3_client` only when `output_s3_bucket` is configured and S3 output retrieval is needed.

- [ ] **Step 2: Implement `_format_command(command, run_as_user, login_shell, use_sudo)`**

```python
def _format_command(command, run_as_user, login_shell, use_sudo):
    if not run_as_user:
        # SSM agent runs as root by default; do not prepend sudo.
        if login_shell:
            return f'bash -lc {shlex.quote(command)}'
        return command

    inner = f'sudo {command}' if use_sudo else command
    if login_shell:
        return f'su - {shlex.quote(run_as_user)} -c {shlex.quote(inner)}'
    return f'su {shlex.quote(run_as_user)} -c {shlex.quote(inner)}'
```

- [ ] **Step 3: Implement batching & `send_command` dispatch**

- Determine whether commands are uniform (`not hosts_variables or isinstance(hosts_variables, dict)`) or per-host (`isinstance(hosts_variables, list)`).
- **Uniform path:**
  - Chunk resolved hosts by `min(self.pool_size, 50)` (SSM's hard limit of 50 `InstanceIds` per `SendCommand`).
  - Issue one `ssm.send_command(...)` per chunk with:
    - `InstanceIds=batch_instance_ids`
    - `DocumentName=document_name`
    - `Parameters={'commands': [wrapped_cmd], 'executionTimeout': [str(execution_timeout)]}`
    - `TimeoutSeconds=delivery_timeout`
    - `MaxConcurrency=str(len(batch_instance_ids))`
    - `MaxErrors='100%'` (ensure one failed host does not abort siblings in the batch)
    - Optional `OutputS3BucketName` / `OutputS3KeyPrefix` when `output_s3_bucket` is non-empty.
- **Per-host path (`hosts_variables` is a `list`):**
  - Chunk resolved hosts by `self.pool_size`.
  - Use `_prepare_batch_commands` to format each host's command, wrap via `_format_command`, and issue one `ssm.send_command(InstanceIds=[instance_id], ...)` per host in the batch.
- Handle `send_command` `ClientError` / `BotoCoreError` per command/host by recording the exception in `error` so sibling commands in the batch still execute/complete.

- [ ] **Step 4: Implement bounded backoff polling & S3 output retrieval**

- Track `in_flight` map of `(command_id, instance_id) -> host` for the current batch.
- Poll loop with `interval = poll_interval_min`, multiplying by `1.5` each round up to `poll_interval_max`:
  - Call `ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)`.
  - If `ClientError` has code `'InvocationDoesNotExist'`:
    - Within grace period (e.g. `60` seconds since `send_command`), treat as `Pending` and continue polling.
    - Past grace period, record as error.
  - Inspect `Status`:
    - Non-terminal (`Pending`, `InProgress`, `Delayed`, `Cancelling`): remain in `in_flight`.
    - `Success`: fetch stdout/stderr (from S3 if configured and inline output truncated or `ResponseCode != 0`), construct `CommandResult(returncode=response_code, stdout=..., stderr=...)`; add to `success` if `response_code == 0` else `error`.
    - `Failed`, `TimedOut`, `Cancelled`: fetch stdout/stderr (from S3 if configured, falling back to inline content on S3 read failure), construct `CommandResult(returncode=response_code if response_code is not None else 1, stdout=..., stderr=...)`, add to `error`.
- Helper `_fetch_s3_output(url)` parses S3 HTTPS/S3 URLs (`s3://bucket/key` or `https://s3...`) and reads via `s3_client.get_object(...)`, returning `None` on failure so inline output is preserved.

- [ ] **Step 5: Implement `KeyboardInterrupt` cancellation**

Wrap the batch polling loop in `try ... except KeyboardInterrupt:`:
- Group remaining `in_flight` targets by `command_id` and call `ssm.cancel_command(CommandId=command_id, InstanceIds=instance_ids)` for each.
- Log warnings and continue if `cancel_command` fails for an already-completed or unreachable command.
- Re-raise `KeyboardInterrupt` after cancellation attempts finish.

---

### Task 5: Unit Tests

**Files:**
- Create: `tests/ssm_orchestration_test.py`
- Modify: `tests/config_test.py`

- [ ] **Step 1: Add `[ssm]` config tests in `tests/config_test.py`**

Test:
- Default `[ssm]` values when loading `tests/resources/config/medusa.ini` (`enabled == 'False'`, `execution_timeout == '86400'`, etc.).
- `--ssm-enabled` (`args={'ssm_enabled': True}`) overrides config without mutating `grpc.enabled` or `kubernetes.enabled`.
- When `ssm_enabled=True` and `[ssh]` fields are omitted/None, `load_config()` succeeds without triggering SSH validation exit.
- Out-of-bounds `execution_timeout` (< 30 or > 172800) or non-integer timeout causes `SystemExit(2)` when SSM is enabled.

- [ ] **Step 2: Create `tests/ssm_orchestration_test.py` (fully offline with `unittest.mock` / `botocore.stub.Stubber`)**

Cover:
1. **Facade dispatch:** `Orchestration` selects `SshOrchestration` when `ssm` is `None` or `enabled='False'`, and `SsmOrchestration` when `enabled='True'`.
2. **`InstanceResolver`:**
   - Direct `i-*` and `mi-*` IDs pass through with zero AWS calls.
   - Static `host_instance_map` file takes precedence over EC2/SSM, handles comments/blank lines and `host,id` / `host=id`, and raises `OrchestrationError` if missing.
   - EC2 `DescribeInstances` matches `PrivateIpAddress`, `PublicIpAddress`, `PrivateDnsName`, `PublicDnsName`.
   - SSM `DescribeInstanceInformation` resolves `IPAddress`/`ComputerName` when EC2 returns `UnauthorizedOperation` (or `mi-*` instances).
   - Unresolved host returns per-host error and causes `pssh_run` to return `False` without aborting resolved siblings.
   - Repeated calls reuse cached resolution without re-querying APIs.
3. **`_format_command`:**
   - `run_as_user=''` + `login_shell=False` + `use_sudo=True` -> raw command (no `sudo`).
   - `run_as_user=''` + `login_shell=True` -> `bash -lc '...'` (properly quoting embedded single quotes).
   - `run_as_user='cassandra'` + `login_shell=True` + `use_sudo=True` -> `su - cassandra -c 'sudo ...'`.
   - `run_as_user='cassandra'` + `login_shell=False` + `use_sudo=False` -> `su cassandra -c '...'`.
4. **Batching & `SendCommand`:**
   - Uniform commands across 75 hosts with `pool_size=100` chunk into batches of 50 + 25 (two `send_command` calls).
   - `hosts_variables` across 3 hosts with `pool_size=2` issues 3 individual `send_command` calls across 2 batches with correct `%s` substitutions.
5. **Polling & output:**
   - `InvocationDoesNotExist` followed by `InProgress` followed by `Success` (`ResponseCode=0`) succeeds.
   - Partial failure (`Failed` / `TimedOut` on 1 of 3 hosts) still collects results for all 3 hosts and returns `False`.
   - S3 output retrieval reads full logs when `output_s3_bucket` is set and gracefully falls back to inline output on S3 error.
6. **Cancellation:**
   - `KeyboardInterrupt` during polling triggers `ssm.cancel_command` for all in-flight `(CommandId, InstanceIds)` pairs and re-raises.

---

### Task 6: Documentation

**Files:**
- Modify: `medusa-example.ini`
- Modify: `docs/Configuration.md`

- [ ] **Step 1: Add commented `[ssm]` section to `medusa-example.ini` and `docs/Configuration.md`**

Document each `[ssm]` setting, its default, and purpose (including `output_s3_bucket` for logs exceeding SSM's 24KB inline limit, and `host_instance_map` for static overrides).

- [ ] **Step 2: Document `--ssm-enabled` and IAM permissions in `docs/Configuration.md`**

Include the required control-node IAM permissions:
- `ssm:SendCommand`
- `ssm:GetCommandInvocation`
- `ssm:CancelCommand`
- `ssm:DescribeInstanceInformation`
- Optional: `ec2:DescribeInstances` (for dynamic EC2 IP/DNS-to-instance-ID resolution)
- Optional: `s3:GetObject` / `s3:PutObject` (if `output_s3_bucket` is configured)

---

### Task 7: Verification

- [ ] **Step 1: Run `flake8` matching `tox.ini` rules**

```bash
poetry run flake8 . --ignore=W503,E402,E713,E231,E201,E241,E702 \
  --exclude=medusa/service/grpc/medusa_pb2.py,medusa/service/grpc/medusa_pb2_grpc.py,.tox,.venv,venv,build,dist,debian
```

- [ ] **Step 2: Run targeted and full unit tests**

```bash
poetry run pytest -v tests/orchestration_test.py tests/ssm_orchestration_test.py tests/config_test.py tests/backup_cluster_test.py tests/restore_cluster_test.py
poetry run pytest -v tests/
```
