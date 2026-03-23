# HFA Archive Incident Response

## Purpose

This runbook covers immediate response steps for suspected archive credential compromise, remote archive client misuse, or unexpected archive exposure.

## Immediate Kill Switches

### Remote client compromise

- rotate or revoke `ARCHIVE_REMOTE_CLIENT_TOKEN`
- disable the public archive remote route on the gate

### Archive runtime containment

- stop `hfa-archive-mcp.service`
- stop `hfa-archive-postgres.service`
- unmount and lock the encrypted archive volume

### Gate containment

- rotate `PASSKEY_GATE_INTERNAL_TOKEN`
- verify gate signing and audit state

## Response Steps

1. Identify the affected surface:
   - remote Mac archive client
   - OpenClaw internal archive access
   - backup artifact handling
   - encrypted storage unlock path
2. Revoke the smallest possible credential first.
3. Stop archive-facing services if live misuse is suspected.
4. Lock the encrypted archive volume if broader containment is required.
5. Verify audit logs for:
   - archive remote ticket issuance
   - archive remote calls
   - MCP proxy calls
   - admin archive actions
6. Rotate the affected secrets before re-enabling service.

## Rotation Targets

- `ARCHIVE_REMOTE_CLIENT_TOKEN`
- `HFA_ARCHIVE_UNLOCK_KEY`
- `HFA_ARCHIVE_BACKUP_PASSPHRASE`
- `PASSKEY_GATE_INTERNAL_TOKEN`
- `HFA_ARCHIVE_PG_PASSWORD` if used as a managed secret

## Recovery Validation

- confirm encrypted mount is locked before service restart
- confirm the new secrets resolve correctly from 1Password
- confirm archive read/admin actions behave according to policy
- confirm public remote archive access is restored only with the new token

## Reporting Requirements

- record what was rotated
- record what services were stopped
- record whether the encrypted volume was locked
- record whether any plaintext exposure is suspected
