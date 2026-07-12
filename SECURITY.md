# Security Policy

## Supported Versions

Security fixes are applied to the latest release on the `main` branch.

| Version | Supported |
| ------- | --------- |
| latest `main` | ✅ |
| older releases | ❌ |

## Reporting a Vulnerability

Please do **not** report security vulnerabilities through public GitHub
issues, discussions, or pull requests.

Instead, report them privately using
[GitHub's private vulnerability reporting](https://github.com/zvdy/lsm-rust/security/advisories/new)
("Report a vulnerability" under the repository's **Security** tab).

Please include as much of the following as you can:

- A description of the vulnerability and its impact
- Steps to reproduce, or a proof-of-concept
- Affected version(s) or commit(s)
- Any suggested remediation

## What to Expect

- We will acknowledge your report within **7 days**.
- We will investigate and keep you informed of progress toward a fix.
- Once a fix is available, we will coordinate disclosure with you and credit
  you in the advisory unless you prefer to remain anonymous.

## Scope

This project is a storage engine library and demo binary. Reports about
memory safety, data corruption, or crashes triggerable through untrusted
input (e.g. corrupt SSTable/WAL files) are all in scope.
