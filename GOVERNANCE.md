# Governance

This document describes how the lsm-rust project is governed. The project is
currently small, so governance is intentionally lightweight; it will evolve
as the community grows.

## Roles

### Contributors

Anyone who files an issue, improves documentation, reviews a pull request, or
submits code is a contributor. Contributions are governed by
[CONTRIBUTING.md](CONTRIBUTING.md) and the
[Code of Conduct](CODE_OF_CONDUCT.md).

### Maintainers

Maintainers are listed in [MAINTAINERS.md](MAINTAINERS.md). They:

- Review and merge pull requests
- Triage and label issues
- Cut releases
- Set technical direction and the roadmap
- Enforce the Code of Conduct

## Decision Making

Decisions are made by **lazy consensus**: proposals (issues, pull requests,
design discussions) are considered accepted if no maintainer objects within a
reasonable review period. When consensus cannot be reached, a simple majority
vote of maintainers decides; the lead maintainer breaks ties.

Significant changes — on-disk format changes, public API breaks, new
dependencies — should be proposed in an issue before implementation so the
discussion is recorded.

## Adding Maintainers

Any maintainer may nominate a contributor with a sustained track record of
quality contributions. The nomination is accepted by consensus of the
existing maintainers and recorded in [MAINTAINERS.md](MAINTAINERS.md).

## Removing Maintainers

Maintainers may step down at any time by moving themselves to the emeritus
section. A maintainer who is inactive for 12 months, or who violates the
Code of Conduct, may be moved to emeritus by consensus of the other
maintainers.

## Changes to Governance

Changes to this document are proposed via pull request and accepted by
consensus of the maintainers.
