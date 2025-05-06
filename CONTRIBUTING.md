# Contributing

We welcome contributions!

As of writing, I don't have an external development roadmap. I have a rough mental idea
of the next few major features to pursue.

# TL;DR dev tools

We use these python tools:

```shell
$ pip install pre-commit tox commitizen
```

The test suite will need `btrfs-progs`. Install it from your OS package manager.

```shell
$ emerge btrfs-progs
```

# Pre-commit

We use [pre-commit](https://pre-commit.com/) for auto-formatting and static checks. Run
it before each commit:

```shell
$ pre-commit
```

or better:

```shell
$ pre-commit run -a
```

# Tests

We use [tox](https://tox.wiki/) to run our test suite.

The test suite must be run as root. The test suite transparently mounts temporary btrfs
filesystem(s) to test interactions with the kernel.

If you've installed `tox` in a virtualenv, you'll need to invoke `sudo` such that it
preserves `$PATH`, like this:

```shell
$ sudo -E env PATH=$PATH tox
```

The `tox` configuration calls `pytest {posargs}`, so `pytest` arguments can be passed
from the command line:

```shell
$ sudo -E env PATH=$PATH tox -- path/to/some/test.py
```

## Golden Tests

We have some simple infrastructure for golden test data.

To update golden data:

```shell
$ sudo -E env PATH=$PATH tox -- --update-golden
```

To prevent stale files, the test suite will warn if there are any `*.golden` files which
were not referenced during a test run.

To remove unused `*.golden` files, use:

```shell
$ sudo -E env PATH=$PATH tox -- --remove-stale-golden
```

## Coverage

We require 100% test coverage. This is enforced by `tox`.

I highly recommend https://testing.googleblog.com/ as a reference for how to write
tests.

# Commit history

Currently, we enforce strictly linear commit history (no merge commits).

To help with this, please submit only **ONE** commit at a time and rebase it as needed.

(I will probably change this policy if this project gains regular contributors)

## Commit messages

We require
[conventional commit messages](https://www.conventionalcommits.org/en/v1.0.0/). Our
commit message policy is managed with
[commitizen](https://commitizen-tools.github.io/commitizen/).

Use `cz` to follow the policy. To create a commit:

```shell
$ cz commit
```

# Submitting

Submit pull requests at https://github.com/sbrudenell/btrfs2s3/pulls
