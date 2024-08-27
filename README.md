# What does it do?

`btrfs2s3` maintains a *tree* of incremental backups in cloud object storage (anything
with an S3-compatible API).

Each backup is just an archive produced by `btrfs send [-p]`.

The root of the tree is a full backup. The other layers of the tree are incremental
backups.

The structure of the tree corresponds to a *schedule*.

Example: you want to keep 1 yearly, 3 monthly and 7 daily backups. It's the 4th day of
the month. The tree of incremental backups will look like this:

- Yearly backup (full)
  - Monthly backup #3 (delta from yearly backup)
  - Monthly backup #2 (delta from yearly backup)
    - Daily backup #7 (delta from monthly backup #2)
    - Daily backup #6 (delta from monthly backup #2)
    - Daily backup #5 (delta from monthly backup #2)
  - Monthly backup #1 (delta from yearly backup)
    - Daily backup #4 (delta from monthly backup #1)
    - Daily backup #3 (delta from monthly backup #1)
    - Daily backup #2 (delta from monthly backup #1)
    - Daily backup #1 (delta from monthly backup #1)

The daily backups will be short-lived and small. Over time, the new data in them will
migrate to the monthly and yearly backups.

Expired backups are automatically deleted.

The design and implementation are tailored to minimize cloud storage and API usage
costs.

`btrfs2s3` will keep one *snapshot* on disk for each *backup* in the cloud. This
one-to-one correspondence is required for incremental backups.

# Advantages

- Atomic snapshot backups.
- Up-to-the-minute backups are reasonable (even full-filesystem snapshots!)
- Simple design with no separate state files.
- Excellent fit with cheap storage classes (e.g. AWS Glacier Deep Archive).
- Excellent fit with object locking for security.
- Designed to minimize API usage and other cloud storage costs.
- Connects directly to S3, no FUSE filesystem required.

# Disadvantages

- Requires btrfs.
- Individual files can't be accessed directly. A *whole sequence* of snapshots (from
  root to leaf) must be restored on a local btrfs filesystem.

# Comparison with other tools

TODO

# Installation

`btrfs2s3` requires:

- `btrfs-progs`
- python bindings for `btrfsutil`

The `btrfsutil` python bindings are a compiled library against kernel interfaces. It
isn't distributed in PyPI. You can find it in your distribution's software repo.

[**Upcoming feature**](https://github.com/sbrudenell/btrfs2s3/issues/27): `btrfsutil`
won't be required in the future.

Ubuntu/debian:

```sh
apt-get install btrfs-progs python3-btrfsutil
```

Arch:

```sh
pacman -S btrfs-progs  # includes python3 btrfsutil
```

Alpine:

```sh
apk btrfs-progs py3-btrfs-progs
```

`btrfs2s3` is distributed on PyPI. You can install the latest version, either globally:

```sh
sudo pip install btrfs2s3
```

...or in a virtualenv.

If you use a virtualenv, you must use `--system-site-packages`. This is due to the
dependency on `btrfsutil`.

```sh
python -m virtualenv --system-site-packages v
source v/bin/activate
pip install btrfs2s3
```

You might find that `--system-site-packages` can produce strange results, like failing
to install or failures at runtime. This is because the virtualenv contains a mixture of
package versions from both the system and the env. These version mixtures are uncommon
and less tested. Python developers are not good at specifying versions of their
dependencies for some reason, so these version mixtures often break things. The simplest
fix for this is to use `pip install --ignore-installed`.

# Config

Minimal example:

```yaml
timezone: America/Los_Angeles
sources:
  - path: /path/to/your/subvolume
    snapshots: /path/to/your/snapshots
    upload_to_remotes:
      - id: aws
        preserve: 1y 3m 30d 24h
remotes:
  - id: aws
    s3:
      bucket: my-s3-bucket-name
```

Full reference:

```yaml
# Your time zone. Changing this affects your preservation policy. Always
# required.
timezone: America/Los_Angeles
# A source is a subvolume which you want to back up. btrfs2s3 will manage
# snapshots and backups of the source. At least one is required.
sources:
    # The path must be a subvolume to which you have write access.
  - path: /path/to/your/subvolume
    # The path where you want btrfs2s3 to store snapshots. btrfs2s3 will
    # automatically manage (create, rename and delete) any snapshots of the
    # source which exist under this path. Any snapshots outside of this path
    # will be ignored by btrfs2s3.
    snapshots: /path/to/your/snapshots
    # upload_to_remotes specifies where btrfs2s3 should store backups of this
    # source, and how they should be managed. At least one is required.
    # Currently, only one is allowed
    # (https://github.com/sbrudenell/btrfs2s3/issues/29)
    upload_to_remotes:
        # The id refers to the "id" field of the top-level "remotes" list.
      - id: aws
        # The preservation policy for backing up this source to this remote.
        # This applies to both snapshots and backups.
        preserve: 1y 3m 30d 24h
        # A sequence of commands to pipe the backup stream through. This is
        # useful for compressing or encrypting your backup on the host before
        # storing it in the cloud. The resulting backup will be the result of
        # a command pipeline like "btrfs send | cmd1 | cmd2 | ..."
        pipe_through:
          - [gzip]
          - [gpg, --encrypt, -r, me@example.com]
# A list of places to store backups remotely. At least one is required.
remotes:
    # A unique id for this remote. Required.
  - id: aws
    # S3 configuration. Required.
    s3:
      # The S3 bucket name. Required.
      bucket: my-s3-bucket-name
      # Optional configuration for the S3 service endpoint.
      endpoint:
        # The AWS config profile in ~/.aws/config and ~/.aws/credentials. If
        # not specified, the default config sections are used.
        profile_name: my-profile-name
        # The AWS region name. Required if not configured in ~/.aws
        region_name: us-west-2
        # Access key id and secret access key for accessing the S3 endpoint.
        # Required if not specified in ~/.aws
        aws_access_key_id: ABCXYZ...
        aws_secret_access_key: ABCXYZ...
        # The S3 endpoint URL. Required if not specified in ~/.aws
        endpoint_url: https://s3.us-west-2.amazonaws.com
        # https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/locking-api-versions.html
        # says a best practice is to pin this version to avoid unexpected API
        # changes. The latest S3 API version is "2006-03-01". Defaults to
        # unset, which means to use the latest API version.
        api_version: 2006-03-01
        # Whether to verify SSL certificates on the endpoint. This can be true
        # to verify against the default certificate store, or false to disable
        # certificate verification, or a path to a combined .pem file to
        # validate against a custom certificate store.
        verify: false
```

# Preservation Policy

The preservation policy is modeled on
[retention policies in btrbk](https://digint.ch/btrbk/doc/btrbk.conf.5.html#_retention_policy).

In configuration, format is:

`[<yearly>y] [<quarterly>q] [<monthly>m] [<weekly>w] [<daily>d] [<hourly>h] [<minutely>M] [<secondly>s]`

where:

`yearly` is how many years of yearly backups should be preserved. The first backup of
the year is the yearly backup.

`quarterly` is how many quarters of quarterly backups should be preserved. The first
backup of the quarter is the quarterly backup.

`monthly` is how many months of monthly backups should be preserved. The first backup of
the month is the monthly backup.

`weekly` is how many weeks of weekly backups should be preserved. The first backup of
the week is the weekly backup.

`daily` is how many days of daily backups should be preserved. The first backup of the
day is the daily backup.

`hourly` is how many hours of hourly backups should be preserved. The first backup of
the hour is the hourly backup.

`minutely` is how many minutes of minutely backups should be preserved. The first backup
of the minute is the minutely backup.

`secondly` is how many seconds of secondly backups should be preserved. The first backup
of the second is the secondly backup.

In `btrfs2s3`, an **interval** is a specific span of time, such as the year 2006. A
**timeframe** is a *type* of interval, such as "years" or "quarters".

The preservation policy defines both the *schedule* at which backups are created, and
the *structure* of the incremental backup tree.

*The first (longest) timeframe declared in the policy will produce full backups. The
other timeframes will produce incremental backups, whose parent is with the previous
timeframe's backup.*

For example, a policy of `1m 1d` will produce one monthly full backup and one daily
incremental backup whose parent is the monthly backup. Weekly backups aren't defined by
this policy, and so are not considered.

Currently, the preservation policy applies to both snapshots and backups.

A deeper backup tree will allow more deduplication between backups. One downside of a
deeper tree is that a single corrupted or lost backup may affect a larger number of
other backups.

It's quite reasonable to define a "deep" preservation policy with very short timeframes
like minutes or seconds. This may produce some small incremental backups, but the data
within them will eventually migrate up the tree as new longer-timeframe backups are
created. In theory, the shortest timeframe you can use in practice is equal to your
commit interval (the `-o commit=` mount option). This defaults to 30 seconds.

# Usage

```
btrfs2s3 update [options] config.yaml
```

Perform a one-time update of snapshots and backups.

For each configured source subvolume, this does the following *once*:

- Create a new read-only snapshot if source data has changed.
- Rename any read-only snapshots to conform to our naming scheme.
- Upload new backups to all configured remotes.
- Delete expired read-only snapshots.
- Delete expired backups.

By default, `btrfs2s3 update` will print a preview of what actions would be done, and
prompt for confirmation. It will refuse to run in a non-interactive terminal, unless
`--force` is supplied.

Currently, the main way to use `btrfs2s3` is to set up `btrfs2s3 update` to run in a
crontab. Note that `btrfs2s3 update` may be long-running or not, depending on whether
it's uploading a new full backup. If running from cron, you may want to protect against
multiple copies running at once, like this:

```crontab
* * * * * pgrep btrfs2s3 >/dev/null || btrfs2s3 update --force config.yaml
```

`--pretend`: Instead of performing actions, *only* print the preview of what actions
would be performed, then exit.

`--force`: Perform the actions without prompting. This is required when running in a
non-interactive terminal.

[**Upcoming feature**](https://github.com/sbrudenell/btrfs2s3/issues/15): in the future
we'll have a "daemon mode" that runs in the background and performs these updates as
needed.

# Design

`btrfs2s3` is mainly designed to solve the problem that it's too easy to delete
self-hosted backups.

Given a source subvolume, `btrfs2s3` will continuously (that is, once per
`btrfs2s3 update`) evaluate whether the source `ctransid` is more recent than any of its
read-only snapshots. If so, it creates a new read-only snapshot.

`btrfs2s3` renames read-only snapshots to conform to a fixed pattern. Currently this
naming pattern cannot be configured.

For cloud backups, `btrfs2s3` encodes metadata about the backup in the filename. This is
so all metadata can be parsed from the result of one `ListObjectsV2` call.

# Cloud storage costs

Cloud storage providers will charge a *storage cost*, which is a fixed amount per byte
(or gigabyte, etc) per month, with some free allowance.

Minimizing storage cost means storing less data, or storing data for less time.

In `btrfs2s3`, the main way to control storage costs is with the preservation policy.
Generally, preserving fewer backups will reduce storage cost.

Moreover, a *deeper backup tree* will reduce storage cost. `btrfs2s3` maintains a tree
of incremental backups. This allows you to de-duplicate data.

Let's assume:

- You have a subvolume with 100GB of data
- You rewrite 1GB of data (randomly distributed in the 100GB) per hour
- You want hourly backups for the last day

With `preserve: 24h` (hourly full backups for 24 hours), you would incur 2400GB of
storage costs.

With `preserve: 1d 24h` (daily full backups for 1 day; hourly incremental backups for 7
days), you would incur 400GB of storage costs, in the following tree:

- 1 full backup (100GB)
  - hourly incremental backup 1 (1GB)
  - hourly incremental backup 2 (2GB)
  - ...
  - hourly incremental backup 24 (24GB)

[**Upcoming feature**](https://github.com/sbrudenell/btrfs2s3/issues/30): `btrfs2s3` can
take advantage of *storage classes*. For example, `btrfs2s3` knows that a yearly backup
will not be modified for a year, so it can store this using AWS's Glacier Deep Archive
storage. This will greatly reduce storage costs on certain providers, especially for
large volumes with infrequent changes.

# Cloud API usage costs

Cloud storage providers charge for each API call. The cost varies depending on the call
and provider.

`btrfs2s3` is generally designed to minimize API calls. In particular, we *store
metadata in the filename of the backup*, which lets us retrieve metadata for all objects
in a bucket with one `ListObjectsV2` call (actually `ceil(num_objects / 1000)` calls due
to pagination).

The best way to minimize API usage costs is to run `btrfs2s3 update` less frequently.

[**Upcoming feature**](https://github.com/sbrudenell/btrfs2s3/issues/15): `btrfs2s3`
could run as a daemon rather than being invoked by cron, which would let us cache the
results of `ListObjectsV2`.

[**Upcoming feature**](https://github.com/sbrudenell/btrfs2s3/issues/32): `btrfs2s3`
will cache backup streams to disk rather than RAM, up to the maximum part size of 5GB,
to minimize multipart upload calls.

`btrfs2s3 update`:

- Calls `ListObjectsV2`.
- May call either `PutObject` or `CreateMultipartUpload` / `UploadPart` /
  `CompleteMultipartUpload`, for each new backup being uploaded.

* May call `DeleteObjects`.

# Cloud-to-host costs

Cloud storage providers charge a cost per byte transmitted *out* to the Internet.

You'll pay cloud egress cost whenever you:

- Restore your backups
- *Test* your backups (**you should regularly test your backups!**)
- Move to another provider

To minimize cloud-to-host costs, you'll need to choose a storage provider with low or
free egress costs. As of mid 2024, I'm aware of a few providers that offer free egress,
such as Backblaze B2. Meanwhile, AWS has some of the highest egress costs in the
industry.

[**Upcoming feature**](https://github.com/sbrudenell/btrfs2s3/issues/29): We will later
support uploading backups to multiple remotes. If you upload backups to multiple
providers to reduce provider risk, you can pick one provider that offers free egress,
and test your backups on that provider.

# Host-to-cloud costs

As of mid 2024, as far as I'm aware, no cloud storage provider charges for ingress (a
cost per byte received *from* the Internet). I'd be surprised if this changes, as free
ingress makes it easier for new customers to sign up.

However, your *host* may have egress costs. Many ISPs impose limits or costs on data
transfer, specifically upload data. Cloud hosting providers may have bandwidth costs.

Host-to-cloud costs are an inherent tradeoff against frequent backups. `btrfs2s3` is
designed for frequent or even continuous backups. If this incurs excessive cost for you,
you may need to configure `btrfs2s3` for less frequent backups.

[**Upcoming feature**](https://github.com/sbrudenell/btrfs2s3/issues/29): When uploading
to multiple remotes, `btrfs2s3` will by default copy backups directly to each remote. It
may be possible to upload each backup just to one remote, and use one of the various
cloud-to-cloud copy mechanisms from there, but this is not planned yet.

# Security

`btrfs2s3` manages snapshots and backups.

It requires Linux user permissions to *create and delete* snapshots.

It requires `CAP_SYS_ADMIN` to perform `btrfs send`.

It requires access to *create and delete* objects on S3.

- `btrfs2s3 update`: requires `s3:ListBucket`, `s3:PutObject` and `s3:DeleteObject`.

You can run `btrfs2s3` as a normal Linux user, rather than root. A few things to keep in
mind:

- Running as non-root isn't officially supported yet, as
  [the test suite doesn't cover it](https://github.com/sbrudenell/btrfs2s3/issues/49).
- The filesystem must be mounted with `-o user_subvol_rm_allowed`, to delete snapshots.
- The `btrfs2s3` user must have write permission to the snapshot directory, and read
  permissions to the source subvolumes.
- The `btrfs2s3` user must also have `CAP_SYS_ADMIN` to perform `btrfs send`.
- The `btrfs2s3` user should presumably be separate from the subvolume owner. Otherwise,
  the subvolume owner could read S3 secrets from `btrfs2s3`'s config files, or modify
  the config to set `pipe_through` to something malicious.
- It also makes sense to have a distinct S3 bucket and S3 access key for each Linux user
  running `btrfs2s3`.

One disadvantage of running `btrfs2s3` with distinct Linux users / buckets / access keys
is that `btrfs2s3 update` will issue one `ListObjectsV2` call for each user / bucket.
This can increase your cloud API usage costs. When running as a single user (root or
otherwise), `btrfs2s3` can be configured to back up multiple sources to a single remote,
and only call `ListObjectsV2` once per update of all sources.

Another disadvantage of running multiple instances of `btrfs2s3` is that they may create
a "thundering herd". If two instances are configured with similar preservation policies,
they may both start uploading new full backups at the same time, creating congestion.
They may also start using large amounts of temporary storage at the same time (see
[quirks when uploading to S3](#quirks-when-uploading-to-s3)).

# Immutable backups

[**Upcoming feature**](https://github.com/sbrudenell/btrfs2s3/issues/31): `btrfs2s3`
could make backups temporarily immutable. The idea is to use the "object lock" S3
functionality to lock each object until it would be expired by the backup schedule. The
lock could even be extended by policy, for example to keep every backup for an extra
month after it would otherwise expire. This could provide protection against ransomware,
or even accidental deletion of backups.

# Encryption

`btrfs2s3` primarily supports encrypting (and compressing) backups via the
`pipe_through` option. The intent is to use something like:

```yaml
pipe_through:
  - [gzip]
  - [gpg, --encrypt, -r, me@example.com]
```

`btrfs2s3` doesn't currently support "server-side encryption", nor is this planned. It
appears to be access control with extra steps. If someone wants this feature, they will
need to convince me it's meaningful.

# Quirks when uploading to S3

The data stream produced by `btrfs send` (with or without `pipe_through`) has
unpredictable length and is not seekable. The S3 API is poorly-designed for this case.

Currently, when uploading to S3, `btrfs2s3` will upload backups in 5 GiB chunks. Each
chunk will be written to temporary disk storage before uploading.

[**Known issue**](https://github.com/sbrudenell/btrfs2s3/issues/54): We will currently
fail to upload backup streams larger than the provider's maximum object size (5 TiB for
AWS).

[**Known issue**](https://github.com/sbrudenell/btrfs2s3/issues/55): We will currently
fail to upload zero-length backup streams. `btrfs send` does not produce these, but this
may occur depending on `pipe_through`.

[**Known issue**](https://github.com/sbrudenell/btrfs2s3/issues/52): Copying data to
temporary storage is currently done in python, which is slow. We could use `sendfile()`
/ `splice()` to speed it up.

[**Upcoming feature**](https://github.com/sbrudenell/btrfs2s3/issues/53): We will have
an option to change the temporary storage location (including `/dev/shm` for storing it
in RAM).

[**Upcoming feature**](https://github.com/sbrudenell/btrfs2s3/issues/62): In the future
we'll only buffer the first 5 GiB part of a backup stream and directly stream the
remaining data, rather than buffering the entire stream in chunks.

For background:

The S3 API provides two methods for uploading data:

- `PutObject`, for "small" objects
- `CreateMultipartUpload` / `UploadPart` / `CompleteMultipartUpload`, for "large"
  objects

The maximum length of data that can be uploaded with `PutObject` or `UploadPart` is 5
GiB. The maximum size of an object is 5 TiB. The maximum number of parts in a multipart
upload is 10000.

As of writing, AWS charges money for each S3 API call, including each of `PutObject`,
`CreateMultipartUpload`, `UploadPart` and `CompleteMultipartUpload`. Multipart uploads
thus incur higher costs per object than a `PutObject` call. Multipart uploads with
smaller parts (anything under the maximum of 5 GiB) further increase costs. Some cloud
storage providers do not charge money for these calls (Backblaze B2, as of writing).

It would be simple to always use multipart uploads. However, `btrfs2s3` is designed for
frequent uploads of small, short-lived backups, so this strategy would incur extra API
usage costs in the common case. This amounts to paying extra money to AWS as thanks for
their failure to design a coherent API, which is obviously abhorrent.

It would also be simple to always use `PutObject`, and split large (> 5 GiB) backup
streams across multiple objects (we must do this anyway for streams larger than the
maximum object size of 5 TiB). However we also want to minimize the number of total
objects in a bucket, to minimize calls to `ListObjectsV2`. Therefore we want to maximize
the size of each object and minimize splitting.

`PutObject` and `UploadPart` are HTTP PUT requests under the hood, where the request
body is the object data, thus they *can* accommodate unbounded data streams without
temporary storage. However there is no way to start with a `PutObject` call and append
data to an object (this would especially not make sense with immutable objects). If we
want to upload an unbounded stream *and* minimize API calls, we must choose the correct
call before beginning the upload of the first 5 GiB part.

Thus: if we want to upload an unbounded, non-seekable stream to S3 while minimizing API
usage costs, we must buffer the first 5 GiB of data. This is awkward, but it is
financially incentivized by AWS.

`btrfs2s3` buffers to disk by default. As of writing, a program using 5 GiB of RAM for
temporary storage would be considered unfriendly to users.

# Could this work with zfs?

Probably.

But I don't like the design of zfs, so I don't intend to support this.

# Could this work with self-hosted object storage?

Probably.

But the main goal of `btrfs2s3` is to hand off backups to a third party. `btrfs2s3` was
written to reduce the risks of self-hosting backups:

- *Durable* self-hosted backups are hard, and/or expensive
- Accidentally deleting all your backups is easy. How do you protect yourself *from*
  yourself?

If you want to backup a btrfs filesystem, and you want to host a backup computer to do
so, the best choice is probably to have another btrfs filesystem on the backup computer,
and manage backups with [btrbk](https://digint.ch/btrbk/).

# Timezones

`btrfs2s3` does not rely on the system timezone. It requires an explicit timezone to be
specified in config (you can use `UTC` if desired).

Further, **be very careful if you ever need to change timezones!**

This is because preservation policies are only meaningful in a given timezone. A change
in timezone can radically change which backups will be kept.

`btrfs2s3` never explicitly marks a backup as "yearly", "hourly" or otherwise. Instead,
it checks the ctime (modification time) of the backup against your preservation policy,
in your timezone.

Recall that "the first backup of a year is the yearly backup", and that `btrfs2s3` is
designed to continuously create backups. So, your yearly backup is likely to have a
ctime (modification time) of midnight on January 1st in your timezone. But "midnight"
and "January 1st" are only meaningful in a timezone, so if you ask `btrfs2s3` to change
to a new timezone, that same ctime may now be 23:00 on December 31st of the previous
year, and `btrfs2s3` may delete it!

A change to the system timezone almost never means the user wants to change their backup
policy too. Thus, we require an explicitly configured timezone.
