# MaxMind inputs on S3 with per-job RAM copies

Store a Country or City `.mmdb` in the existing raw S3 mount, for example:

```text
s3://netds3o:aweis/ipid-2/raw/maxmind/2026-11-01/GeoLite2-Country.mmdb
/mnt/data01/ipid-analysis/data/raw/maxmind/2026-11-01/GeoLite2-Country.mmdb
```

The database must already be uploaded. This feature does not download databases
from MaxMind, acquire credentials, change mount settings, or modify S3 objects.
Use a dated/versioned key and retain its exact SHA-256 for the whole campaign.
For example, compute the digest of the original `.mmdb` with `sha256sum` before
uploading. Keep that same file for retries rather than updating it in place.

## Configuration

Add the following settings to the environment sourced before starting the
analysis worker (for example `/etc/default/ipid-geographic-analysis`):

```bash
IPID_MAXMIND_DB_S3=s3://netds3o:aweis/ipid-2/raw/maxmind/2026-11-01/GeoLite2-Country.mmdb
IPID_MAXMIND_DB_VERSION=2026-11-01
IPID_MAXMIND_DB_SHA256='<64-hex-digit-SHA-256-of-that-file>'
IPID_MAXMIND_DB_MAX_BYTES=268435456
```

Replace the placeholder before sourcing this example; it is not a usable digest.
S3 staging requires `IPID_ANALYSIS_STORAGE_MODE=mounted-s3`,
`IPID_ANALYSIS_SCRATCH_STORAGE=ram`, the existing working mounts, and a matching
prefix in `IPID_ANALYSIS_S3_ROOTS`. It performs no fallback to disk or unpinned
reference files if explicit S3 configuration is invalid. No new scheduler
resource profile is needed. Ansible deployments must render these settings into
the worker environment; this analysis-repository change does not edit Ansible.

A valid explicit `--maxmind-db` path takes precedence. An existing file named by
`IPID_MAXMIND_DB` also overrides S3 staging; leave that variable unset to use S3.
When S3 is not configured, the existing `references/GeoLite2-Country.mmdb`,
`GeoLite2-City.mmdb`, `GeoIP2-Country.mmdb`, `GeoIP2-City.mmdb` search is unchanged.
`ipid_geographic_skip_external_roles` / `IPID_ANALYSIS_SKIP_ITDK` /
`IPID_ANALYSIS_SKIP_RIPE` do not disable MaxMind continent plots.

## Storage and resource accounting

On first use within a job, the source is read through the configured S3 mount
into `<job-scratch>/maxmind/<sha256>.mmdb`. With RAM scratch this is beneath the
private `/dev/shm/ipid-analysis-<uid>-<checkout-hash>/` tree. The code reserves
space before copying, bounds each read, checks the actual remaining job/global
scratch allowance during transfer, verifies size and SHA-256, and validates
the MMDB type before atomically exposing the copy. Interrupted or invalid
transfers remove only their own partial file. The source is never modified.

The default maximum input size is 256 MiB, adjustable via
`IPID_MAXMIND_DB_MAX_BYTES`. This ceiling does not add RAM or enlarge a job's
scratch allocation. A job fails if it cannot fit the database alongside its
other temporary data. Copying uses bounded buffers rather than loading the
whole file into Python memory. Lookups use the RAM copy, not the S3 mount.

Multiple comparisons in a job reuse the verified copy. Simultaneous jobs own
separate copies so the existing scheduler/watchdog accounts for each copy in
the correct job. Scheduled-job cleanup removes the copy with the stopped job's
scratch. Standalone CLI use retains its scratch copy for reuse in that scratch
directory; RAM copies disappear when the VM is rebooted. S3 remains durable.

## Provenance

The probing-interval-by-continent figure JSON retains `maxmind_database` and
adds `maxmind_provenance` with `source_uri`, `version`, `sha256`, `size_bytes`,
`database_type`, and `build_epoch` for staged inputs. Local database overrides
record `source_path`, SHA-256, size, type and build epoch. Custom lookup callbacks
record `null`, because they do not use a database file. Preparation logs also
include the configured source, version and digest.

This preserves the existing optional behavior when no database is configured:
the continent plot is skipped, while the other mandatory analyses still run.
An explicitly configured but missing, invalid, oversized or checksum-mismatched
S3 database raises an error during preparation instead of silently substituting
another database version.
