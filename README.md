**Effective February 18, 2022. This project is no longer maintained. We recommend using the PostgreSQL tools `pg_dump` and `pg_restore` instead. For more information on using these tools with TimescaleDB, please refer to the TimescaleDB 
[backup and restore documentation](https://docs.timescale.com/timescaledb/latest/how-to-guides/backup-and-restore/).**

---

# timescaledb-backup 

`timescaledb-backup` is a program for making dumping and restoring a
[TimescaleDB](//github.com/timescale/timescaledb) database simpler, less error-prone,
and more performant.  In particular, the current use of vanilla PostgreSQL tools
[`pg_dump`](//www.postgresql.org/docs/current/app-pgdump.html) and
[`pg_restore`](//www.postgresql.org/docs/current/app-pgrestore.html) has several 
limitations when applied to TimescaleDB:
1. The PostgreSQL backup/restore tools do not support backup/restore _across_
   versions of extensions.  So that if you take a backup from (say) TimescaleDB
   v1.7.1, you need to restore to a database version that is _also_ running
   TimescaleDB v1.7.1, and then manually upgrade TimescaleDB to a later version.
1. The backup/restore tools do not _track_ which version of TimescaleDB is in the
   backup, so a developer needs to maintain additional external information to
   ensure the proper restore process.
1. Users need to take manual steps to run pre- and post-restore hooks (database
   functions) in TimescaleDB to ensure correct behavior.  Failure to execute these
   hooks can prevent restores from functioning correctly.
1. The restore process cannot easily perform parallel restoration for greater
   speed/efficiency.

Towards this end, `timescaledb-backup` overcomes many of these current
limitations.  It continues to use `pg_dump` and `pg_restore`, but properly wraps
them to:
1. Track automatically the version of TimescaleDB internally in the information
   dumped, to ensure that the proper version is always restored.
1. Run all pre and post restore hooks at their proper times during a restore; and
1. Enable parallel restore by properly sequencing catalog and data restores during
   the restore process.

## Installing `timescaledb-backup`

You can install by running `go get`

```bash
$ go get github.com/timescale/timescaledb-backup/
```

Or by downloading a binary at [our release page](//github.com/timescale/timescaledb-backup/releases)
It will also be distributed with a number of our tools in `timescaledb-tools`; yum, apt, Homebrew,etc.

## Using `timescaledb-backup`

### Requirements
   - You will need binaries for `pg_dump`, `pg_dumpall`, and `pg_restore` installed where you are running 
   `timescaledb-backup`
   - The target database needs the `.so` file of the dumped version so that we can restore to the correct version. It will also need the `.so` of your target version.

### Using `ts-dump`
First create a dump using the `ts-dump` command, for those used to using `pg_dump`, the
options are pared down significantly you will need to provide the following parameters:

   - `--db-URI` the database connection string in [Postgres URI](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) format to connect to. The Postgres format is: `postgresql://[user[:password]@][host][:port][,...][/dbname][?param1=value1&...]` many of these parameters can be specified in enviornmental variables in the normal Postgres convention and passwords will be looked up in the usual ways as allowed by the `pgx` go library.
   - `--dump-dir` the dump directory where you would like the dump to be stored. The dump directory should not exist, it will be created as part of creating the dump, however the path to the directory should exist.

Optional parameters:
   - `--jobs` Sets the number of jobs to run for the dump, by default it is set to 4 and will run in parallel mode, set to 0 to disable parallelism
   - `--verbose` Determines whether verbose output will be provided from `pg_dump`. Defaults to false. 
   - `--dump-roles` Determines whether to use `pg_dumpall` to dump roles (without password information) before running the dump. Can be useful in order to restore permissions on tables etc. Defaults to true.
   - `--dump-tablespaces` Determines whether to use `pg_dumpall` to dump tablespaces before running the dump. Can be useful if using multiple tablespaces and in restoring tables to the correct tablespaces. Defaults to true. 
   - `--dump-pause-jobs` Determines whether to pause background jobs that could disrupt a parallel dump process by performing DDL during the dump. Defaults to true, only affects parallel dumps. 
   - `--dump-pause-UDAs` Determines whether to pause user defined actions (available in Timescale 2.0+) when pausing jobs. Defaults to true, only affects parallel dumps where jobs are being paused.
	- `--dump-job-finish-timeout` The number of seconds to wait for jobs which may perform DDL to finish before timing out. Defaults to 600 (10 minutes), set to -1 to not wait on jobs to finish. This only affects parallel dumps where jobs are being paused. 
   - `-- <pg_dump options>` options to pass along to the `pg\_dump` binary
	

When a dump is created, the dump directory will be created, as well as a subdirectory for the dump. The main directory contains a JSON with TimescaleDB version information as well as any `sql` files generated by `pg_dumpall`. 

### Using `ts-restore`
Once you have a backup you can run a `ts-restore` by specifying the same dump directory
and a new database uri. The database you are restoring to must already exist, so be sure
to create it before running the restore. By default, `roles.sql` and `tablespaces.sql`
files are created in the dump directory. These may be run before the restore by running
`psql -d <db-URI> -f <dump-dir>/roles.sql` & `psql -d <db-URI> -f <dump-dir>/tablespaces.sql`. 
The `<db-URI>` parameter is specified in the same format as below. If you are restoring
multiple databases on the same `postgres` instance, they only need to be run once and will
error if run multiple times, however errors resulting from roles or tablespaces being
created when they already exist can be safely disregarded. 

If TimescaleDB is installed it will be dropped and re-created at the proper version, we
recommend restoring only to an empty database.  

You will need to provide the following parameters: 

  - `--db-URI` the database connection string in [Postgres URI](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) format to connect to. The Postgres format is: `postgresql://[user[:password]@][host][:port][,...][/dbname][?param1=value1&...]` many of these parameters can be specified in environment variables in the normal Postgres convention and passwords will be looked up in the usual ways as allowed by the `pgx` go library.
   - `--dump-dir` the dump directory where the output from `ts-dump` was stored.

Optional parameters:
   - `--jobs` Sets the number of jobs to run for the restore, by default it is set to 4 and will run in parallel mode during the sections[^1] that are able to be parallelized. Set to 0 to disable parallelism.
   - `--verbose` Provide verbose output from `pg_restore`. Defaults to true.
   - `--do-update` Update the TimescaleDB version to the latest default version immediately following the restore.[^2] Defaults to true.
   - `-- <pg_restore options>` options to pass along to the `pg\_restore` binary

As an example, let's suppose I have two `postgres` clusters running on my machine, perhaps on versions 11 on port 5432 and 12 on 5433 and I wish to dump and restore in order to upgrade between versions: 
I would run `ts-dump --db-URI=postgresql://postgres:pwd1@localhost:5432/tsdb --dump-dir=~/dumps/dump1 --verbose --jobs=2`
which will run the dump in verbose mode with 2 workers
Then : `ts-restore --db-URI=postgresql://postgres:pwd1@localhost:5433/tsdb --dump-dir=~/dumps/dump1 --verbose --jobs=2`
which will run in the same mode, and restore the dump I just created to the same database in the cluster running on port 5433. 

## Limitations of TimescaleDB Backup
We currently support passing along all the options to `pg_dump` and `pg_restore`,
however not all interactions of these flags together with `timescaledb-backup` flags have
been fully tested. Please submit issues or contributions if you run into such an issue.

[^1]: In order to support parallel restores given the way the TimescaleDB catalog works,
   we first perform a pre-data restore, then restore the data for the catalog, then in
   parallel perform the data section for everything else and the post-data section for
   everything. 

[^2]: This requires that you have the .so for the version you are restoring from and the
   version that you will update to. For instance, if your dump is from TimescaleDB 1.6.2
   and the latest version is TimescaleDB 1.7.4, you need the .so from 1.6.2 available to
   restore to, and then it will update to 1.7.4 following the restore. Our default
   packages include several older versions to enable updates. 
