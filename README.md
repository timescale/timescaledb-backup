#timescaledb-backup 

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
   - You will need binaries for `pg_dump` and `pg_restore` installed where you are running 
   `timescaledb-backup`
   - The target database needs the `.so` file of the dumped version so that we can restore to the correct version. It will also need the `.so` of your target version.

### Using `ts-dump`
First create a dump using the `ts-dump` command, for those used to using `pg_dump`, the
options are pared down significantly you will need to provide the following parameters:

   - `--db-URI` the database connection string in [Postgres URI](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) format to connect to. The Postgres format is: `postgresql://[user[:password]@][host][:port][,...][/dbname][?param1=value1&...]` many of these parameters can be specified in enviornmental variables in the normal Postgres convention and passwords will be looked up in the usual ways as allowed by the `pgx` go library.
   - `--dump-dir` the dump directory where you would like the dump to be stored. The dump directory should not exist, it will be created as part of creating the dump, however the path to the directory should exist.

Optional parameters:
   - `--jobs` Sets the number of jobs to run for the dump, by default it is set to 4 and will run  in parallel mode, set to 0 to disable parallelism
   - `--verbose` Determines whether verbose output will be provided from `pg_dump`. Defaults to false. 


When a dump is created, the dump directory will be created, and a json file containing
TimescaleDB version information as well as a subdirectory containing the dump will be
created. The dump is always created in directory format inside the subdirectory. 

### Using `ts-restore`
Once you have a backup you can run a `ts-restore` by specifying the same dump directory
and a new database uri. Currently, the database you are restoring to must already exist.
If TimescaleDB is installed it will be dropped and re-created at the proper
version, we recommend restoring only to an empty database.  You will need to provide the following parameters: 

  - `--db-URI` the database connection string in [Postgres URI](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) format to connect to. The Postgres format is: `postgresql://[user[:password]@][host][:port][,...][/dbname][?param1=value1&...]` many of these parameters can be specified in enviornmental variables in the normal Postgres convention and passwords will be looked up in the usual ways as allowed by the `pgx` go library.
   - `--dump-dir` the dump directory where the output from `ts-dump` was stored.

Optional parameters:
   - `--jobs` Sets the number of jobs to run for the restore, by default it is set to 4 and will run in parallel mode during the sections[^1] that are able to be parallelized. Set to 0 to disable parallelism.
   - `--verbose` Determines whether verbose output will be provided from `pg_restore`. Defaults to true.

As an example, let's suppose I have two `postgres` clusters running on my machine, perhaps on versions 11 on port 5432 and 12 on 5433 and I wish to dump and restore in order to upgrade between versions: 
I would run `ts-dump --db-URI=postgresql://postgres:pwd1@localhost:5432/tsdb --dump-dir=~/dumps/dump1 --verbose --jobs=2`
which will run the dump in verbose mode with 2 workers
Then : `ts-restore --db-URI=postgresql://postgres:pwd1@localhost:5433/tsdb --dump-dir=~/dumps/dump1 --verbose --jobs=2`
which will run in the same mode, and restore the dump I just created to the same database in the cluster running on port 5433. 

## Limitations of TimescaleDB Backup
We currently do not support many of the options that `pg_dump` and `pg_restore` do, please
submit issues or contributions for flags or options that you believe are important to
include.  



[^1]: In order to support parallel restores given the way the TimescaleDB catalog works,
   we first perform a pre-data restore, then restore the data for the catalog, then in
   parallel perform the data section for everything else and the post-data section for
   everything. 