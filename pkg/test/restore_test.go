// This file and its contents are licensed under the Timescale License
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/timescale/ts-dump-restore/pkg/dump"
	"github.com/timescale/ts-dump-restore/pkg/restore"
	"github.com/timescale/ts-dump-restore/pkg/util"
)

type dbInfo struct {
	image       string
	port        nat.Port
	host        string
	dbName      string
	dbUser      string
	dbPass      string
	defaultDB   string
	defaultUser string
	defaultPass string
}

var ()

const (
	defaultDB   = "postgres"
	defaultUser = "postgres"
	defaultPass = "password"
)

func newDbInfo() dbInfo {
	db := dbInfo{}
	db.dbName = defaultDB
	db.dbUser = defaultUser
	db.dbPass = defaultPass
	db.defaultDB = defaultDB
	db.defaultUser = defaultUser
	db.defaultPass = defaultPass
	return db
}

func TestMain(m *testing.M) {

	code := m.Run()
	os.Exit(code)
}

func TestBackupRestore(t *testing.T) {
	cases := []struct {
		desc         string
		dumpImage    string
		restoreImage string
		tsVersion    string
		numJobs      int
		doUpdate     bool
	}{
		{
			desc:         "pg-11-parallel",
			dumpImage:    "timescale/timescaledb:1.7.2-pg11",
			restoreImage: "timescale/timescaledb:1.7.2-pg11",
			tsVersion:    "1.7.1",
			numJobs:      4,
			doUpdate:     false,
		},
		{
			desc:         "pg-11-non-parallel",
			dumpImage:    "timescale/timescaledb:1.7.2-pg11",
			restoreImage: "timescale/timescaledb:1.7.2-pg11",
			tsVersion:    "1.7.1",
			numJobs:      0,
			doUpdate:     false,
		},
		{
			desc:         "pg-12-parallel",
			dumpImage:    "timescale/timescaledb:1.7.2-pg12",
			restoreImage: "timescale/timescaledb:1.7.2-pg12",
			tsVersion:    "1.7.1",
			numJobs:      4,
			doUpdate:     false,
		},
		{
			desc:         "pg-11-to-12",
			dumpImage:    "timescale/timescaledb:1.7.2-pg11",
			restoreImage: "timescale/timescaledb:1.7.2-pg12",
			tsVersion:    "1.7.1",
			numJobs:      4,
			doUpdate:     false,
		},
		{
			desc:         "pg-11-older-ts-1.6.1",
			dumpImage:    "timescale/timescaledb:1.7.2-pg11",
			restoreImage: "timescale/timescaledb:1.7.2-pg11",
			tsVersion:    "1.6.1",
			numJobs:      4,
			doUpdate:     false,
		},
		{
			desc:         "pg-10-parallel",
			dumpImage:    "timescale/timescaledb:1.7.2-pg10",
			restoreImage: "timescale/timescaledb:1.7.2-pg10",
			tsVersion:    "1.7.2",
			numJobs:      4,
			doUpdate:     false,
		},
		{
			desc:         "pg-10-11-upgrade-ts-1.6.1",
			dumpImage:    "timescale/timescaledb:1.7.2-pg10",
			restoreImage: "timescale/timescaledb:1.7.2-pg11",
			tsVersion:    "1.6.1",
			numJobs:      4,
			doUpdate:     false,
		},
		{
			desc:         "pg-12-update",
			dumpImage:    "timescale/timescaledb:1.7.4-pg12",
			restoreImage: "timescale/timescaledb:1.7.4-pg12",
			tsVersion:    "1.7.1",
			numJobs:      4,
			doUpdate:     true,
		},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			dumpContainer, dumpDb, err := startContainer(ctx, c.dumpImage)
			if err != nil {
				t.Fatal("Failed to create dump container ", err)
			}
			defer dumpContainer.Terminate(ctx)
			dumpDb.dbName = "dump_test"
			restoreContainer, restoreDb, err := startContainer(ctx, c.restoreImage)
			if err != nil {
				t.Fatal("Failed to create restore container ", err)
			}
			defer restoreContainer.Terminate(ctx)
			restoreDb.dbName = "restore_test"

			setupOrigDB(t, dumpDb, "public", c.tsVersion)
			// setup dump config
			dumpConfig := &util.Config{}
			dumpConfig.DbURI = PGConnectURI(dumpDb, false)
			dumpConfig.DumpDir = fmt.Sprintf("%s.%d", dumpDb.dbName, dumpDb.port.Int())
			dumpConfig.Jobs = c.numJobs
			dumpConfig.Verbose = false //default settings
			dumpConfig.DumpRoles = true
			dumpConfig.DumpTablespaces = true
			util.CleanConfig(dumpConfig)
			// corresponding restore config

			createTestDB(t, restoreDb)
			restoreConfig := &util.Config{}
			restoreConfig.DbURI = PGConnectURI(restoreDb, false)
			restoreConfig.DumpDir = fmt.Sprintf("%s.%d", dumpDb.dbName, dumpDb.port.Int()) //dump dir is from dump not from restore
			restoreConfig.Verbose = true                                                   //default settings
			restoreConfig.Jobs = c.numJobs
			restoreConfig.DoUpdate = c.doUpdate
			util.CleanConfig(restoreConfig)

			//make sure we remove the dumpDir at the end no matter what
			defer os.RemoveAll(dumpConfig.DumpDir)
			err = dump.DoDump(dumpConfig)
			if err != nil {
				t.Fatal("Failed on restore: ", err)
			}
			err = restore.DoRestore(restoreConfig)
			if err != nil {
				t.Fatal("Failed on restore: ", err)
			}
			confirmTablesCongruent(t, pgx.Identifier{"public"}, pgx.Identifier{"two_Partitions"}, dumpConfig.DbURI, restoreConfig.DbURI)
			confirmTablesCongruent(t, pgx.Identifier{"public"}, pgx.Identifier{"insert_test"}, dumpConfig.DbURI, restoreConfig.DbURI)
			confirmCanStillInsert(t, restoreConfig.DbURI)
		})
	}
}

func confirmTablesCongruent(t *testing.T, tableSchema pgx.Identifier, tableName pgx.Identifier, origURI string, restoredURI string) {

	quotedTableSchema := tableSchema.Sanitize()
	quotedTableName := tableName.Sanitize()

	sql := fmt.Sprintf(`SELECT * FROM %s.%s AS t ORDER BY t`, quotedTableSchema, quotedTableName)
	origConn, err := util.GetDBConn(context.Background(), origURI)
	if err != nil {
		t.Fatal("Unable to connect to dump db: ", err)
	}
	defer origConn.Close(context.Background())
	restoredConn, err := util.GetDBConn(context.Background(), restoredURI)
	if err != nil {
		t.Fatal("Unable to connect to restore db: ", err)
	}
	defer restoredConn.Close(context.Background())

	origRows, err := origConn.Query(context.Background(), sql)
	if err != nil {
		t.Fatal("Query failed on origDB: ", err)
	}
	defer origRows.Close()
	restoredRows, err := restoredConn.Query(context.Background(), sql)
	if err != nil {
		t.Fatal("Query failed on restoredDB: ", err)
	}
	defer restoredRows.Close()
	var rowCount int
	for origRows.Next() {
		rowCount++
		if !restoredRows.Next() {
			t.Fatalf("Restored table %s.%s has too few rows", quotedTableSchema, quotedTableName)
		}
		if !reflect.DeepEqual(origRows.RawValues(), restoredRows.RawValues()) {
			t.Fatalf("Restored table %s.%s has element unequal to original row: %d", quotedTableSchema, quotedTableName, rowCount)
		}
	}
	if restoredRows.Next() {
		t.Fatalf("Restored table %s.%s has too many rows", quotedTableSchema, quotedTableName)
	}
}

func confirmCanStillInsert(t *testing.T, restoredURI string) {
	restoredConn, err := util.GetDBConn(context.Background(), restoredURI)
	if err != nil {
		t.Fatal("Unable to connect to restore db: ", err)
	}
	defer restoredConn.Close(context.Background())
	_, err = restoredConn.Exec(context.Background(), `INSERT INTO public."insert_test"(tstamp, device_id, series_0, series_1) VALUES ('2020-10-04 14:21:08+00', 'dev2', 1.5, 1)`)
	if err != nil {
		t.Fatalf("Post-restore insert failure")
	}
}

func setupOrigDB(t *testing.T, db dbInfo, tsSchema string, tsVersion string) {

	createTestDB(t, db)
	dbURI := PGConnectURI(db, false)
	err := util.CreateTimescaleAtVer(context.Background(), dbURI, tsSchema, tsVersion)
	if err != nil {
		//in tests errors still are fatal (if unexpected) or is there a better pattern?
		t.Fatalf("Error setting up original database: %v", err)
	}
	conn, err := util.GetDBConn(context.Background(), dbURI)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	// basically re-creating the pg_dump test from timescaledb repo here.
	mustExec(t, conn, `CREATE TABLE PUBLIC."two_Partitions" (
		"timeCustom" BIGINT NOT NULL,
		device_id TEXT NOT NULL,
		series_0 DOUBLE PRECISION NULL,
		series_1 DOUBLE PRECISION NULL,
		series_2 DOUBLE PRECISION NULL,
		series_bool BOOLEAN NULL) ;`)
	mustExec(t, conn, `CREATE INDEX ON PUBLIC."two_Partitions" (device_id, "timeCustom" DESC NULLS LAST) WHERE device_id IS NOT NULL`)
	mustExec(t, conn, `CREATE INDEX ON PUBLIC."two_Partitions" ("timeCustom" DESC NULLS LAST, series_0) WHERE series_0 IS NOT NULL`)
	mustExec(t, conn, `CREATE INDEX ON PUBLIC."two_Partitions" ("timeCustom" DESC NULLS LAST, series_1)  WHERE series_1 IS NOT NULL`)
	mustExec(t, conn, `CREATE INDEX ON PUBLIC."two_Partitions" ("timeCustom" DESC NULLS LAST, series_2) WHERE series_2 IS NOT NULL`)
	mustExec(t, conn, `CREATE INDEX ON PUBLIC."two_Partitions" ("timeCustom" DESC NULLS LAST, series_bool) WHERE series_bool IS NOT NULL`)
	mustExec(t, conn, `CREATE INDEX ON PUBLIC."two_Partitions" ("timeCustom" DESC NULLS LAST, device_id)`)
	mustExec(t, conn, `SELECT * FROM create_hypertable('"public"."two_Partitions"'::regclass, 'timeCustom'::name, 'device_id'::name, associated_schema_name=>'_timescaledb_internal'::text, number_partitions => 2, chunk_time_interval=>_timescaledb_internal.interval_to_usec('1 month'));`)
	mustExec(t, conn, `INSERT INTO public."two_Partitions" VALUES
		(1257894000000000000, 'dev1',1.5, 1,2, true), 
		(1257894000000000000, 'dev1',1.5, 2,NULL, NULL),
		(1257894000000001000, 'dev1',2.5, 3,NULL, NULL),
		(1257894001000000000, 'dev1',3.5, 4,NULL, NULL),
		(1257897600000000000, 'dev1',4.5, 5,NULL, false),
		(1257894002000000000, 'dev1',5.5, 6,NULL, true),
		(1257894002000000000, 'dev1',5.5, 7,NULL, false)`)
	mustExec(t, conn, `INSERT INTO public."two_Partitions"("timeCustom", device_id, series_0, series_1) VALUES
		(1257987600000000000, 'dev1', 1.5, 1),
		(1257987600000000000, 'dev1', 1.5, 2),
		(1257894000000000000, 'dev2', 1.5, 1),
		(1257894002000000000, 'dev1', 2.5, 3)`)
	mustExec(t, conn, `INSERT INTO public."two_Partitions"("timeCustom", device_id, series_0, series_1) VALUES
		(1257894000000000000, 'dev2', 1.5, 2)`)
	mustExec(t, conn, `ALTER TABLE public."two_Partitions" SET (timescaledb.compress=true)`)
	mustExec(t, conn, `SELECT compress_chunk((SELECT chunk FROM show_chunks('public."two_Partitions"'::regclass) c (chunk) LIMIT 1))`)

	mustExec(t, conn, `CREATE TABLE PUBLIC."insert_test" (
		tstamp timestamptz NOT NULL,
		device_id TEXT NOT NULL,
		series_0 DOUBLE PRECISION NULL, 
		series_1 DOUBLE PRECISION NULL) ;`)

	mustExec(t, conn, `SELECT * FROM create_hypertable('"public"."insert_test"'::regclass, 'tstamp'::name, chunk_time_interval=>'1 day'::interval);`)
	mustExec(t, conn, `INSERT INTO public."insert_test"(tstamp, device_id, series_0, series_1) VALUES
	('2020-10-04 14:21:08+00', 'dev1', 1.5, 1),
	('2020-10-04 14:21:30+00', 'dev1', 1.5, 2),
	('2020-10-10 14:21:30+00', 'dev2', 1.5, 1),
	('2020-10-14 14:21:30+00', 'dev1', 2.5, 3)`)
}

func PGConnectURI(db dbInfo, useDefault bool) string {
	template := "postgres://%s:%s@%s:%d/%s"
	if useDefault {
		return fmt.Sprintf(template, db.defaultUser, db.defaultPass, db.host, db.port.Int(), db.defaultDB)
	}
	return fmt.Sprintf(template, db.dbUser, db.dbPass, db.host, db.port.Int(), db.dbName)
}

func startContainer(ctx context.Context, image string) (testcontainers.Container, dbInfo, error) {
	containerPort := nat.Port("5432/tcp")
	db := newDbInfo()
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{string(containerPort)},
		WaitingFor:   wait.NewHostPortStrategy(containerPort),
		Env: map[string]string{
			"POSTGRES_PASSWORD": defaultPass,
		},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, db, err
	}

	db.host, err = container.Host(ctx)
	if err != nil {
		return nil, db, err
	}
	db.port, err = container.MappedPort(ctx, containerPort)
	if err != nil {
		return nil, db, err
	}

	return container, db, nil
}

func createTestDB(t *testing.T, db dbInfo) {

	conn, err := util.GetDBConn(context.Background(), PGConnectURI(db, true))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	mustExec(t, conn, fmt.Sprintf("DROP DATABASE IF EXISTS %s", db.dbName))

	mustExec(t, conn, fmt.Sprintf("CREATE DATABASE %s", db.dbName))

	return
}

func mustExec(t testing.TB, conn *pgx.Conn, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag) {
	var err error
	if commandTag, err = conn.Exec(context.Background(), sql, arguments...); err != nil {
		t.Fatalf("Exec unexpectedly failed with %v: %v", sql, err)
	}
	return
}
