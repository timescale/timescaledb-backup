package test

import (
	"context"
	"flag"
	"fmt"
	"os"
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

var (
	database           = flag.String("database", "tmp_db_timescale_migrate_test", "database to run integration tests on")
	useDocker          = flag.Bool("use-docker", true, "start database using a docker container")
	pgHost             = "localhost"
	pgPort    nat.Port = "5432/tcp"
)

const (
	expectedVersion = 1
	defaultDB       = "postgres"
	defaultUser     = "postgres"
	defaultPass     = "password"
	defaultDumpDir	= "foo"
)

func TestMain(m *testing.M) {
	flag.Parse()
	ctx := context.Background()
	if !testing.Short() && *useDocker {
		container, err := startContainer(ctx)
		if err != nil {
			fmt.Println("Error setting up container", err)
			os.Exit(1)
		}
		defer container.Terminate(ctx)
	}
	code := m.Run()
	os.Exit(code)
}

func TestBackupRestore(t *testing.T) {
	origDBName := "backup_restore_orig"
	restoredDBName := "backup_restore_restored"
	setupOrigDB(t, origDBName, "public", "1.6.1")
	createTestDB(t, restoredDBName)
	config := util.Config{
		DbURI=PGConnectURI(t, origDBName)
		DumpDir=defaultDumpDir
	}
	util.CleanConfig(config)
	dump.DoDump(config)
	}
	return
}

func setupOrigDB(t *testing.T, dbName string, tsSchema, tsVersion string) {

	createTestDB(t, dbName)
	dbURI := PGConnectURI(t, dbName)
	util.CreateTimescaleAtVer(context.Background(), dbURI, tsSchema, tsVersion)
	conn := util.GetDBConn(context.Background(), dbURI)
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
		(1257894002000000000, 'dev1', 2.5, 3);`)
	mustExec(t, conn, `INSERT INTO "two_Partitions"("timeCustom", device_id, series_0, series_1) VALUES
		(1257894000000000000, 'dev2', 1.5, 2)`)

}

func PGConnectURI(t *testing.T, dbName string) string {
	template := "postgres://%s:%s@%s:%d/%s"
	return fmt.Sprintf(template, defaultUser, defaultPass, pgHost, pgPort.Int(), dbName)
}

func startContainer(ctx context.Context) (testcontainers.Container, error) {
	containerPort := nat.Port("5432/tcp")
	req := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg11",
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
		return nil, err
	}

	pgHost, err = container.Host(ctx)
	if err != nil {
		return nil, err
	}

	pgPort, err = container.MappedPort(ctx, containerPort)
	if err != nil {
		return nil, err
	}

	return container, nil
}

func createTestDB(t *testing.T, DBName string) {
	if len(*database) == 0 {
		t.Skip()
	}
	conn := util.GetDBConn(context.Background(), PGConnectURI(t, defaultDB))
	defer conn.Close(context.Background())

	mustExec(t, conn, fmt.Sprintf("DROP DATABASE IF EXISTS %s", DBName))

	mustExec(t, conn, fmt.Sprintf("CREATE DATABASE %s", DBName))

	return
}

func mustExec(t testing.TB, conn *pgx.Conn, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag) {
	var err error
	if commandTag, err = conn.Exec(context.Background(), sql, arguments...); err != nil {
		t.Fatalf("Exec unexpectedly failed with %v: %v", sql, err)
	}
	return
}
