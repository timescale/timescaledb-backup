package test

import (
	"context"
	"flag"
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

var (
	database              = flag.String("database", "tmp_db_timescale_migrate_test", "database to run integration tests on")
	useDocker             = flag.Bool("use-docker", true, "start database using a docker container")
	dumpImage             = flag.String("dump-image", "timescale/timescaledb:latest-pg11", "specifies the docker image for the db to dump from")
	restoreImage          = flag.String("restore-image", "timescale/timescaledb:latest-pg12", "specifies the image for the db to restore to")
	pgPort       nat.Port = "5432/tcp"
	dumpHost              = "localhost"
	restoreHost           = "localhost"
	tsVersion             = flag.String("timescale-version", "1.7.1", "the version of Timescale to test when dumping/restoring")
)

const (
	expectedVersion = 1
	defaultDB       = "postgres"
	defaultUser     = "postgres"
	defaultPass     = "password"
)

func TestMain(m *testing.M) {
	flag.Parse()
	ctx := context.Background()
	dumpContainer, err := startContainer(ctx, *dumpImage)
	if err != nil {
		fmt.Println("Error setting up container", err)
		os.Exit(1)
	}
	defer dumpContainer.Terminate(ctx)
	dumpHost, err = dumpContainer.Host(ctx)
	if err != nil {
		fmt.Println("error getting db host from container")
		os.Exit(1)
	}
	restoreContainer, err := startContainer(ctx, *restoreImage)
	if err != nil {
		fmt.Println("Error setting up container", err)
		os.Exit(1)
	}
	defer restoreContainer.Terminate(ctx)
	restoreHost, err = restoreContainer.Host(ctx)
	if err != nil {
		fmt.Println("error getting db host from container")
		os.Exit(1)
	}
	code := m.Run()
	os.Exit(code)
}

func TestParallelBackupRestore(t *testing.T) {
	t.Parallel()
	BackupRestoreTest(t, "parallel_orig", "parallel_restored", 4)
}

func TestNonParallelBackupRestore(t *testing.T) {
	t.Parallel()
	BackupRestoreTest(t, "non_parallel_orig", "non_parallel_restored", 0)
}

func BackupRestoreTest(t *testing.T, origDBName string, restoredDBName string, numJobs int) {

	setupOrigDB(t, origDBName, dumpHost, "public", *tsVersion)
	// setup dump config
	dumpConfig := &util.Config{}
	dumpConfig.DbURI = PGConnectURI(origDBName, dumpHost)
	dumpConfig.DumpDir = origDBName
	dumpConfig.Verbose = false //default settings
	dumpConfig.Jobs = numJobs
	util.CleanConfig(dumpConfig)
	// corresponding restore config

	createTestDB(t, restoredDBName, restoreHost)
	restoreConfig := &util.Config{}
	restoreConfig.DbURI = PGConnectURI(restoredDBName, restoreHost)
	restoreConfig.DumpDir = origDBName
	restoreConfig.Verbose = true //default settings
	restoreConfig.Jobs = numJobs
	util.CleanConfig(restoreConfig)

	//make sure we remove the dumpDir at the end no matter what
	defer os.RemoveAll(dumpConfig.DumpDir)
	err := dump.DoDump(dumpConfig)
	if err != nil {
		t.Fatal("Failed on restore: ", err)
	}
	err = restore.DoRestore(restoreConfig)
	if err != nil {
		t.Fatal("Failed on restore: ", err)
	}
	confirmTablesCongruent(t, pgx.Identifier{"public"}, pgx.Identifier{"two_Partitions"}, dumpConfig.DbURI, restoreConfig.DbURI)
	return
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

func setupOrigDB(t *testing.T, dbName string, dbHost string, tsSchema string, tsVersion string) {

	createTestDB(t, dbName, dbHost)
	dbURI := PGConnectURI(dbName, dbHost)
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

}

func PGConnectURI(dbName string, pgHost string) string {
	template := "postgres://%s:%s@%s:%d/%s"
	return fmt.Sprintf(template, defaultUser, defaultPass, pgHost, pgPort.Int(), dbName)
}

func startContainer(ctx context.Context, image string) (testcontainers.Container, error) {
	containerPort := nat.Port("5432/tcp")
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
		return nil, err
	}

	pgPort, err = container.MappedPort(ctx, containerPort)
	if err != nil {
		return nil, err
	}

	return container, nil
}

func createTestDB(t *testing.T, DBName string, dbHost string) {
	if len(*database) == 0 {
		t.Skip()
	}
	conn, err := util.GetDBConn(context.Background(), PGConnectURI(defaultDB, dbHost))
	if err != nil {
		t.Fatal(err)
	}
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
