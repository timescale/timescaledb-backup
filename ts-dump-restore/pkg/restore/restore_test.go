package restore

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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
	setupOrigDB(t, origDBName)
	createTestDB(t, restoredDBName)

	return
}

func setupOrigDB(t *testing.T, dbName string, tsVersion string) {

	createTestDB(t, dbName)
	conn := util.GetDBConn(context.Background(), PGConnectURL(dbName))
	defer conn.Close(context.Background())
	batch := conn.NewBatch()
	batch.Queue("BEGIN`")
	batch.Queue(fmt.Sprintf("CREATE EXTENSION timescaledb WITH VERSION '%s' ", tsVersion))
	// basically re-creating the pg_dump test from timescaledb repo here.
	batch.Queue(`CREATE TABLE PUBLIC."two_Partitions" (
		"timeCustom" BIGINT NOT NULL,
		device_id TEXT NOT NULL,
		series_0 DOUBLE PRECISION NULL,
		series_1 DOUBLE PRECISION NULL,
		series_2 DOUBLE PRECISION NULL,
		series_bool BOOLEAN NULL) `)
	batch.Queue(`CREATE INDEX ON PUBLIC."two_Partitions" (device_id, "timeCustom" DESC NULLS LAST) WHERE device_id IS NOT NULL`)
	batch.Queue(`CREATE INDEX ON PUBLIC."two_Partitions" ("timeCustom" DESC NULLS LAST, series_0) WHERE series_0 IS NOT NULL`)
	batch.Queue(`CREATE INDEX ON PUBLIC."two_Partitions" ("timeCustom" DESC NULLS LAST, series_1)  WHERE series_1 IS NOT NULL`)
	batch.Queue(`CREATE INDEX ON PUBLIC."two_Partitions" ("timeCustom" DESC NULLS LAST, series_2) WHERE series_2 IS NOT NULL`)
	batch.Queue(`CREATE INDEX ON PUBLIC."two_Partitions" ("timeCustom" DESC NULLS LAST, series_bool) WHERE series_bool IS NOT NULL`)
	batch.Queue(`CREATE INDEX ON PUBLIC."two_Partitions" ("timeCustom" DESC NULLS LAST, device_id)`)

	batch.Queue("COMMIT`")

}

func PGConnectURL(t *testing.T, dbName string) string {
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
	conn := util.GetDBConn(context.Background(), PGConnectURL(t, DBName))
	defer conn.Close(context.Background())

	_, err := conn.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", DBName))
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s", DBName))
	if err != nil {
		t.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return
}
