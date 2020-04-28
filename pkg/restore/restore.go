package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/ts-dump-restore/pkg/util"
)

// DoRestore takes a config and performs a pg_restore with the proper wrappings for Timescale
func DoRestore(cf *util.Config) int {
	restorePath, err := exec.LookPath("pg_restore")
	if err != nil {
		log.Fatalf("could not find pg_restore")
	}
	getver := exec.Command(restorePath, "--version")
	out, err := getver.CombinedOutput()
	if err != nil {
		log.Fatalf("Failed to get version of pg_restore: %s\n", err)
	}
	fmt.Printf("pg_restore version: %s\n", string(out))

	tsInfo := parseInfoFile(cf)
	preRestoreTimescale(cf.DbURI, tsInfo)
	defer postRestoreTimescale(cf.DbURI, tsInfo)

	dump := exec.Command(restorePath)
	dump.Env = append(os.Environ()) //may use this to set other environmental vars
	dump.Args = append(dump.Args,
		fmt.Sprintf("--dbname=%s", cf.DbURI),
		"--format=directory",
		"--verbose",
		cf.PgDumpDir) //final argument to pg_restore should be the filename to restore from. Bad UI...
	dump.Stdout = os.Stdout
	dump.Stderr = os.Stderr
	err = dump.Run()
	if err != nil {
		log.Printf("pg_restore run failed with: %s\n", err)
		return 1
	}
	return 0
}

func parseInfoFile(cf *util.Config) util.TsInfo {
	file, err := os.Open(cf.TsInfoFileName)
	if err != nil {
		log.Fatalf("Failed to open version file: %s,\n", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var tsInfo util.TsInfo
	err = decoder.Decode(&tsInfo)
	if err != nil {
		log.Fatalf("Failed to decode tsInfo JSON: %s\n", err)
	}
	return tsInfo
}

func preRestoreTimescale(dbURI string, tsInfo util.TsInfo) {

	// First create the extension at the correct version in the correct schema
	util.CreateTimescaleAtVer(context.Background(), dbURI, tsInfo.TsSchema, tsInfo.TsVersion)
	conn := util.GetDBConn(context.Background(), dbURI)
	defer conn.Close(context.Background())
	// Now run our pre-restoring function
	var pr bool
	err := conn.QueryRow(context.Background(), fmt.Sprintf("SELECT %s.timescaledb_pre_restore() ", tsInfo.TsSchema)).Scan(&pr)
	if err != nil {
		log.Fatal(err)
	}
	if !pr {
		log.Fatal("Pre restore function failed to run")
	}

	return
}

func postRestoreTimescale(dbURI string, tsInfo util.TsInfo) {

	conn := util.GetDBConn(context.Background(), dbURI)
	defer conn.Close(context.Background())

	// Now run our post-restoring function
	var pr bool
	err := conn.QueryRow(context.Background(), fmt.Sprintf("SELECT %s.timescaledb_post_restore() ", tsInfo.TsSchema)).Scan(&pr)
	if err != nil {
		log.Fatal(err)
	}
	if !pr {
		log.Fatal("Post restore function failed")
	}

	// Confirm that no one pulled a fast one on us, and that we're at the right extension version
	info := util.TsInfo{}
	err = conn.QueryRow(context.Background(), "SELECT e.extversion, n.nspname FROM pg_extension e INNER JOIN pg_namespace n ON e.extnamespace = n.oid WHERE e.extname='timescaledb'").Scan(&info.TsVersion, &info.TsSchema)
	if err != nil {
		if err == pgx.ErrNoRows {
			log.Fatal("Could not confirm creation of TimescaleDB extension")
		}
		log.Fatal(err)
	}
	if info.TsSchema != tsInfo.TsSchema || info.TsVersion != tsInfo.TsVersion {
		log.Fatal("TimescaleDB extension created in incorrect schema or at incorrect version, please drop the extension and restart the restore.")
	}
	return
}
