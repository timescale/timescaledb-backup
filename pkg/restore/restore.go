package restore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/ts-dump-restore/pkg/util"
)

// DoRestore takes a config and performs a pg_restore with the proper wrappings for Timescale
func DoRestore(cf *util.Config) error {
	restorePath, err := exec.LookPath("pg_restore")
	if err != nil {
		return errors.New("could not find pg_restore")
	}
	getver := exec.Command(restorePath, "--version")
	out, err := getver.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to get version of pg_restore: %w", err)
	}
	fmt.Printf("pg_restore version: %s\n", string(out))

	tsInfo, err := parseInfoFile(cf)
	if err != nil {
		return err
	}
	err = preRestoreTimescale(cf.DbURI, tsInfo)
	if err != nil {
		return err
	}
	//How does error handling in deferred things work?
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
		return fmt.Errorf("pg_restore run failed with: %w", err)
	}
	return err
}

func parseInfoFile(cf *util.Config) (util.TsInfo, error) {
	var tsInfo util.TsInfo
	file, err := os.Open(cf.TsInfoFileName)
	if err != nil {
		return tsInfo, fmt.Errorf("failed to open version file: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)

	err = decoder.Decode(&tsInfo)
	if err != nil {
		return tsInfo, fmt.Errorf("failed to decode tsInfo JSON: %w", err)
	}
	return tsInfo, err
}

func preRestoreTimescale(dbURI string, tsInfo util.TsInfo) error {

	// First create the extension at the correct version in the correct schema
	err := util.CreateTimescaleAtVer(context.Background(), dbURI, tsInfo.TsSchema, tsInfo.TsVersion)
	if err != nil {
		return err
	}
	conn, err := util.GetDBConn(context.Background(), dbURI)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	// Now run our pre-restoring function
	var pr bool
	err = conn.QueryRow(context.Background(), fmt.Sprintf("SELECT %s.timescaledb_pre_restore() ", tsInfo.TsSchema)).Scan(&pr)
	if err != nil {
		return err
	}
	if !pr {
		return errors.New("TimescaleDB pre restore function failed to run")
	}

	return err
}

func postRestoreTimescale(dbURI string, tsInfo util.TsInfo) error {

	conn, err := util.GetDBConn(context.Background(), dbURI)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	// Now run our post-restoring function
	var pr bool
	err = conn.QueryRow(context.Background(), fmt.Sprintf("SELECT %s.timescaledb_post_restore() ", tsInfo.TsSchema)).Scan(&pr)
	if err != nil {
		return err
	}
	if !pr {
		return errors.New("post restore function failed")
	}

	// Confirm that no one pulled a fast one on us, and that we're at the right extension version
	info := util.TsInfo{}
	err = conn.QueryRow(context.Background(), "SELECT e.extversion, n.nspname FROM pg_extension e INNER JOIN pg_namespace n ON e.extnamespace = n.oid WHERE e.extname='timescaledb'").Scan(&info.TsVersion, &info.TsSchema)
	if err != nil {
		if err == pgx.ErrNoRows {
			return errors.New("could not confirm creation of TimescaleDB extension")
		}
		return err
	}
	if info.TsSchema != tsInfo.TsSchema || info.TsVersion != tsInfo.TsVersion {
		return errors.New("TimescaleDB extension created in incorrect schema or at incorrect version, please drop the extension and restart the restore")
	}
	return err
}
