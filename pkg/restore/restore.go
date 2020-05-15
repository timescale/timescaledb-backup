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
	tsInfo, err := parseInfoFile(cf)
	if err != nil {
		return err
	}
	err = preRestoreTimescale(cf.DbURI, tsInfo)
	if err != nil {
		return err
	}

	defer postRestoreTimescale(cf.DbURI, tsInfo)
	restorePath, err := getRestoreVersion()
	if err != nil {
		return err
	}

	//In order to support parallel restores, we have to first do a pre-data
	//restore, then restore only the data for the _timescaledb_catalog schema,
	//which has circular foreign key constraints and can't be restored in
	//parallel, then restore (in parallel) the data for everything else and the
	//post-data (also in parallel, this includes building indexes and the like
	//so it can be significantly faster that way)
	var baseArgs = []string{fmt.Sprintf("--dbname=%s", cf.DbURI), "--format=directory"}

	if cf.Verbose {
		baseArgs = append(baseArgs, "--verbose")
	}
	// Now just the pre-data section
	err = runRestore(restorePath, cf.PgDumpDir, baseArgs, "--section=pre-data")
	if err != nil {
		return fmt.Errorf("pg_restore run failed in pre-data section: %w", err)
	}

	//Now data for just the _timescaledb_catalog schema
	err = runRestore(restorePath, cf.PgDumpDir, baseArgs, "--section=data", "--schema=_timescaledb_catalog")
	if err != nil {
		return fmt.Errorf("pg_restore run failed while restoring _timescaledb_catalog: %w", err)
	}
	// now we can add parallel jobs to baseArgs for the rest of the process, if we have them.
	if cf.Jobs > 0 {
		baseArgs = append(baseArgs, fmt.Sprintf("--jobs=%d", cf.Jobs))
	}
	//Now the data for everything else
	err = runRestore(restorePath, cf.PgDumpDir, baseArgs, "--section=data", "--exclude-schema=_timescaledb_catalog")
	if err != nil {
		return fmt.Errorf("pg_restore run failed while restoring user data: %w", err)
	}

	//Now the full post-data run, which should also be in parallel
	err = runRestore(restorePath, cf.PgDumpDir, baseArgs, "--section=post-data")
	if err != nil {
		return fmt.Errorf("pg_restore run failed during post-data step: %w", err)
	}
	return err
}

func runRestore(restorePath string, dumpDir string, baseArgs []string, addlArgs ...string) error {
	restore := exec.Command(restorePath)
	restore.Env = append(os.Environ()) //may use this to set other environmental vars
	restore.Stdout = os.Stdout
	restore.Stderr = os.Stderr
	restore.Args = append(restore.Args, baseArgs...)
	restore.Args = append(restore.Args, addlArgs...)
	restore.Args = append(restore.Args, dumpDir) // the location of the dump has to be the last argument
	return restore.Run()
}

func getRestoreVersion() (string, error) {
	restorePath, err := exec.LookPath("pg_restore")
	if err != nil {
		return restorePath, errors.New("could not find pg_restore")
	}
	getver := exec.Command(restorePath, "--version")
	out, err := getver.CombinedOutput()
	if err != nil {
		return restorePath, fmt.Errorf("failed to get version of pg_restore: %w", err)
	}
	fmt.Printf("pg_restore version: %s\n", string(out))
	return restorePath, err
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
