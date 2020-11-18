// This file and its contents are licensed under the Timescale License
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package restore

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"

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

	//Because of several odd limitations we can't do a simple restore here,
	//we're going to need to perform the restore in multiple steps. The main
	//goals this allows us to reach are 1) supporting parallel data restores,
	//which are significantly faster for hypertables, 2) supporting restores to
	//services in which we don't have superuser access (ie Cloud and Forge).

	//We have to create a table of contents, see note on makeRestoreTOC
	TOCFile, err := ioutil.TempFile("", "ts_restore_toc")
	if err != nil {
		return fmt.Errorf("pg_restore run failed while creating TOC file: %w", err)
	}
	defer os.Remove(TOCFile.Name())
	err = makeRestoreTOC(restorePath, cf.PgDumpDir, TOCFile)
	if err != nil {
		return fmt.Errorf("pg_restore run failed while writing TOC file: %w", err)
	}
	err = TOCFile.Close()
	if err != nil {
		return fmt.Errorf("pg_restore run failed while closing TOC file: %w", err)
	}
	//In order to support parallel restores, we have to first do a pre-data
	//restore, then restore only the data for the _timescaledb_catalog and
	//_timescaledb_config schemas, which has circular foreign key constraints
	//and can't be restored in parallel, then restore (in parallel) the data for
	//everything else and the post-data (also in parallel, this includes
	//building indexes and the like so it can be significantly faster that way)

	var baseArgs = []string{fmt.Sprintf("--dbname=%s", cf.DbURI), "--format=directory", fmt.Sprintf("--use-list=%s", TOCFile.Name())}

	if cf.Verbose {
		baseArgs = append(baseArgs, "--verbose")
	}
	// Now just the pre-data section
	restore := getRestoreCmd(restorePath, cf.PgDumpDir, baseArgs, "--section=pre-data", "--single-transaction")
	err = util.RunCommandAndFilterOutput(restore, os.Stdout, os.Stderr, true)
	if err != nil {
		return fmt.Errorf("pg_restore run failed in pre-data section: %w", err)
	}
	//Now data for just the _timescaledb_catalog and _timescaledb_config  schemas
	restore = getRestoreCmd(restorePath, cf.PgDumpDir, baseArgs, "--section=data", "--schema=_timescaledb_catalog", "--schema=_timescaledb_config")
	err = util.RunCommandAndFilterOutput(restore, os.Stdout, os.Stderr, true)
	if err != nil {
		return fmt.Errorf("pg_restore run failed while restoring _timescaledb_catalog: %w", err)
	}
	// now we can add parallel jobs to baseArgs for the rest of the process, if we have them.
	if cf.Jobs > 0 {
		baseArgs = append(baseArgs, fmt.Sprintf("--jobs=%d", cf.Jobs))
	}
	//Now the data for everything else
	restore = getRestoreCmd(restorePath, cf.PgDumpDir, baseArgs, "--section=data", "--exclude-schema=_timescaledb_catalog", "--exclude-schema=_timescaledb_config")
	err = util.RunCommandAndFilterOutput(restore, os.Stdout, os.Stderr, true)
	if err != nil {
		return fmt.Errorf("pg_restore run failed while restoring user data: %w", err)
	}

	//Now the full post-data run, which should also be in parallel
	restore = getRestoreCmd(restorePath, cf.PgDumpDir, baseArgs, "--section=post-data")
	err = util.RunCommandAndFilterOutput(restore, os.Stdout, os.Stderr, true)
	if err != nil {
		return fmt.Errorf("pg_restore run failed during post-data step: %w", err)
	}

	//Now perform the extension update if we're doing that.
	if cf.DoUpdate {
		err = doUpdate(cf.DbURI)
		if err != nil {
			return fmt.Errorf("pg_restore run failed while updating extension: %w", err)
		}
	}
	return err

}

func getRestoreCmd(restorePath string, dumpDir string, baseArgs []string, addlArgs ...string) *exec.Cmd {
	restore := exec.Command(restorePath)
	restore.Env = append(os.Environ()) //may use this to set other environmental vars
	restore.Args = append(restore.Args, baseArgs...)
	restore.Args = append(restore.Args, addlArgs...)
	restore.Args = append(restore.Args, dumpDir) // the location of the dump has to be the last argument
	return restore
}

//makeRestoreTOC creates a filtered table of contents in a temp file to use for
//the rest of the restore.
//Some background: In order to support non-superuser restores without errors due
//to a few objects not having the correct permissions, we first have to create a
//table of contents (TOC) file and filter out a couple of lines. This TOC will
//no longer include the comment on the extension, which, because there is
//apparently no such thing as an owner of an extension, see:
//https://www.postgresql.org/message-id/CANu8Fixm7w5RoCO95n_ETcR%2BmcVQd-FkBgAOic-9H%2BZYrSeSwg%40mail.gmail.com
//Because comments on objects can be modified by superusers or objects' owners,
//modifying a comment on an extension requires superuser permissions. This means
//that this command in the restore will error, though we really do not care if
//it does, as the comment is just being set back to the default anyway. However,
//we cannot distinguish easily between this error and a real error that could
//have caused real problems, so we just do not perform the restore of the
//comment.
func makeRestoreTOC(restorePath string, dumpDir string, TOCFile *os.File) error {
	restore := exec.Command(restorePath)
	restore.Args = append(restore.Args, dumpDir)
	restore.Args = append(restore.Args, "--list")
	TOCWriter := bufio.NewWriter(TOCFile)
	return util.RunCommandAndFilterOutput(restore, TOCWriter, os.Stderr, false, "COMMENT - EXTENSION timescaledb")
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
	return err
}

func doUpdate(dbURI string) error {

	conn, err := util.GetDBConn(context.Background(), dbURI)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), "ALTER EXTENSION timescaledb UPDATE ")
	if err != nil {
		return fmt.Errorf("failed to update extension version: %w", err)
	}
	conn.Close(context.Background())                          // close the alter extension connection
	conn2, err := util.GetDBConn(context.Background(), dbURI) // open a new one to confirm we can make a connection
	if err != nil {
		return fmt.Errorf("failed to connect after updating extension:%w", err)
	}
	defer conn2.Close(context.Background())

	// confirm that the installed version now matches the default version
	var vm bool
	err = conn2.QueryRow(context.Background(), "SELECT installed_version = default_version FROM pg_catalog.pg_available_extensions WHERE name = 'timescaledb'").Scan(&vm)
	if err != nil {
		return err
	}
	if !vm {
		return errors.New("TimescaleDB extension was not updated to the default version")
	}
	return err
}
