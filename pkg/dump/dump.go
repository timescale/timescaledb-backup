// This file and its contents are licensed under the Timescale License
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package dump

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

// DoDump takes a config and performs a database dump
func DoDump(cf *util.Config) error {
	dumpPath, err := exec.LookPath("pg_dump")
	if err != nil {
		return errors.New("pg_dump not found, please make sure it is installed")
	}
	getver := exec.Command(dumpPath, "--version")
	out, err := getver.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to get version of pg_dump: %w", err)
	}
	fmt.Printf("pg_dump version: %s\n", string(out))

	file, err := createInfoFile(cf)
	if err != nil {
		return fmt.Errorf("error with dump file creation: %w", err)
	}
	defer file.Close()

	tsInfo, err := getTimescaleInfo(cf.DbURI)
	encoder := json.NewEncoder(file)
	err = encoder.Encode(&tsInfo)
	if err != nil {
		return fmt.Errorf("Error writing Timescale info %w", err)
	}

	dump := exec.Command(dumpPath)
	dump.Args = append(dump.Args,
		fmt.Sprintf("--dbname=%s", cf.DbURI),
		"--format=directory",
		fmt.Sprintf("--file=%s", cf.PgDumpDir))
	if cf.Verbose {
		dump.Args = append(dump.Args, "--verbose")
	}
	if cf.Jobs > 0 {
		dump.Args = append(dump.Args, fmt.Sprintf("--jobs=%d", cf.Jobs))
	}
	dump.Stdout = os.Stdout
	dump.Stderr = os.Stderr
	err = dump.Run()
	if err != nil {
		return fmt.Errorf("pg_dump run failed with: %w", err)
	}
	return err
}

func createInfoFile(cf *util.Config) (*os.File, error) {
	//Are these perms correct?
	err := os.Mkdir(string(cf.DumpDir), 0700)
	if err != nil {
		//is this what I should be returning in this case? or should I just throw the error here?
		return nil, err
	}
	file, err := os.Create(cf.TsInfoFileName)
	return file, err
}

func getTimescaleInfo(dbURI string) (util.TsInfo, error) {
	info := util.TsInfo{}

	conn, err := util.GetDBConn(context.Background(), dbURI)
	if err != nil {
		return info, err
	}
	defer conn.Close(context.Background())

	err = conn.QueryRow(context.Background(), "SELECT e.extversion,  n.nspname FROM pg_extension e INNER JOIN pg_namespace n ON e.extnamespace = n.oid WHERE e.extname='timescaledb'").Scan(&info.TsVersion, &info.TsSchema)
	if err != nil {
		if err == pgx.ErrNoRows {
			return info, errors.New("TimescaleDB extension not found, make sure it is installed in the database being dumped")
		}
		return info, err
	}
	return info, err
}
