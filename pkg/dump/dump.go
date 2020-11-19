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
	"path/filepath"
	"time"

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

	//We need to use pg_dumpall to dump roles and tablespaces, these may be necessary to
	//do a restore later, so best to have them around.
	if cf.DumpRoles {
		if cf.Verbose {
			fmt.Println(time.Now().Format("2006/01/02 15:04:05 ") + "Dumping roles")
		}
		err = runDumpAll(cf, "roles")
		if err != nil {
			return fmt.Errorf("Error dumping roles %w", err)
		}
	}
	if cf.DumpTablespaces {
		if cf.Verbose {
			fmt.Println(time.Now().Format("2006/01/02 15:04:05 ") + "Dumping tablespaces")
		}
		err = runDumpAll(cf, "tablespaces")
		if err != nil {
			return fmt.Errorf("Error dumping tablespaces %w", err)
		}
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
	err = util.RunCommandAndFilterOutput(dump, os.Stdout, os.Stderr, true)
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

func runDumpAll(cf *util.Config, dumpType string) error {
	//For now, we are going to assume that pg_dumpall is the same version as pg_dump and
	//not print it out or anything.
	dumpAllPath, err := exec.LookPath("pg_dumpall")
	if err != nil {
		return errors.New("pg_dumpall not found, please make sure it is installed")
	}
	dumpPath := filepath.Join(cf.DumpDir, dumpType+".sql")
	if dumpType == "roles" {
		dumpType = "--roles-only"
	} else if dumpType == "tablespaces" {
		dumpType = "--tablespaces-only"
	} else {
		return errors.New("unrecognized pg_dumpall type")
	}
	config, err := pgx.ParseConfig(cf.DbURI)
	if err != nil {
		return fmt.Errorf("invalid connection string: %w", err)
	}
	dumpAll := exec.Command(dumpAllPath)
	dumpAll.Args = append(dumpAll.Args,
		fmt.Sprintf("--dbname=%s", cf.DbURI),
		fmt.Sprintf("--database=%s", config.Config.Database), // tells pg_dumpall to actually connect to that database to do things
		fmt.Sprintf("--file=%s", dumpPath),
		"--no-role-passwords", //dump roles without passwords, will have to have folks reset passwords for now, potentially add flag in future, but doesn't work on cloud etc as it needs access to pg_authid
		dumpType)
	dumpAll.Stdout = os.Stdout
	dumpAll.Stderr = os.Stderr
	return dumpAll.Run()
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
