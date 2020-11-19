// This file and its contents are licensed under the Timescale License
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package util

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
)

//Config holds configuration needed for the dump/restore commands to work properly
type Config struct {
	DbURI           string
	DumpDir         string
	PgDumpDir       string
	TsInfoFileName  string
	Verbose         bool
	Jobs            int
	DoUpdate        bool // whether to do an update after restoring.
	DumpRoles       bool
	DumpTablespaces bool
}

//TsInfo holds information about the Timescale installation
type TsInfo struct {
	TsInfoVersion int //TODO: How to do versioning here? Should this happen?
	TsVersion     string
	TsSchema      string
}

//RegisterCommonConfigFlags registers user input flags common to both dump and restore (incl defaults) in the config struct
func RegisterCommonConfigFlags(cf *Config) *Config {
	flag.StringVar(&cf.DbURI, "db-URI", "", "the PostgreSQL URI in postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...] format")
	flag.StringVar(&cf.DumpDir, "dump-dir", "", "the directory to place the dump in or to restore from")
	flag.IntVar(&cf.Jobs, "jobs", 4, "specifies whether parallel jobs will be used, defaults to 4, set to 0 to disable parallelism")
	return cf
}

//CleanConfig cleans and standardizes user input as well as enriching the config with derived values
func CleanConfig(cf *Config) (*Config, error) {
	dd, err := filepath.Abs(cf.DumpDir)
	if err != nil {
		return cf, err
	}

	cf.DumpDir = dd
	cf.PgDumpDir = filepath.Join(cf.DumpDir, "pgdump")
	cf.TsInfoFileName = filepath.Join(cf.DumpDir, "timescaleVersionInfo.json")
	return cf, err
}

//GetDBConn returns a pgx Conn from a dbURI
func GetDBConn(dbContext context.Context, dbURI string) (*pgx.Conn, error) {
	config, err := pgx.ParseConfig(dbURI)
	if err != nil {
		return nil, fmt.Errorf("invalid connection string: %w", err)
	}
	conn, err := pgx.ConnectConfig(dbContext, config)
	if err != nil {
		return nil, err
	}
	return conn, err
}

//CreateTimescaleAtVer takes in a dbURI, a Timescale version and schema and
// cleans out any version of the extension that exists, then creates the version
// we want in the correct schema etc.
func CreateTimescaleAtVer(dbContext context.Context, dbURI string, targetSchema string, targetVersion string) error {
	conn, err := GetDBConn(dbContext, dbURI)
	if err != nil {
		return err
	}
	defer conn.Close(dbContext)
	//First connect and clean out any old versions We drop the extension without
	//cascade so we will error if there are any dependencies, this is mostly
	//there in cases where the extension is created due to the template db
	_, err = conn.Exec(dbContext, `DROP EXTENSION IF EXISTS timescaledb`)
	if err != nil {
		return fmt.Errorf("Error dropping old extension version: %w", err)
	}

	// Need a new connection now to prevent odd loading issues with different ext versions
	conn.Close(dbContext)
	conn, err = GetDBConn(dbContext, dbURI)
	if err != nil {
		return err
	}
	defer conn.Close(dbContext)
	stmnt := fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA %s VERSION '%s'", targetSchema, targetVersion)
	_, err = conn.Exec(dbContext, stmnt)
	if err != nil {
		return fmt.Errorf("Error creating extension in correct schema and at correct version: %w", err)
	}
	// Confirm that worked correctly
	info := TsInfo{}
	err = conn.QueryRow(context.Background(), "SELECT e.extversion, n.nspname FROM pg_extension e INNER JOIN pg_namespace n on e.extnamespace = n.oid WHERE e.extname='timescaledb'").Scan(&info.TsVersion, &info.TsSchema)
	if err != nil {
		if err == pgx.ErrNoRows {
			return errors.New("Could not confirm creation of TimescaleDB extension")
		}
		return err
	}
	if info.TsSchema != targetSchema || info.TsVersion != targetVersion {
		return errors.New("TimescaleDB extension created in incorrect schema or at incorrect version, please drop the extension and restart the restore")
	}
	return err
}

//RunCommandAndFilterOutput runs a specified command (cmd), and writes output to stdout and stderr after applying
//filters, if prepend time is specified, then it also prepends the time that an output was produced.
func RunCommandAndFilterOutput(cmd *exec.Cmd, stdout io.Writer, stderr io.Writer, prependTime bool, filters ...string) error {
	stdoutIn, _ := cmd.StdoutPipe()
	stderrIn, _ := cmd.StderrPipe()

	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("cmd.Start() failed with '%w'", err)
	}

	var errStdout, errStderr error
	// cmd.Wait() should be called only after we finish reading
	// from stdoutIn and stderrIn.
	// wg ensures that we finish
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		errStdout = writeAndFilterOutput(stdoutIn, stdout, prependTime, filters...)
		wg.Done()
	}()

	errStderr = writeAndFilterOutput(stderrIn, stderr, prependTime, filters...)
	wg.Wait()
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("cmd.Run() failed with '%w'", err)
	}
	if errStdout != nil || errStderr != nil {
		return fmt.Errorf("failed to capture output: '%w'", err)
	}
	return err
}

func writeAndFilterOutput(readIn io.Reader, scanOut io.Writer, prependTime bool, filters ...string) error {
	scanIn := bufio.NewScanner(readIn)
	var err error
	for scanIn.Scan() {
		stringMatch := false

		for _, filter := range filters {
			if strings.Contains(scanIn.Text(), filter) {
				stringMatch = true
				break
			}
		}
		if !stringMatch {
			if prependTime {
				_, err = fmt.Fprintln(scanOut, time.Now().Format("2006/01/02 15:04:05 ")+scanIn.Text())
			} else {
				_, err = fmt.Fprintln(scanOut, scanIn.Text())
			}
			if err != nil {
				return err
			}
		}
	}
	return err
}
