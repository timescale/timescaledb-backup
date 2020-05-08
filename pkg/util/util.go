package util

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"path/filepath"

	"github.com/jackc/pgx/v4"
)

//Config holds configuration needed for the dump/restore commands to work properly
type Config struct {
	DbURI          string
	DumpDir        string
	PgDumpDir      string
	TsInfoFileName string
}

//TsInfo holds information about the Timescale installation
type TsInfo struct {
	TsInfoVersion int //TODO: How to do versioning here? Should this happen?
	TsVersion     string
	TsSchema      string
}

//RegisterConfigFlags registers user input flags in the config struct
func RegisterConfigFlags(cf *Config) *Config {
	flag.StringVar(&cf.DbURI, "db-URI", "", "The PostgreSQL URI in postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...] format")
	flag.StringVar(&cf.DumpDir, "dump-dir", "", "The directory to place the dump in or to restore from")
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
