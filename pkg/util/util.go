package util

import (
	"context"
	"flag"
	"fmt"
	"log"
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
func CleanConfig(cf *Config) *Config {
	dd, err := filepath.Abs(cf.DumpDir)
	if err != nil {
		log.Fatal(err)
	}

	cf.DumpDir = dd
	cf.PgDumpDir = filepath.Join(cf.DumpDir, "pgdump")
	cf.TsInfoFileName = filepath.Join(cf.DumpDir, "timescaleVersionInfo.json")
	return cf
}

//GetDBConn returns a pgx Conn from a dbURI
func GetDBConn(dbContext context.Context, dbURI string) *pgx.Conn {
	config, err := pgx.ParseConfig(dbURI)
	if err != nil {
		log.Fatalf("invalid connection string: %s\n", err)
	}
	conn, err := pgx.ConnectConfig(dbContext, config)
	if err != nil {
		log.Fatalf("invalid connection string: %s\n", err)
	}
	return conn
}

//CreateTimescaleAtVer takes in a dbURI, a Timescale version and schema and
// cleans out any version of the extension that exists, then creates the version
// we want in the correct schema etc.
func CreateTimescaleAtVer(dbContext context.Context, dbURI string, targetSchema string, targetVersion string) {
	conn := GetDBConn(dbContext, dbURI)
	//First connect and clean out any old versions
	_, err := conn.Exec(dbContext, `DROP EXTENSION IF EXISTS timescaledb`)
	if err != nil {
		log.Fatal("Error dropping old extension version: ", err)
	}
	// Now have to close and reconnect to prevent loading different versions in same backend.
	conn.Close(dbContext)
	conn = GetDBConn(dbContext, dbURI)
	defer conn.Close(dbContext)
	stmnt := fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA %s VERSION '%s'", targetSchema, targetVersion)
	fmt.Println(stmnt)
	_, err = conn.Exec(dbContext, stmnt)
	if err != nil {
		log.Fatal("Error creating extension in correct schema and at correct version: ", err)
	}
	// Confirm that worked correctly
	info := TsInfo{}
	err = conn.QueryRow(context.Background(), "SELECT e.extversion, n.nspname FROM pg_extension e INNER JOIN pg_namespace n on e.extnamespace = n.oid WHERE e.extname='timescaledb'").Scan(&info.TsVersion, &info.TsSchema)
	if err != nil {
		if err == pgx.ErrNoRows {
			log.Fatal("Could not confirm creation of TimescaleDB extension")
		}
		log.Fatal(err)
	}
	if info.TsSchema != targetSchema || info.TsVersion != targetVersion {
		log.Fatal("TimescaleDB extension created in incorrect schema or at incorrect version, please drop the extension and restart the restore.")
	}
}
