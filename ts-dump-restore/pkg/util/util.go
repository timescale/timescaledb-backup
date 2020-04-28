package util

import (
	"context"
	"flag"
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

//ParseFlags parses user input and places it into the config struct
func ParseFlags(cf *Config) *Config {
	flag.StringVar(&cf.DbURI, "db-URI", "", "The PostgreSQL URI in postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...] format")
	flag.StringVar(&cf.DumpDir, "dump-dir", "", "The directory to place the dump in or to restore from")
	flag.Parse()
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
