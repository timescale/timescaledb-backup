package dump

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

// DoDump takes a config and performs a database dump
func DoDump(cf *util.Config) {
	dumpPath, err := exec.LookPath("pg_dump")
	if err != nil {
		log.Fatalf("could not find pg_dump")
	}
	getver := exec.Command(dumpPath, "--version")
	out, err := getver.CombinedOutput()
	if err != nil {
		log.Fatalf("Failed to get version of pg_dump: %s\n", err)
	}
	fmt.Printf("pg_dump version: %s\n", string(out))

	file, err := createInfoFile(cf)
	if err != nil {
		log.Fatalf("Error with dump file creation: %s\n", err)
	}
	defer file.Close()

	tsInfo := getTimescaleInfo(cf.DbURI)
	encoder := json.NewEncoder(file)
	err = encoder.Encode(&tsInfo)
	if err != nil {
		log.Fatalf("Error writing timescale infor %v", err)
	}

	dump := exec.Command(dumpPath)
	dump.Args = append(dump.Args,
		string(cf.DbURI),
		"--format=directory",
		fmt.Sprintf("--file=%s", cf.PgDumpDir))
	dump.Stdout = os.Stdout
	dump.Stderr = os.Stderr
	err = dump.Run()
	if err != nil {
		log.Fatalf("pg_dump run failed with: %s\n", err)
	}
	return
}

func createInfoFile(cf *util.Config) (*os.File, error) {
	//Are these perms correct?
	err := os.Mkdir(string(cf.DumpDir), os.ModeDir)
	if err != nil {
		//is this what I should be returning in this case? or should I just throw the error here?
		return nil, err
	}
	file, err := os.Create(cf.TsInfoFileName)
	return file, err
}

func getTimescaleInfo(dbURI string) util.TsInfo {
	conn := util.GetDBConn(context.Background(), dbURI)
	defer conn.Close(context.Background())

	info := util.TsInfo{}
	err := conn.QueryRow(context.Background(), "SELECT e.extversion,  n.nspname FROM pg_extension e INNER JOIN pg_namespace n ON e.extnamespace = n.oid WHERE e.extname='timescaledb'").Scan(&info.TsVersion, &info.TsSchema)
	if err != nil {
		if err == pgx.ErrNoRows {
			log.Fatal("TimescaleDB extension not found, is it installed in the database you are dumping?")
		}
		log.Fatal(err)
	}
	return info
}
