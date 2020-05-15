package main

import (
	"flag"
	"log"

	"github.com/timescale/ts-dump-restore/pkg/restore"
	"github.com/timescale/ts-dump-restore/pkg/util"
)

func main() {
	config := &util.Config{}
	config = util.RegisterCommonConfigFlags(config)
	// for restore we want to default to verbose output, it gives good information about how the restore is proceeding
	flag.BoolVar(&config.Verbose, "verbose", true, "specifies whether verbose output is requested, default true")

	flag.Parse()
	config, err := util.CleanConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	err = restore.DoRestore(config)
	if err != nil {
		log.Fatal(err)
	}
}
