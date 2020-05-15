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
