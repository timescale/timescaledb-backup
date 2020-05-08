package main

import (
	"flag"
	"log"

	"github.com/timescale/ts-dump-restore/pkg/dump"
	"github.com/timescale/ts-dump-restore/pkg/util"
)

func main() {
	config := &util.Config{}
	config = util.RegisterConfigFlags(config)

	flag.Parse()
	config, err := util.CleanConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	err = dump.DoDump(config)
	if err != nil {
		log.Fatal(err)
	}
}
