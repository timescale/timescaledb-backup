package main

import (
	"flag"

	"github.com/timescale/ts-dump-restore/pkg/dump"
	"github.com/timescale/ts-dump-restore/pkg/util"
)

func main() {
	config := &util.Config{}
	util.RegisterConfigFlags(config)
	flag.Parse()
	util.CleanConfig(config)
	dump.DoDump(config)
}
