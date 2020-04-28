package main

import (
	"github.com/timescale/ts-dump-restore/pkg/dump"
	"github.com/timescale/ts-dump-restore/pkg/util"
)

func main() {
	flags := &util.Config{}
	util.ParseFlags(flags)

	dump.DoDump(flags)
}
