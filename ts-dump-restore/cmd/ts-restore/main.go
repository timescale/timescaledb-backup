package main

import (
	"github.com/timescale/ts-dump-restore/pkg/restore"
	"github.com/timescale/ts-dump-restore/pkg/util"
)

func main() {
	flags := &util.Config{}
	util.ParseFlags(flags)

	restore.DoRestore(flags)
}
