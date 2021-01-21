// This file and its contents are licensed under the Timescale License
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package main

import (
	"flag"
	"log"

	"github.com/timescale/timescaledb-backup/pkg/restore"
	"github.com/timescale/timescaledb-backup/pkg/util"
)

func main() {
	config := &util.Config{}
	config = util.RegisterCommonConfigFlags(config)
	// for restore we want to default to verbose output, it gives good information about how the restore is proceeding
	flag.BoolVar(&config.Verbose, "verbose", true, "specifies whether verbose output is requested, default true")
	flag.BoolVar(&config.DoUpdate, "do-update", true, "set to false to leave TimescaleDB at the dumped version, defaults to true, which upgrades to default installed")
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
