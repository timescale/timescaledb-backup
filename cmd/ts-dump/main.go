// This file and its contents are licensed under the Timescale License
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package main

import (
	"flag"
	"log"

	"github.com/timescale/ts-dump-restore/pkg/dump"
	"github.com/timescale/ts-dump-restore/pkg/util"
)

func main() {
	config := &util.Config{}
	config = util.RegisterCommonConfigFlags(config)
	// for dump we want to default to non-verbose output, as it is a bit too verbose
	flag.BoolVar(&config.Verbose, "verbose", false, "specifies whether verbose output is requested, default false")
	flag.BoolVar(&config.DumpRoles, "dump-roles", true, "specifies whether to use pg_dumpall to dump roles to a file, default true")
	flag.BoolVar(&config.DumpTablespaces, "dump-tablespaces", true, "specifies whether to use pg_dumpall to dump tablespaces to a file, default true")

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
