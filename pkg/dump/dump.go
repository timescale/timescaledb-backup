// This file and its contents are licensed under the Timescale License
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package dump

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/ts-dump-restore/pkg/util"
)

// DoDump takes a config and performs a database dump
func DoDump(cf *util.Config) error {
	// start moving jobs, we can do other things while waiting for them to stop potentially
	var wg sync.WaitGroup
	cleanup := make(chan bool)
	jobsStopped := make(chan bool)
	if cf.DumpPauseJobs && cf.Jobs > 0 {
		wg.Add(1)
		go compressionJobMover(cf.DbURI, &wg, jobsStopped, cleanup, cf.DumpPauseUDAs, cf.Verbose)
		defer compressionJobMoverStop(&wg, cleanup)
		//now we need to wait and make sure our jobs are stopped
		if cf.DumpJobFinishTimeout >= 0 {
			timer := time.NewTimer(time.Duration(cf.DumpJobFinishTimeout) * time.Second)
			select {
			case <-jobsStopped:
				break
			case timeout := <-timer.C:
				return fmt.Errorf("%s timed out waiting for jobs to be rescheduled, exiting", timeout)
			}
		}
	}
	dumpPath, err := exec.LookPath("pg_dump")
	if err != nil {
		return errors.New("pg_dump not found, please make sure it is installed")
	}
	getver := exec.Command(dumpPath, "--version")
	out, err := getver.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to get version of pg_dump: %w", err)
	}
	fmt.Printf("pg_dump version: %s\n", string(out))

	file, err := createInfoFile(cf)
	if err != nil {
		return fmt.Errorf("error with dump file creation: %w", err)
	}
	defer file.Close()

	tsInfo, err := getTimescaleInfo(cf.DbURI)
	encoder := json.NewEncoder(file)
	err = encoder.Encode(&tsInfo)
	if err != nil {
		return fmt.Errorf("Error writing Timescale info %w", err)
	}

	//We need to use pg_dumpall to dump roles and tablespaces, these may be necessary to
	//do a restore later, so best to have them around.
	if cf.DumpRoles {
		if cf.Verbose {
			fmt.Println(time.Now().Format("2006/01/02 15:04:05 ") + "Dumping roles")
		}
		err = runDumpAll(cf, "roles")
		if err != nil {
			return fmt.Errorf("Error dumping roles %w", err)
		}
	}
	if cf.DumpTablespaces {
		if cf.Verbose {
			fmt.Println(time.Now().Format("2006/01/02 15:04:05 ") + "Dumping tablespaces")
		}
		err = runDumpAll(cf, "tablespaces")
		if err != nil {
			return fmt.Errorf("Error dumping tablespaces %w", err)
		}
	}
	dump := exec.Command(dumpPath)
	dump.Args = append(dump.Args,
		fmt.Sprintf("--dbname=%s", cf.DbURI),
		"--format=directory",
		fmt.Sprintf("--file=%s", cf.PgDumpDir))
	if cf.Verbose {
		dump.Args = append(dump.Args, "--verbose")
	}
	if cf.Jobs > 0 {
		dump.Args = append(dump.Args, fmt.Sprintf("--jobs=%d", cf.Jobs))
	}

	err = util.RunCommandAndFilterOutput(dump, os.Stdout, os.Stderr, true)
	if err != nil {
		return fmt.Errorf("pg_dump run failed with: %w", err)
	}
	return err
}

func createInfoFile(cf *util.Config) (*os.File, error) {
	err := os.Mkdir(string(cf.DumpDir), 0700)
	if err != nil {
		return nil, err
	}
	file, err := os.Create(cf.TsInfoFileName)
	return file, err
}

func runDumpAll(cf *util.Config, dumpType string) error {
	//For now, we are going to assume that pg_dumpall is the same version as pg_dump and
	//not print it out or anything.
	dumpAllPath, err := exec.LookPath("pg_dumpall")
	if err != nil {
		return errors.New("pg_dumpall not found, please make sure it is installed")
	}
	dumpPath := filepath.Join(cf.DumpDir, dumpType+".sql")
	if dumpType == "roles" {
		dumpType = "--roles-only"
	} else if dumpType == "tablespaces" {
		dumpType = "--tablespaces-only"
	} else {
		return errors.New("unrecognized pg_dumpall type")
	}
	config, err := pgx.ParseConfig(cf.DbURI)
	if err != nil {
		return fmt.Errorf("invalid connection string: %w", err)
	}
	dumpAll := exec.Command(dumpAllPath)
	dumpAll.Args = append(dumpAll.Args,
		fmt.Sprintf("--dbname=%s", cf.DbURI),
		fmt.Sprintf("--database=%s", config.Config.Database), // tells pg_dumpall to actually connect to that database to do things
		fmt.Sprintf("--file=%s", dumpPath),
		"--no-role-passwords", //dump roles without passwords, will have to have folks reset passwords for now, potentially add flag in future, but doesn't work on cloud etc as it needs access to pg_authid
		dumpType)
	dumpAll.Stdout = os.Stdout
	dumpAll.Stderr = os.Stderr
	return dumpAll.Run()
}

func getTimescaleInfo(dbURI string) (util.TsInfo, error) {
	info := util.TsInfo{}

	conn, err := util.GetDBConn(context.Background(), dbURI)
	if err != nil {
		return info, err
	}
	defer conn.Close(context.Background())

	err = conn.QueryRow(context.Background(), "SELECT e.extversion,  n.nspname FROM pg_extension e INNER JOIN pg_namespace n ON e.extnamespace = n.oid WHERE e.extname='timescaledb'").Scan(&info.TsVersion, &info.TsSchema)
	if err != nil {
		if err == pgx.ErrNoRows {
			return info, errors.New("TimescaleDB extension not found, make sure it is installed in the database being dumped")
		}
		return info, err
	}
	return info, err
}

// Compression jobs (or any other sort of job that performs DDL on a chunk such as
// reorder) running at the wrong time can cause deadlocks in the following way: 1)
// compression job takes write lock & main dump process takes read lock (no conflict) 2)
// compression job requests exclusive lock (waits on main dump process) 3) child dump
// process requests read lock (waits on compression process). Essentially, any concurrent
// DDL operation could cause this problem, but the user can put off DDL, but this is
// harder with scheduled operations like compression jobs.

// The simple solution would seem to be pausing jobs while we are doing the dump and
// resuming them when we've finished. Unfortunately, a full pause is problematic as, if
// the dump process were to crash, you'd end up with your jobs in a state where they would
// never run. In order to avoid that, we instead create a process that checks for
// problematic jobs scheduled to start in the next ten minutes and moves them into the
// future by 15-20 minutes. This way, if the dump process crashes, the worst that can
// happen is that your job is put off by 20 minutes. We also keep track of any jobs we
// rescheduled and try to put them back on schedule when we finish the dump.
func compressionJobMover(dbURI string, wg *sync.WaitGroup, jobsStopped chan bool, cleanup chan bool, pauseUDAs bool, verbose bool) {
	defer wg.Done()

	movedJobs := make(map[int64]time.Time)
	runningJobs := true

	ticker := time.NewTicker(30 * time.Second)
	conn, err := util.GetDBConn(context.Background(), dbURI)
	if err != nil {
		compressionJobMoverWarn(err)
		return
	}
	defer conn.Close(context.Background())
	var tsMajorVersion int
	err = conn.QueryRow(context.Background(), "SELECT split_part(extversion, '.', 1)::INT FROM pg_catalog.pg_extension WHERE extname='timescaledb' LIMIT 1").Scan(&tsMajorVersion)
	if err != nil {
		compressionJobMoverWarn(err)
		return
	}
	runningJobsSQL, moveSQL, replaceJobSQL, err := compressionJobMoverSQL(tsMajorVersion, pauseUDAs)
	if err != nil {
		compressionJobMoverWarn(err)
		return
	}
	var runningJobIDs string
	for {
		rows, err := conn.Query(context.Background(), moveSQL)
		if err != nil {
			compressionJobMoverWarn(err)
			return
		}
		var (
			jobID     int64
			origStart time.Time
		)
		for rows.Next() {
			if err = rows.Scan(&jobID, &origStart); err != nil {
				compressionJobMoverWarn(err)
				return
			}
			if _, ok := movedJobs[jobID]; !ok {
				if verbose {
					fmt.Printf("%sMoved Job: %d\n", time.Now().Format("2006/01/02 15:04:05 "), jobID)
				}
				movedJobs[jobID] = origStart
			}
		}
		rows.Close()
		if runningJobs {

			err = conn.QueryRow(context.Background(), runningJobsSQL).Scan(&runningJobIDs)
			if err != nil && err != pgx.ErrNoRows {
				compressionJobMoverWarn(err)
				return
			}
			if err == pgx.ErrNoRows {
				err = nil
				runningJobs = false
				fmt.Printf("%sJobs: %s have stopped, continuing \n ", time.Now().Format("2006/01/02 15:04:05 "), runningJobIDs)
				jobsStopped <- true // signal back to our parent that it's safe to move on, the jobs have stopped
			}
			if runningJobs {
				if verbose {
					fmt.Printf("%sBackgroud jobs %s are currently running\n these jobs may cause the dump to fail by causing deadlocks, the main dump will wait for them to finish \n ", time.Now().Format("2006/01/02 15:04:05 "), runningJobIDs)
				}
			}
		}
		select {
		case <-cleanup:
			var jobMoved bool
			for jobID, origStart := range movedJobs {
				if verbose {
					fmt.Printf("%sReplacing Job: %d\n", time.Now().Format("2006/01/02 15:04:05 "), jobID)
				}
				err = conn.QueryRow(context.Background(), replaceJobSQL, jobID, origStart).Scan(&jobMoved)
				if err != nil && err != pgx.ErrNoRows {
					compressionJobMoverWarn(err)
					return
				}
			}
			return
		case <-ticker.C:
			continue
		}
	}
}

func compressionJobMoverWarn(err error) {
	fmt.Println(time.Now().Format("2006/01/02 15:04:05 ")+"WARNING: ", fmt.Errorf("problem while rescheduling jobs: %w", err))
}

func compressionJobMoverStop(wg *sync.WaitGroup, cleanup chan bool) {
	cleanup <- true
	wg.Wait()
}

func compressionJobMoverSQL(tsMajorVersion int, pauseUDAs bool) (runningJobsSQL string, moveSQL string, replaceJobSQL string, err error) {
	err = nil
	if tsMajorVersion == 1 {
		// Note that jobs with last_finish and next_start = -infinity are the jobs that are
		// currently running. Unfortunately, there's no other indication of this, and it's
		// hard to know if it's a real indication or if it might lie to use sometimes.
		runningJobsSQL = `SELECT string_agg(job_id::text, ', ') 
		FROM timescaledb_information.policy_stats 
		WHERE last_finish ='-infinity' AND next_start = '-infinity' 
		AND job_type IN ('compress_chunks', 'reorder') 
		HAVING string_agg(job_id::text, ', ') IS NOT NULL`

		moveSQL = `SELECT t.job_id, prev_start FROM 
			(SELECT (alter_job_schedule(p.job_id, next_start=> now()+ ('15 min' + random()*'5 min'::interval))).*, p.next_start as prev_start
			FROM timescaledb_information.policy_stats p 
			WHERE NOT(last_finish ='-infinity' AND next_start = '-infinity') 
			AND next_start < now()+'10 min' 
			AND p.job_type IN ('compress_chunks', 'reorder') ) t`

		// don't want to move jobs back and have them be completely synced up now, so add a
		// little randomness if we've passed them by
		replaceJobSQL = `SELECT (alter_job_schedule($1, next_start=>CASE WHEN $2::timestamptz > now() THEN $2::timestamptz ELSE now()+ random()*'5 min'::interval END)).job_id = $1`
	} else if tsMajorVersion == 2 {
		var jobWhere string

		// Not a huge fan of finding the job types this way, but I'm not sure of a better one.
		if pauseUDAs {
			jobWhere = "(application_name LIKE 'Compression%' OR application_name LIKE 'Reorder%' OR application_name LIKE 'User-Defined%')"
		} else {
			jobWhere = "(application_name LIKE 'Compression%' OR application_name LIKE 'Reorder%')"
		}
		// Note that jobs with next_start = -infinity and last_run_status = NULL and job_status = 'Scheduled'
		// currently running. Unfortunately, there's no other indication of this, and it's
		// hard to know if it's a real indication or if it might lie to us	 sometimes.
		runningJobsSQL = fmt.Sprintf(`SELECT string_agg(j.job_id::text, ', ') 
		FROM timescaledb_information.jobs j 
		INNER JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id 
		WHERE js.next_start='-infinity' AND js.last_run_status IS NULL AND js.job_status = 'Scheduled' 
		AND %s
		HAVING string_agg(j.job_id::text, ', ') IS NOT NULL`, jobWhere)

		moveSQL = fmt.Sprintf(`SELECT t.job_id, prev_start FROM 
			(SELECT (alter_job(js.job_id, next_start=> now()+ ('15 min' + random()*'5 min'::interval))).*, js.next_start as prev_start
			FROM timescaledb_information.jobs j 
			INNER JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id  
			WHERE NOT(js.next_start='-infinity' AND js.last_run_status IS NULL AND js.job_status = 'Scheduled' ) 
			AND js.next_start < now()+'10 min' 
			AND %s ) t`, jobWhere)

		// don't want to move jobs back and have them be completely synced up now, so add a
		// little randomness if we've passed them by
		replaceJobSQL = `SELECT (alter_job($1, next_start=>CASE WHEN $2::timestamptz > now() THEN $2::timestamptz ELSE now()+ random()*'5 min'::interval END)).job_id = $1`
	} else {
		err = fmt.Errorf("unknown Timescale major version")
	}

	return runningJobsSQL, moveSQL, replaceJobSQL, err
}
