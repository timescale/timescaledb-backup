// This file and its contents are licensed under the Timescale License
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package dump

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/ts-dump-restore/pkg/util"
)

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

//JobMover moves compression or any DDL performing jobs in the database specified by
//dbURI, it is meant to be run in a separate goroutine and will also signal via
//jobsStopped channel that these jobs have stopped. It will stop if it is told to on the
//cleanup channel. The waitgroup wg is for coordinating cleanup. pauseUDAs specifies
//whether we should pause user-defined actions in Timescale 2+ and verbose controls how
//much information we print about what's going on.
func JobMover(dbURI string, wg *sync.WaitGroup, jobsStopped chan<- bool, cleanup <-chan bool, pauseUDAs bool, verbose bool) {
	defer wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	conn, err := util.GetDBConn(context.Background(), dbURI) //only want to use a single connection here so we'll pass it around
	if jobMoverWarn(err) {
		return
	}
	defer conn.Close(context.Background())
	// need to get the Timescale major version to determine the set of SQL statements we'll use throughout
	var tsMajorVersion int
	err = conn.QueryRow(context.Background(), "SELECT split_part(extversion, '.', 1)::INT FROM pg_catalog.pg_extension WHERE extname='timescaledb' LIMIT 1").Scan(&tsMajorVersion)
	if jobMoverWarn(err) {
		return
	}
	runningJobsSQL, moveSQL, replaceJobSQL, err := jobMoverSQL(tsMajorVersion, pauseUDAs)
	if jobMoverWarn(err) {
		return
	}

	// Now the main event where we wait move jobs in a loop, always do it at least once
	movedJobs := make(map[int64]time.Time)
	runningJobs := true //assume we have running jobs until proven otherwise
	for {
		err = moveScheduledJobs(conn, movedJobs, verbose, moveSQL)
		if jobMoverWarn(err) {
			return
		}
		if runningJobs {
			runningJobs, err = probeRunningJobs(conn, jobsStopped, verbose, runningJobsSQL)
			if jobMoverWarn(err) {
				return
			}
		}
		select {
		case <-cleanup:
			err = rescheduleJobs(conn, movedJobs, verbose, replaceJobSQL)
			_ = jobMoverWarn(err)
			return
		case <-ticker.C:
			continue
		}
	}
}

func probeRunningJobs(conn *pgx.Conn, jobsStopped chan<- bool, verbose bool, runningJobsSQL string) (runningJobs bool, err error) {
	var runningJobIDs string
	err = conn.QueryRow(context.Background(), runningJobsSQL).Scan(&runningJobIDs)
	if err != nil && err != pgx.ErrNoRows {
		return true, err
	}
	if err == pgx.ErrNoRows {
		err = nil
		runningJobs = false
		fmt.Printf("%sJobs: %s have stopped, continuing \n ", time.Now().Format("2006/01/02 15:04:05 "), runningJobIDs)
		close(jobsStopped) // signal back to our parent that it's safe to move on, the jobs have stopped by closing our channel
	}
	if runningJobs && verbose {
		fmt.Printf("%sBackgroud jobs %s are currently running\n these jobs may cause the dump to fail by causing deadlocks, the main dump will wait for them to finish \n ", time.Now().Format("2006/01/02 15:04:05 "), runningJobIDs)
	}
	return runningJobs, err
}

func moveScheduledJobs(conn *pgx.Conn, movedJobs map[int64]time.Time, verbose bool, moveSQL string) (err error) {
	rows, err := conn.Query(context.Background(), moveSQL)
	defer rows.Close()
	if err != nil {
		return err
	}
	var (
		jobID     int64
		origStart time.Time
	)
	for rows.Next() {
		if err = rows.Scan(&jobID, &origStart); err != nil {
			return err
		}
		if _, ok := movedJobs[jobID]; !ok {
			if verbose {
				fmt.Printf("%sMoved Job: %d\n", time.Now().Format("2006/01/02 15:04:05 "), jobID)
			}
			movedJobs[jobID] = origStart
		}
	}
	return err
}

func rescheduleJobs(conn *pgx.Conn, movedJobs map[int64]time.Time, verbose bool, replaceJobSQL string) (err error) {
	var jobMoved bool
	for jobID, origStart := range movedJobs {
		if verbose {
			fmt.Printf("%sScheduling job %d to start again\n", time.Now().Format("2006/01/02 15:04:05 "), jobID)
		}
		err = conn.QueryRow(context.Background(), replaceJobSQL, jobID, origStart).Scan(&jobMoved)
		if err != nil && err != pgx.ErrNoRows {
			return err
		}
	}
	return err
}

func jobMoverWarn(err error) bool {
	if err != nil {
		fmt.Println(time.Now().Format("2006/01/02 15:04:05 ")+"WARNING: ", fmt.Errorf("problem while rescheduling jobs: %w", err))
		return true
	}
	return false
}

func jobMoverSQL(tsMajorVersion int, pauseUDAs bool) (runningJobsSQL string, moveSQL string, replaceJobSQL string, err error) {
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
		// are the jobs currently running. Unfortunately, there's no other indication of this, and it's
		// hard to know if it's a real indication or if it might lie to us sometimes.
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
