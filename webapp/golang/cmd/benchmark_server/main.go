package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"go.opencensus.io/plugin/ocgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	xsuportal "github.com/isucon/isucon10-final/webapp/golang"
	"github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/resources"
	resourcespb "github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/resources"
	"github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/services/bench"
	"github.com/isucon/isucon10-final/webapp/golang/util"
)

var db *sqlx.DB

func CleanContext(ctx context.Context) context.Context {
	return context.Background()
}

type benchmarkQueueService struct {
}

func (b *benchmarkQueueService) Svc() *bench.BenchmarkQueueService {
	return &bench.BenchmarkQueueService{
		ReceiveBenchmarkJob: b.ReceiveBenchmarkJob,
	}
}

func (b *benchmarkQueueService) ReceiveBenchmarkJob(ctx context.Context, req *bench.ReceiveBenchmarkJobRequest) (*bench.ReceiveBenchmarkJobResponse, error) {
	var jobHandle *bench.ReceiveBenchmarkJobResponse_JobHandle
	for {
		next, err := func() (bool, error) {
			tx, err := db.Beginx()
			if err != nil {
				return false, fmt.Errorf("begin tx: %w", err)
			}
			defer tx.Rollback()

			job, err := pollBenchmarkJob(ctx, tx)
			if err != nil {
				return false, fmt.Errorf("poll benchmark job: %w", err)
			}
			if job == nil {
				return false, nil
			}

			randomBytes := make([]byte, 16)
			_, err = rand.Read(randomBytes)
			if err != nil {
				return false, fmt.Errorf("read random: %w", err)
			}
			handle := base64.StdEncoding.EncodeToString(randomBytes)
			res, err := tx.ExecContext(CleanContext(ctx), "UPDATE `benchmark_jobs` SET `status` = ?, `handle` = ? WHERE `id` = ? AND `status` = ? LIMIT 1",
				resources.BenchmarkJob_SENT,
				handle,
				job.ID,
				resources.BenchmarkJob_PENDING,
			)
			if err != nil {
				return false, fmt.Errorf("update benchmark job status: %w", err)
			}
			if i, err := res.RowsAffected(); i == 0 && err == nil {
				return true, nil
			}

			var contestStartsAt time.Time
			err = tx.GetContext(CleanContext(ctx), &contestStartsAt, "SELECT `contest_starts_at` FROM `contest_config` LIMIT 1")
			if err != nil {
				return false, fmt.Errorf("get contest starts at: %w", err)
			}

			if err := tx.Commit(); err != nil {
				return false, fmt.Errorf("commit tx: %w", err)
			}

			jobHandle = &bench.ReceiveBenchmarkJobResponse_JobHandle{
				JobId:            job.ID,
				Handle:           handle,
				TargetHostname:   job.TargetHostName,
				ContestStartedAt: timestamppb.New(contestStartsAt),
				JobCreatedAt:     timestamppb.New(job.CreatedAt),
			}
			return false, nil
		}()
		if err != nil {
			return nil, fmt.Errorf("fetch queue: %w", err)
		}
		if !next {
			break
		}
	}
	if jobHandle != nil {
		log.Printf("[DEBUG] Dequeued: job_handle=%+v", jobHandle)
	}
	return &bench.ReceiveBenchmarkJobResponse{
		JobHandle: jobHandle,
	}, nil
}

type benchmarkReportService struct {
}

func (b *benchmarkReportService) Svc() *bench.BenchmarkReportService {
	return &bench.BenchmarkReportService{
		ReportBenchmarkResult: b.ReportBenchmarkResult,
	}
}

func (b *benchmarkReportService) ReportBenchmarkResult(srv bench.BenchmarkReport_ReportBenchmarkResultServer) error {
	var notifier xsuportal.Notifier
	for {
		req, err := srv.Recv()
		if err != nil {
			return err
		}
		if req.Result == nil {
			return status.Error(codes.InvalidArgument, "result required")
		}

		err = func() error {
			tx, err := db.Beginx()
			if err != nil {
				return fmt.Errorf("begin tx: %w", err)
			}
			defer tx.Rollback()

			var job xsuportal.BenchmarkJob
			err = tx.GetContext(CleanContext(context.TODO()), &job,
				"SELECT * FROM `benchmark_jobs` WHERE `id` = ? AND `handle` = ? LIMIT 1 FOR UPDATE",
				req.JobId,
				req.Handle,
			)
			if err == sql.ErrNoRows {
				log.Printf("[ERROR] Job not found: job_id=%v, handle=%+v", req.JobId, req.Handle)
				return status.Errorf(codes.NotFound, "Job %d not found or handle is wrong", req.JobId)
			}
			if err != nil {
				return fmt.Errorf("get benchmark job: %w", err)
			}
			if req.Result.Finished {
				log.Printf("[DEBUG] %v: save as finished", req.JobId)
				if err := b.saveAsFinished(tx, &job, req); err != nil {
					return err
				}
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("commit tx: %w", err)
				}
				if err := notifier.NotifyBenchmarkJobFinished(db, &job); err != nil {
					return fmt.Errorf("notify benchmark job finished: %w", err)
				}
			} else {
				log.Printf("[DEBUG] %v: save as running", req.JobId)
				if err := b.saveAsRunning(tx, &job, req); err != nil {
					return err
				}
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("commit tx: %w", err)
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
		err = srv.Send(&bench.ReportBenchmarkResultResponse{
			AckedNonce: req.GetNonce(),
		})
		if err != nil {
			return fmt.Errorf("send report: %w", err)
		}
	}
}

func (b *benchmarkReportService) saveAsFinished(db *sqlx.Tx, job *xsuportal.BenchmarkJob, req *bench.ReportBenchmarkResultRequest) error {
	if !job.StartedAt.Valid || job.FinishedAt.Valid {
		return status.Errorf(codes.FailedPrecondition, "Job %v has already finished or has not started yet", req.JobId)
	}
	if req.Result.MarkedAt == nil {
		return status.Errorf(codes.InvalidArgument, "marked_at is required")
	}
	markedAt := req.Result.MarkedAt.AsTime().Round(time.Microsecond)

	result := req.Result
	var raw, deduction sql.NullInt32
	if result.ScoreBreakdown != nil {
		raw.Valid = true
		raw.Int32 = int32(result.ScoreBreakdown.Raw)
		deduction.Valid = true
		deduction.Int32 = int32(result.ScoreBreakdown.Deduction)
	}
	_, err := db.Exec(
		"UPDATE `benchmark_jobs` SET `status` = ?, `score_raw` = ?, `score_deduction` = ?, `passed` = ?, `reason` = ?, `updated_at` = NOW(6), `finished_at` = ? WHERE `id` = ? LIMIT 1",
		resources.BenchmarkJob_FINISHED,
		raw,
		deduction,
		result.Passed,
		result.Reason,
		markedAt,
		req.JobId,
	)
	if err != nil {
		return fmt.Errorf("update benchmark job status: %w", err)
	}
	cs, err := getCurrentContestStatus(context.TODO(), db)
	if err != nil {
		return fmt.Errorf("get contest status: %w", err)
	}
	//if cs.ContestFreezesAt.After(markedAt) {
	var team xsuportal.Team
	err = db.Get(&team, "SELECT * FROM `teams` WHERE id = ? LIMIT 1", job.TeamID)
	if err != nil {
		return fmt.Errorf("querying team: %w", err)
	}
	score := raw.Int32 - deduction.Int32
	sets := []string{
		"`real_final_count` = `final_count` + 1",
		"`real_latest_score` = ?",
		"`real_latest_started_at` = ?",
		"`real_latest_marked_at` = ?",
	}
	setArgs := []interface{}{
		score,
		job.StartedAt.Time,
		markedAt,
	}
	if cs.ContestFreezesAt.After(markedAt) {
		sets = append(sets,
			"`final_count` = `final_count` + 1",
			"`latest_score` = ?",
			"`latest_started_at` = ?",
			"`latest_marked_at` = ?",
		)
		setArgs = append(setArgs,
			score,
			job.StartedAt.Time,
			markedAt,
		)
	}

	if team.BestScore.Int64 <= int64(score) {
		sets = append(sets,
			"real_best_score = ?",
			"real_best_started_at = ?",
			"real_best_marked_at = ?",
		)
		setArgs = append(setArgs,
			score,
			job.StartedAt.Time,
			markedAt,
		)
		if cs.ContestFreezesAt.After(markedAt) {
			sets = append(sets,
				"best_score = ?",
				"best_started_at = ?",
				"best_marked_at = ?",
			)
			setArgs = append(setArgs,
				score,
				job.StartedAt.Time,
				markedAt,
			)
		}
	}
	setArgs = append(setArgs, team.ID)
	_, err = db.Exec("UPDATE `teams` SET "+strings.Join(sets, ", ")+" WHERE `id` = ?", setArgs...)
	if err != nil {
		return fmt.Errorf("updating team stats: %w", err)
	}
	//}

	return nil
}
func getCurrentContestStatus(ctx context.Context, db sqlx.QueryerContext) (*xsuportal.ContestStatus, error) {
	var contestStatus xsuportal.ContestStatus
	err := sqlx.GetContext(CleanContext(ctx), db, &contestStatus, "SELECT *, NOW(6) AS `current_time`, CASE WHEN NOW(6) < `registration_open_at` THEN 'standby' WHEN `registration_open_at` <= NOW(6) AND NOW(6) < `contest_starts_at` THEN 'registration' WHEN `contest_starts_at` <= NOW(6) AND NOW(6) < `contest_ends_at` THEN 'started' WHEN `contest_ends_at` <= NOW(6) THEN 'finished' ELSE 'unknown' END AS `status`, IF(`contest_starts_at` <= NOW(6) AND NOW(6) < `contest_freezes_at`, 1, 0) AS `frozen` FROM `contest_config`")
	if err != nil {
		return nil, fmt.Errorf("query contest status: %w", err)
	}
	switch contestStatus.StatusStr {
	case "standby":
		contestStatus.Status = resourcespb.Contest_STANDBY
	case "registration":
		contestStatus.Status = resourcespb.Contest_REGISTRATION
	case "started":
		contestStatus.Status = resourcespb.Contest_STARTED
	case "finished":
		contestStatus.Status = resourcespb.Contest_FINISHED
	default:
		return nil, fmt.Errorf("unexpected contest status: %q", contestStatus.StatusStr)
	}
	return &contestStatus, nil
}

func (b *benchmarkReportService) saveAsRunning(db sqlx.Execer, job *xsuportal.BenchmarkJob, req *bench.ReportBenchmarkResultRequest) error {
	if req.Result.MarkedAt == nil {
		return status.Errorf(codes.InvalidArgument, "marked_at is required")
	}
	var startedAt time.Time
	if job.StartedAt.Valid {
		startedAt = job.StartedAt.Time
	} else {
		startedAt = req.Result.MarkedAt.AsTime().Round(time.Microsecond)
	}
	_, err := db.Exec(
		"UPDATE `benchmark_jobs` SET `status` = ?, `score_raw` = NULL, `score_deduction` = NULL, `passed` = FALSE, `reason` = NULL, `started_at` = ?, `updated_at` = NOW(6), `finished_at` = NULL WHERE `id` = ? LIMIT 1",
		resources.BenchmarkJob_RUNNING,
		startedAt,
		req.JobId,
	)
	if err != nil {
		return fmt.Errorf("update benchmark job status: %w", err)
	}
	return nil
}

func pollBenchmarkJob(ctx context.Context, db sqlx.QueryerContext) (*xsuportal.BenchmarkJob, error) {
	for i := 0; i < 10; i++ {
		if i >= 1 {
			time.Sleep(25 * time.Millisecond)
		}
		var job xsuportal.BenchmarkJob
		err := sqlx.GetContext(CleanContext(ctx), db,
			&job,
			"SELECT * FROM `benchmark_jobs` WHERE `status` = ? ORDER BY `id` LIMIT 1",
			resources.BenchmarkJob_PENDING,
		)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get benchmark job: %w", err)
		}
		return &job, nil
	}
	return nil, nil
}

func main() {
	port := util.GetEnv("PORT", "50051")
	address := ":" + port

	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}
	log.Print("[INFO] listen ", address)

	db, _ = xsuportal.GetDB()
	db.SetMaxOpenConns(10)

	server := grpc.NewServer(grpc.StatsHandler(&ocgrpc.ServerHandler{}))

	queue := &benchmarkQueueService{}
	report := &benchmarkReportService{}

	bench.RegisterBenchmarkQueueService(server, queue.Svc())
	bench.RegisterBenchmarkReportService(server, report.Svc())

	if err := server.Serve(listener); err != nil {
		panic(err)
	}
}
