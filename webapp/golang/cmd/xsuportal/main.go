package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/sessions"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.opencensus.io/trace"
	"google.golang.org/protobuf/types/known/timestamppb"

	xsuportal "github.com/isucon/isucon10-final/webapp/golang"
	xsuportalpb "github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal"
	resourcespb "github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/resources"
	adminpb "github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/services/admin"
	audiencepb "github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/services/audience"
	commonpb "github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/services/common"
	contestantpb "github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/services/contestant"
	registrationpb "github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/services/registration"
	"github.com/isucon/isucon10-final/webapp/golang/util"
)

const (
	TeamCapacity               = 10
	AdminID                    = "admin"
	AdminPassword              = "admin"
	DebugContestStatusFilePath = "/tmp/XSUPORTAL_CONTEST_STATUS"
	MYSQL_ER_DUP_ENTRY         = 1062
	SessionName                = "xsucon_session"
)

var db *sqlx.DB
var notifier xsuportal.Notifier

func CleanContext(ctx context.Context) context.Context {
	return trace.NewContext(context.Background(), trace.FromContext(ctx))
}

func main() {
	xsuportal.InitProfiler("web")
	xsuportal.InitTrace()

	srv := echo.New()
	srv.Debug = util.GetEnv("DEBUG", "") != ""
	srv.Server.Addr = fmt.Sprintf(":%v", util.GetEnv("PORT", "9292"))
	srv.HideBanner = true

	srv.Binder = ProtoBinder{}
	srv.HTTPErrorHandler = func(err error, c echo.Context) {
		if !c.Response().Committed {
			c.Logger().Error(c.Request().Method, " ", c.Request().URL.Path, " ", err)
			_ = halt(c, http.StatusInternalServerError, "", err)
		}
	}

	db, _ = xsuportal.GetDB()

	srv.Use(echo.WrapMiddleware(xsuportal.WithTrace))
	srv.Use(middleware.Recover())
	srv.Use(session.Middleware(sessions.NewCookieStore([]byte("tagomoris"))))

	srv.File("/", "public/audience.html")
	srv.File("/registration", "public/audience.html")
	srv.File("/signup", "public/audience.html")
	srv.File("/login", "public/audience.html")
	srv.File("/logout", "public/audience.html")
	srv.File("/teams", "public/audience.html")

	srv.File("/contestant", "public/contestant.html")
	srv.File("/contestant/benchmark_jobs", "public/contestant.html")
	srv.File("/contestant/benchmark_jobs/:id", "public/contestant.html")
	srv.File("/contestant/clarifications", "public/contestant.html")

	srv.File("/admin", "public/admin.html")
	srv.File("/admin/", "public/admin.html")
	srv.File("/admin/clarifications", "public/admin.html")
	srv.File("/admin/clarifications/:id", "public/admin.html")

	srv.Static("/", "public")

	admin := &AdminService{}
	audience := &AudienceService{}
	registration := &RegistrationService{}
	contestant := &ContestantService{}
	common := &CommonService{}

	srv.POST("/initialize", admin.Initialize)
	srv.GET("/api/admin/clarifications", admin.ListClarifications)
	srv.GET("/api/admin/clarifications/:id", admin.GetClarification)
	srv.PUT("/api/admin/clarifications/:id", admin.RespondClarification)
	srv.GET("/api/session", common.GetCurrentSession)
	srv.GET("/api/audience/teams", audience.ListTeams)
	srv.GET("/api/audience/dashboard", audience.Dashboard)
	srv.GET("/api/registration/session", registration.GetRegistrationSession)
	srv.POST("/api/registration/team", registration.CreateTeam)
	srv.POST("/api/registration/contestant", registration.JoinTeam)
	srv.PUT("/api/registration", registration.UpdateRegistration)
	srv.DELETE("/api/registration", registration.DeleteRegistration)
	srv.POST("/api/contestant/benchmark_jobs", contestant.EnqueueBenchmarkJob)
	srv.GET("/api/contestant/benchmark_jobs", contestant.ListBenchmarkJobs)
	srv.GET("/api/contestant/benchmark_jobs/:id", contestant.GetBenchmarkJob)
	srv.GET("/api/contestant/clarifications", contestant.ListClarifications)
	srv.POST("/api/contestant/clarifications", contestant.RequestClarification)
	srv.GET("/api/contestant/dashboard", contestant.Dashboard)
	srv.GET("/api/contestant/notifications", contestant.ListNotifications)
	srv.POST("/api/contestant/push_subscriptions", contestant.SubscribeNotification)
	srv.DELETE("/api/contestant/push_subscriptions", contestant.UnsubscribeNotification)
	srv.POST("/api/signup", contestant.Signup)
	srv.POST("/api/login", contestant.Login)
	srv.POST("/api/logout", contestant.Logout)

	srv.Logger.Error(srv.StartServer(srv.Server))
}

type ProtoBinder struct{}

func (p ProtoBinder) Bind(i interface{}, e echo.Context) error {
	rc := e.Request().Body
	defer rc.Close()
	b, err := ioutil.ReadAll(rc)
	if err != nil {
		return halt(e, http.StatusBadRequest, "", fmt.Errorf("read request body: %w", err))
	}
	if err := proto.Unmarshal(b, i.(proto.Message)); err != nil {
		return halt(e, http.StatusBadRequest, "", fmt.Errorf("unmarshal request body: %w", err))
	}
	return nil
}

type AdminService struct{}

func (*AdminService) Initialize(e echo.Context) error {
	var req adminpb.InitializeRequest
	if err := e.Bind(&req); err != nil {
		return err
	}

	queries := []string{
		"TRUNCATE `teams`",
		"TRUNCATE `contestants`",
		"TRUNCATE `benchmark_jobs`",
		"TRUNCATE `clarifications`",
		"TRUNCATE `notifications`",
		"TRUNCATE `push_subscriptions`",
		"TRUNCATE `contest_config`",
	}
	for _, query := range queries {
		_, err := db.ExecContext(CleanContext(e.Request().Context()), query)
		if err != nil {
			return fmt.Errorf("truncate table: %w", err)
		}
	}

	passwordHash := sha256.Sum256([]byte(AdminPassword))
	digest := hex.EncodeToString(passwordHash[:])
	_, err := db.ExecContext(CleanContext(e.Request().Context()), "INSERT `contestants` (`id`, `password`, `staff`, `created_at`) VALUES (?, ?, TRUE, NOW(6))", AdminID, digest)
	if err != nil {
		return fmt.Errorf("insert initial contestant: %w", err)
	}

	if req.Contest != nil {
		_, err := db.ExecContext(CleanContext(e.Request().Context()), "INSERT `contest_config` (`registration_open_at`, `contest_starts_at`, `contest_freezes_at`, `contest_ends_at`) VALUES (?, ?, ?, ?)",
			req.Contest.RegistrationOpenAt.AsTime().Round(time.Microsecond),
			req.Contest.ContestStartsAt.AsTime().Round(time.Microsecond),
			req.Contest.ContestFreezesAt.AsTime().Round(time.Microsecond),
			req.Contest.ContestEndsAt.AsTime().Round(time.Microsecond),
		)
		if err != nil {
			return fmt.Errorf("insert contest: %w", err)
		}
	} else {
		_, err := db.ExecContext(CleanContext(e.Request().Context()), "INSERT `contest_config` (`registration_open_at`, `contest_starts_at`, `contest_freezes_at`, `contest_ends_at`) VALUES (TIMESTAMPADD(SECOND, 0, NOW(6)), TIMESTAMPADD(SECOND, 5, NOW(6)), TIMESTAMPADD(SECOND, 40, NOW(6)), TIMESTAMPADD(SECOND, 50, NOW(6)))")
		if err != nil {
			return fmt.Errorf("insert contest: %w", err)
		}
	}

	host := util.GetEnv("BENCHMARK_SERVER_HOST", "localhost")
	port, _ := strconv.Atoi(util.GetEnv("BENCHMARK_SERVER_PORT", "50051"))
	res := &adminpb.InitializeResponse{
		Language: "go",
		BenchmarkServer: &adminpb.InitializeResponse_BenchmarkServer{
			Host: host,
			Port: int64(port),
		},
	}
	return writeProto(e, http.StatusOK, res)
}

func (*AdminService) ListClarifications(e echo.Context) error {
	if ok, err := loginRequired(e, db, &loginRequiredOption{}); !ok {
		return wrapError("check session", err)
	}
	contestant, _ := getCurrentContestant(e, db, false)
	if !contestant.Staff {
		return halt(e, http.StatusForbidden, "管理者権限が必要です", nil)
	}
	var clarifications []xsuportal.Clarification
	err := db.SelectContext(CleanContext(e.Request().Context()), &clarifications, "SELECT * FROM `clarifications` ORDER BY `updated_at` DESC")
	if err != sql.ErrNoRows && err != nil {
		return fmt.Errorf("query clarifications: %w", err)
	}
	res := &adminpb.ListClarificationsResponse{}
	if len(clarifications) == 0 {
		return writeProto(e, http.StatusOK, res)
	}
	var teamIDs []int64
	teamIDuniq := make(map[int64]struct{})
	for _, clarification := range clarifications {
		if _, ok := teamIDuniq[clarification.TeamID]; !ok {
			teamIDs = append(teamIDs, clarification.TeamID)
		}
	}
	var teams []xsuportal.Team
	query, args, err := sqlx.In("SELECT * FROM `teams` WHERE `id` IN (?)", teamIDs)
	if err != nil {
		return fmt.Errorf("creating get teams query: %w", err)
	}
	err = db.SelectContext(CleanContext(e.Request().Context()), &teams, query, args...)
	if err != nil {
		return fmt.Errorf("query teams: %w", err)
	}
	var contestants []xsuportal.Contestant
	query, args, err = sqlx.In("SELECT * FROM `contestants` WHERE `team_id` IN (?)", teamIDs)
	if err != nil {
		return fmt.Errorf("creating get teams query: %w", err)
	}
	err = db.SelectContext(CleanContext(e.Request().Context()), &contestants, query, args...)
	if err != nil {
		return fmt.Errorf("query teams: %w", err)
	}
	contestantByTeam := make(map[int64][]xsuportal.Contestant, len(teams))
	for _, c := range contestants {
		s := contestantByTeam[c.TeamID.Int64]
		s = append(s, c)
		contestantByTeam[c.TeamID.Int64] = s
	}
	for t, s := range contestantByTeam {
		sort.Slice(s, func(i, j int) bool {
			return s[i].CreatedAt.Before(s[j].CreatedAt)
		})
		contestantByTeam[t] = s
	}
	for _, clarification := range clarifications {
		for _, t := range teams {
			if t.ID == clarification.TeamID {
				c, err := makeClarificationPB(e.Request().Context(), db, &clarification, &t, contestantByTeam[t.ID])
				if err != nil {
					return fmt.Errorf("make clarification: %w", err)
				}
				res.Clarifications = append(res.Clarifications, c)
				break
			}
		}
	}
	return writeProto(e, http.StatusOK, res)
}

func (*AdminService) GetClarification(e echo.Context) error {
	if ok, err := loginRequired(e, db, &loginRequiredOption{}); !ok {
		return wrapError("check session", err)
	}
	id, err := strconv.Atoi(e.Param("id"))
	if err != nil {
		return fmt.Errorf("parse id: %w", err)
	}
	contestant, _ := getCurrentContestant(e, db, false)
	if !contestant.Staff {
		return halt(e, http.StatusForbidden, "管理者権限が必要です", nil)
	}
	var clarification xsuportal.Clarification
	err = db.GetContext(CleanContext(e.Request().Context()), &clarification,
		"SELECT * FROM `clarifications` WHERE `id` = ? LIMIT 1",
		id,
	)
	if err != nil {
		return fmt.Errorf("get clarification: %w", err)
	}
	var team xsuportal.Team
	err = db.GetContext(CleanContext(e.Request().Context()), &team,
		"SELECT * FROM `teams` WHERE id = ? LIMIT 1",
		clarification.TeamID,
	)
	if err != nil {
		return fmt.Errorf("get team: %w", err)
	}
	c, err := makeClarificationPB(e.Request().Context(), db, &clarification, &team, nil)
	if err != nil {
		return fmt.Errorf("make clarification: %w", err)
	}
	return writeProto(e, http.StatusOK, &adminpb.GetClarificationResponse{
		Clarification: c,
	})
}

func (*AdminService) RespondClarification(e echo.Context) error {
	if ok, err := loginRequired(e, db, &loginRequiredOption{}); !ok {
		return wrapError("check session", err)
	}
	id, err := strconv.Atoi(e.Param("id"))
	if err != nil {
		return fmt.Errorf("parse id: %w", err)
	}
	contestant, _ := getCurrentContestant(e, db, false)
	if !contestant.Staff {
		return halt(e, http.StatusForbidden, "管理者権限が必要です", nil)
	}
	var req adminpb.RespondClarificationRequest
	if err := e.Bind(&req); err != nil {
		return err
	}

	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var clarificationBefore xsuportal.Clarification
	err = tx.GetContext(CleanContext(e.Request().Context()), &clarificationBefore,
		"SELECT * FROM `clarifications` WHERE `id` = ? LIMIT 1 FOR UPDATE",
		id,
	)
	if err == sql.ErrNoRows {
		return halt(e, http.StatusNotFound, "質問が見つかりません", nil)
	}
	if err != nil {
		return fmt.Errorf("get clarification with lock: %w", err)
	}
	wasAnswered := clarificationBefore.AnsweredAt.Valid
	wasDisclosed := clarificationBefore.Disclosed

	_, err = tx.ExecContext(CleanContext(e.Request().Context()), "UPDATE `clarifications` SET `disclosed` = ?, `answer` = ?, `updated_at` = NOW(6), `answered_at` = NOW(6) WHERE `id` = ? LIMIT 1",
		req.Disclose,
		req.Answer,
		id,
	)
	if err != nil {
		return fmt.Errorf("update clarification: %w", err)
	}
	var clarification xsuportal.Clarification
	err = tx.GetContext(CleanContext(e.Request().Context()), &clarification,
		"SELECT * FROM `clarifications` WHERE `id` = ? LIMIT 1",
		id,
	)
	if err != nil {
		return fmt.Errorf("get clarification: %w", err)
	}
	var team xsuportal.Team
	err = tx.GetContext(CleanContext(e.Request().Context()), &team,
		"SELECT * FROM `teams` WHERE `id` = ? LIMIT 1",
		clarification.TeamID,
	)
	if err != nil {
		return fmt.Errorf("get team: %w", err)
	}
	c, err := makeClarificationPB(e.Request().Context(), tx, &clarification, &team, nil)
	if err != nil {
		return fmt.Errorf("make clarification: %w", err)
	}
	updated := wasAnswered && wasDisclosed == clarification.Disclosed
	if !updated {
		if clarification.Disclosed.Bool {
			_, err = tx.ExecContext(CleanContext(e.Request().Context()), "UPDATE `teams` SET `last_clar_id` = ? WHERE `last_clar_id` < ?",
				id,
				id,
			)
			if err != nil {
				return fmt.Errorf("update last_clar_id: %w", err)
			}
		} else {
			_, err = tx.ExecContext(CleanContext(e.Request().Context()), "UPDATE `teams` SET `last_clar_id` = ? WHERE `id` = ? AND `last_clar_id` < ?",
				id,
				team.ID,
				id,
			)
			if err != nil {
				return fmt.Errorf("update last_clar_id: %w", err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	if err := notifier.NotifyClarificationAnswered(db, &clarification, updated); err != nil {
		return fmt.Errorf("notify clarification answered: %w", err)
	}
	return writeProto(e, http.StatusOK, &adminpb.RespondClarificationResponse{
		Clarification: c,
	})
}

type CommonService struct{}

func (*CommonService) GetCurrentSession(e echo.Context) error {
	res := &commonpb.GetCurrentSessionResponse{}
	currentContestant, err := getCurrentContestant(e, db, false)
	if err != nil {
		return fmt.Errorf("get current contestant: %w", err)
	}
	if currentContestant != nil {
		res.Contestant = makeContestantPB(currentContestant)
	}
	currentTeam, err := getCurrentTeam(e, db, false)
	if err != nil {
		return fmt.Errorf("get current team: %w", err)
	}
	if currentTeam != nil {
		res.Team, err = makeTeamPB(e.Request().Context(), db, currentTeam, true, true)
		if err != nil {
			return fmt.Errorf("make team: %w", err)
		}
	}
	res.Contest, err = makeContestPB(e)
	if err != nil {
		return fmt.Errorf("make contest: %w", err)
	}
	vapidKey := notifier.VAPIDKey()
	if vapidKey != nil {
		res.PushVapidKey = vapidKey.VAPIDPublicKey
	}
	return writeProto(e, http.StatusOK, res)
}

type ContestantService struct{}

func (*ContestantService) EnqueueBenchmarkJob(e echo.Context) error {
	var req contestantpb.EnqueueBenchmarkJobRequest
	if err := e.Bind(&req); err != nil {
		return err
	}
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()
	if ok, err := loginRequired(e, tx, &loginRequiredOption{Team: true}); !ok {
		return wrapError("check session", err)
	}
	if ok, err := contestStatusRestricted(e, tx, resourcespb.Contest_STARTED, "競技時間外はベンチマークを実行できません"); !ok {
		return wrapError("check contest status", err)
	}
	team, _ := getCurrentTeam(e, tx, false)
	var jobCount int
	err = tx.GetContext(CleanContext(e.Request().Context()), &jobCount,
		"SELECT COUNT(*) AS `cnt` FROM `benchmark_jobs` WHERE `team_id` = ? AND `finished_at` IS NULL",
		team.ID,
	)
	if err != nil {
		return fmt.Errorf("count benchmark job: %w", err)
	}
	if jobCount > 0 {
		return halt(e, http.StatusForbidden, "既にベンチマークを実行中です", nil)
	}
	_, err = tx.ExecContext(CleanContext(e.Request().Context()), "INSERT INTO `benchmark_jobs` (`team_id`, `target_hostname`, `status`, `updated_at`, `created_at`) VALUES (?, ?, ?, NOW(6), NOW(6))",
		team.ID,
		req.TargetHostname,
		int(resourcespb.BenchmarkJob_PENDING),
	)
	if err != nil {
		return fmt.Errorf("enqueue benchmark job: %w", err)
	}
	var job xsuportal.BenchmarkJob
	err = tx.GetContext(CleanContext(e.Request().Context()), &job,
		"SELECT * FROM `benchmark_jobs` WHERE `id` = (SELECT LAST_INSERT_ID()) LIMIT 1",
	)
	if err != nil {
		return fmt.Errorf("get benchmark job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	j := makeBenchmarkJobPB(&job)
	return writeProto(e, http.StatusOK, &contestantpb.EnqueueBenchmarkJobResponse{
		Job: j,
	})
}

func (*ContestantService) ListBenchmarkJobs(e echo.Context) error {
	if ok, err := loginRequired(e, db, &loginRequiredOption{Team: true}); !ok {
		return wrapError("check session", err)
	}
	jobs, err := makeBenchmarkJobsPB(e, db, 0)
	if err != nil {
		return fmt.Errorf("make benchmark jobs: %w", err)
	}
	return writeProto(e, http.StatusOK, &contestantpb.ListBenchmarkJobsResponse{
		Jobs: jobs,
	})
}

func (*ContestantService) GetBenchmarkJob(e echo.Context) error {
	if ok, err := loginRequired(e, db, &loginRequiredOption{Team: true}); !ok {
		return wrapError("check session", err)
	}
	id, err := strconv.Atoi(e.Param("id"))
	if err != nil {
		return fmt.Errorf("parse id: %w", err)
	}
	team, _ := getCurrentTeam(e, db, false)
	var job xsuportal.BenchmarkJob
	err = db.GetContext(CleanContext(e.Request().Context()), &job,
		"SELECT * FROM `benchmark_jobs` WHERE `team_id` = ? AND `id` = ? LIMIT 1",
		team.ID,
		id,
	)
	if err == sql.ErrNoRows {
		return halt(e, http.StatusNotFound, "ベンチマークジョブが見つかりません", nil)
	}
	if err != nil {
		return fmt.Errorf("get benchmark job: %w", err)
	}
	return writeProto(e, http.StatusOK, &contestantpb.GetBenchmarkJobResponse{
		Job: makeBenchmarkJobPB(&job),
	})
}

func (*ContestantService) ListClarifications(e echo.Context) error {
	if ok, err := loginRequired(e, db, &loginRequiredOption{Team: true}); !ok {
		return wrapError("check session", err)
	}
	team, _ := getCurrentTeam(e, db, false)
	var clarifications []xsuportal.Clarification
	err := db.SelectContext(CleanContext(e.Request().Context()), &clarifications,
		"SELECT * FROM `clarifications` WHERE `team_id` = ? OR `disclosed` = TRUE ORDER BY `id` DESC",
		team.ID,
	)
	if err != sql.ErrNoRows && err != nil {
		return fmt.Errorf("select clarifications: %w", err)
	}
	res := &contestantpb.ListClarificationsResponse{}
	if len(clarifications) == 0 {
		return writeProto(e, http.StatusOK, res)
	}
	var teamIDs []int64
	teamIDuniq := make(map[int64]struct{})
	for _, clarification := range clarifications {
		if _, ok := teamIDuniq[clarification.TeamID]; !ok {
			teamIDs = append(teamIDs, clarification.TeamID)
		}
	}
	var teams []xsuportal.Team
	query, args, err := sqlx.In("SELECT * FROM `teams` WHERE `id` IN (?)", teamIDs)
	if err != nil {
		return fmt.Errorf("creating get teams query: %w", err)
	}
	err = db.SelectContext(CleanContext(e.Request().Context()), &teams, query, args...)
	if err != nil {
		return fmt.Errorf("query teams: %w", err)
	}
	var contestants []xsuportal.Contestant
	query, args, err = sqlx.In("SELECT * FROM `contestants` WHERE `team_id` IN (?)", teamIDs)
	if err != nil {
		return fmt.Errorf("creating get teams query: %w", err)
	}
	err = db.SelectContext(CleanContext(e.Request().Context()), &contestants, query, args...)
	if err != nil {
		return fmt.Errorf("query teams: %w", err)
	}
	contestantByTeam := make(map[int64][]xsuportal.Contestant, len(teams))
	for _, c := range contestants {
		s := contestantByTeam[c.TeamID.Int64]
		s = append(s, c)
		contestantByTeam[c.TeamID.Int64] = s
	}
	for t, s := range contestantByTeam {
		sort.Slice(s, func(i, j int) bool {
			return s[i].CreatedAt.Before(s[j].CreatedAt)
		})
		contestantByTeam[t] = s
	}
	for _, clarification := range clarifications {
		for _, t := range teams {
			if t.ID == clarification.TeamID {
				c, err := makeClarificationPB(e.Request().Context(), db, &clarification, &t, contestantByTeam[t.ID])
				if err != nil {
					return fmt.Errorf("make clarification: %w", err)
				}
				res.Clarifications = append(res.Clarifications, c)
				break
			}
		}
	}
	return writeProto(e, http.StatusOK, res)
}

func (*ContestantService) RequestClarification(e echo.Context) error {
	if ok, err := loginRequired(e, db, &loginRequiredOption{Team: true}); !ok {
		return wrapError("check session", err)
	}
	var req contestantpb.RequestClarificationRequest
	if err := e.Bind(&req); err != nil {
		return err
	}
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()
	team, _ := getCurrentTeam(e, tx, false)
	_, err = tx.ExecContext(CleanContext(e.Request().Context()), "INSERT INTO `clarifications` (`team_id`, `question`, `created_at`, `updated_at`) VALUES (?, ?, NOW(6), NOW(6))",
		team.ID,
		req.Question,
	)
	if err != nil {
		return fmt.Errorf("insert clarification: %w", err)
	}
	var clarification xsuportal.Clarification
	err = tx.GetContext(CleanContext(e.Request().Context()), &clarification, "SELECT * FROM `clarifications` WHERE `id` = LAST_INSERT_ID() LIMIT 1")
	if err != nil {
		return fmt.Errorf("get clarification: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	c, err := makeClarificationPB(e.Request().Context(), db, &clarification, team, nil)
	if err != nil {
		return fmt.Errorf("make clarification: %w", err)
	}
	return writeProto(e, http.StatusOK, &contestantpb.RequestClarificationResponse{
		Clarification: c,
	})
}

func (*ContestantService) Dashboard(e echo.Context) error {
	if ok, err := loginRequired(e, db, &loginRequiredOption{Team: true}); !ok {
		return wrapError("check session", err)
	}
	team, _ := getCurrentTeam(e, db, false)
	leaderboard, err := makeLeaderboardPB(e, team.ID)
	if err != nil {
		return fmt.Errorf("make leaderboard: %w", err)
	}
	return writeProto(e, http.StatusOK, &contestantpb.DashboardResponse{
		Leaderboard: leaderboard,
	})
}

func (*ContestantService) ListNotifications(e echo.Context) error {
	if ok, err := loginRequired(e, db, &loginRequiredOption{Team: true}); !ok {
		return wrapError("check session", err)
	}

	afterStr := e.QueryParam("after")

	_, span := trace.StartSpan(e.Request().Context(), "Tx.Begin")
	tx, err := db.Beginx()
	if err != nil {
		span.End()
		return fmt.Errorf("begin tx: %w", err)
	}
	span.End()
	defer tx.Rollback()
	contestant, _ := getCurrentContestant(e, tx, false)

	var notifications []*xsuportal.Notification
	if afterStr != "" {
		after, err := strconv.Atoi(afterStr)
		if err != nil {
			return fmt.Errorf("parse after: %w", err)
		}
		err = tx.SelectContext(CleanContext(e.Request().Context()), &notifications,
			"SELECT * FROM `notifications` WHERE `contestant_id` = ? AND `id` > ? ORDER BY `id`",
			contestant.ID,
			after,
		)
		if err != sql.ErrNoRows && err != nil {
			return fmt.Errorf("select notifications(after=%v): %w", after, err)
		}
	} else {
		err = tx.SelectContext(CleanContext(e.Request().Context()), &notifications,
			"SELECT * FROM `notifications` WHERE `contestant_id` = ? ORDER BY `id`",
			contestant.ID,
		)
		if err != sql.ErrNoRows && err != nil {
			return fmt.Errorf("select notifications: %w", err)
		}
	}
	_, err = tx.ExecContext(CleanContext(e.Request().Context()), "UPDATE `notifications` SET `read` = TRUE WHERE `contestant_id` = ? AND `read` = FALSE",
		contestant.ID,
	)
	if err != nil {
		return fmt.Errorf("update notifications: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	team, _ := getCurrentTeam(e, db, false)

	//var lastAnsweredClarificationID int64
	//err = db.GetContext(CleanContext(e.Request().Context()), &lastAnsweredClarificationID,
	//        "SELECT `id` FROM `clarifications` WHERE (`team_id` = ? OR `disclosed` = TRUE) AND `answered_at` IS NOT NULL ORDER BY `id` DESC LIMIT 1",
	//        team.ID,
	//)
	//if err != sql.ErrNoRows && err != nil {
	//        return fmt.Errorf("get last answered clarification: %w", err)
	//}
	ns, err := makeNotificationsPB(notifications)
	if err != nil {
		return fmt.Errorf("make notifications: %w", err)
	}
	return writeProto(e, http.StatusOK, &contestantpb.ListNotificationsResponse{
		Notifications:               ns,
		LastAnsweredClarificationId: team.LastClarID,
	})
}

func (*ContestantService) SubscribeNotification(e echo.Context) error {
	if ok, err := loginRequired(e, db, &loginRequiredOption{Team: true}); !ok {
		return wrapError("check session", err)
	}

	if notifier.VAPIDKey() == nil {
		return halt(e, http.StatusServiceUnavailable, "WebPush は未対応です", nil)
	}

	var req contestantpb.SubscribeNotificationRequest
	if err := e.Bind(&req); err != nil {
		return err
	}

	contestant, _ := getCurrentContestant(e, db, false)
	_, err := db.ExecContext(CleanContext(e.Request().Context()), "INSERT INTO `push_subscriptions` (`contestant_id`, `endpoint`, `p256dh`, `auth`, `created_at`, `updated_at`) VALUES (?, ?, ?, ?, NOW(6), NOW(6))",
		contestant.ID,
		req.Endpoint,
		req.P256Dh,
		req.Auth,
	)
	if err != nil {
		return fmt.Errorf("insert push_subscription: %w", err)
	}
	return writeProto(e, http.StatusOK, &contestantpb.SubscribeNotificationResponse{})
}

func (*ContestantService) UnsubscribeNotification(e echo.Context) error {
	if ok, err := loginRequired(e, db, &loginRequiredOption{Team: true}); !ok {
		return wrapError("check session", err)
	}

	if notifier.VAPIDKey() == nil {
		return halt(e, http.StatusServiceUnavailable, "WebPush は未対応です", nil)
	}

	var req contestantpb.UnsubscribeNotificationRequest
	if err := e.Bind(&req); err != nil {
		return err
	}

	contestant, _ := getCurrentContestant(e, db, false)
	_, err := db.ExecContext(CleanContext(e.Request().Context()), "DELETE FROM `push_subscriptions` WHERE `contestant_id` = ? AND `endpoint` = ? LIMIT 1",
		contestant.ID,
		req.Endpoint,
	)
	if err != nil {
		return fmt.Errorf("delete push_subscription: %w", err)
	}
	return writeProto(e, http.StatusOK, &contestantpb.UnsubscribeNotificationResponse{})
}

func (*ContestantService) Signup(e echo.Context) error {
	var req contestantpb.SignupRequest
	if err := e.Bind(&req); err != nil {
		return err
	}

	hash := sha256.Sum256([]byte(req.Password))
	_, err := db.ExecContext(CleanContext(e.Request().Context()), "INSERT INTO `contestants` (`id`, `password`, `staff`, `created_at`) VALUES (?, ?, FALSE, NOW(6))",
		req.ContestantId,
		hex.EncodeToString(hash[:]),
	)
	if mErr, ok := err.(*mysql.MySQLError); ok && mErr.Number == MYSQL_ER_DUP_ENTRY {
		return halt(e, http.StatusBadRequest, "IDが既に登録されています", nil)
	}
	if err != nil {
		return fmt.Errorf("insert contestant: %w", err)
	}
	sess, err := session.Get(SessionName, e)
	if err != nil {
		return fmt.Errorf("get session: %w", err)
	}
	sess.Options = &sessions.Options{
		Path:   "/",
		MaxAge: 3600,
	}
	sess.Values["contestant_id"] = req.ContestantId
	if err := sess.Save(e.Request(), e.Response()); err != nil {
		return fmt.Errorf("save session: %w", err)
	}
	return writeProto(e, http.StatusOK, &contestantpb.SignupResponse{})
}

func (*ContestantService) Login(e echo.Context) error {
	var req contestantpb.LoginRequest
	if err := e.Bind(&req); err != nil {
		return err
	}
	var password string
	err := db.GetContext(CleanContext(e.Request().Context()), &password,
		"SELECT `password` FROM `contestants` WHERE `id` = ? LIMIT 1",
		req.ContestantId,
	)
	if err != sql.ErrNoRows && err != nil {
		return fmt.Errorf("get contestant: %w", err)
	}
	passwordHash := sha256.Sum256([]byte(req.Password))
	digest := hex.EncodeToString(passwordHash[:])
	if err != sql.ErrNoRows && subtle.ConstantTimeCompare([]byte(digest), []byte(password)) == 1 {
		sess, err := session.Get(SessionName, e)
		if err != nil {
			return fmt.Errorf("get session: %w", err)
		}
		sess.Options = &sessions.Options{
			Path:   "/",
			MaxAge: 3600,
		}
		sess.Values["contestant_id"] = req.ContestantId
		if err := sess.Save(e.Request(), e.Response()); err != nil {
			return fmt.Errorf("save session: %w", err)
		}
	} else {
		return halt(e, http.StatusBadRequest, "ログインIDまたはパスワードが正しくありません", nil)
	}
	return writeProto(e, http.StatusOK, &contestantpb.LoginResponse{})
}

func (*ContestantService) Logout(e echo.Context) error {
	sess, err := session.Get(SessionName, e)
	if err != nil {
		return fmt.Errorf("get session: %w", err)
	}
	if _, ok := sess.Values["contestant_id"]; ok {
		delete(sess.Values, "contestant_id")
		sess.Options = &sessions.Options{
			Path:   "/",
			MaxAge: -1,
		}
		if err := sess.Save(e.Request(), e.Response()); err != nil {
			return fmt.Errorf("delete session: %w", err)
		}
	} else {
		return halt(e, http.StatusUnauthorized, "ログインしていません", nil)
	}
	return writeProto(e, http.StatusOK, &contestantpb.LogoutResponse{})
}

type RegistrationService struct{}

func (*RegistrationService) GetRegistrationSession(e echo.Context) error {
	var team *xsuportal.Team
	currentTeam, err := getCurrentTeam(e, db, false)
	if err != nil {
		return fmt.Errorf("get current team: %w", err)
	}
	team = currentTeam
	if team == nil {
		teamIDStr := e.QueryParam("team_id")
		inviteToken := e.QueryParam("invite_token")
		if teamIDStr != "" && inviteToken != "" {
			teamID, err := strconv.Atoi(teamIDStr)
			if err != nil {
				return fmt.Errorf("parse team id: %w", err)
			}
			var t xsuportal.Team
			err = db.GetContext(CleanContext(e.Request().Context()), &t,
				"SELECT * FROM `teams` WHERE `id` = ? AND `invite_token` = ? AND `withdrawn` = FALSE LIMIT 1",
				teamID,
				inviteToken,
			)
			if err == sql.ErrNoRows {
				return halt(e, http.StatusNotFound, "招待URLが無効です", nil)
			}
			if err != nil {
				return fmt.Errorf("get team: %w", err)
			}
			team = &t
		}
	}

	var members []xsuportal.Contestant
	if team != nil {
		err := db.SelectContext(CleanContext(e.Request().Context()), &members,
			"SELECT * FROM `contestants` WHERE `team_id` = ?",
			team.ID,
		)
		if err != nil {
			return fmt.Errorf("select members: %w", err)
		}
	}

	res := &registrationpb.GetRegistrationSessionResponse{
		Status: 0,
	}
	contestant, err := getCurrentContestant(e, db, false)
	if err != nil {
		return fmt.Errorf("get current contestant: %w", err)
	}
	switch {
	case contestant != nil && contestant.TeamID.Valid:
		res.Status = registrationpb.GetRegistrationSessionResponse_JOINED
	case team != nil && len(members) >= 3:
		res.Status = registrationpb.GetRegistrationSessionResponse_NOT_JOINABLE
	case contestant == nil:
		res.Status = registrationpb.GetRegistrationSessionResponse_NOT_LOGGED_IN
	case team != nil:
		res.Status = registrationpb.GetRegistrationSessionResponse_JOINABLE
	case team == nil:
		res.Status = registrationpb.GetRegistrationSessionResponse_CREATABLE
	default:
		return fmt.Errorf("undeterminable status")
	}
	if team != nil {
		res.Team, err = makeTeamPB(e.Request().Context(), db, team, contestant != nil && currentTeam != nil && contestant.ID == currentTeam.LeaderID.String, true)
		if err != nil {
			return fmt.Errorf("make team: %w", err)
		}
		res.MemberInviteUrl = fmt.Sprintf("/registration?team_id=%v&invite_token=%v", team.ID, team.InviteToken)
		res.InviteToken = team.InviteToken
	}
	return writeProto(e, http.StatusOK, res)
}

func (*RegistrationService) CreateTeam(e echo.Context) error {
	var req registrationpb.CreateTeamRequest
	if err := e.Bind(&req); err != nil {
		return err
	}
	if ok, err := loginRequired(e, db, &loginRequiredOption{}); !ok {
		return wrapError("check session", err)
	}
	ok, err := contestStatusRestricted(e, db, resourcespb.Contest_REGISTRATION, "チーム登録期間ではありません")
	if !ok {
		return wrapError("check contest status", err)
	}

	ctx := context.Background()
	conn, err := db.Connx(ctx)
	if err != nil {
		return fmt.Errorf("get conn: %w", err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "LOCK TABLES `teams` WRITE, `contestants` WRITE")
	if err != nil {
		return fmt.Errorf("lock tables: %w", err)
	}
	defer conn.ExecContext(ctx, "UNLOCK TABLES")

	randomBytes := make([]byte, 64)
	_, err = rand.Read(randomBytes)
	if err != nil {
		return fmt.Errorf("read random: %w", err)
	}
	inviteToken := base64.URLEncoding.EncodeToString(randomBytes)
	var withinCapacity bool
	err = conn.QueryRowContext(
		ctx,
		"SELECT COUNT(*) < ? AS `within_capacity` FROM `teams`",
		TeamCapacity,
	).Scan(&withinCapacity)
	if err != nil {
		return fmt.Errorf("check capacity: %w", err)
	}
	if !withinCapacity {
		return halt(e, http.StatusForbidden, "チーム登録数上限です", nil)
	}
	_, err = conn.ExecContext(
		ctx,
		"INSERT INTO `teams` (`name`, `email_address`, `invite_token`, `created_at`) VALUES (?, ?, ?, NOW(6))",
		req.TeamName,
		req.EmailAddress,
		inviteToken,
	)
	if err != nil {
		return fmt.Errorf("insert team: %w", err)
	}
	var teamID int64
	err = conn.QueryRowContext(
		ctx,
		"SELECT LAST_INSERT_ID() AS `id`",
	).Scan(&teamID)
	if err != nil || teamID == 0 {
		return halt(e, http.StatusInternalServerError, "チームを登録できませんでした", nil)
	}

	contestant, _ := getCurrentContestant(e, db, false)

	_, err = conn.ExecContext(
		ctx,
		"UPDATE `contestants` SET `name` = ?, `student` = ?, `team_id` = ? WHERE id = ? LIMIT 1",
		req.Name,
		req.IsStudent,
		teamID,
		contestant.ID,
	)
	if err != nil {
		return fmt.Errorf("update contestant: %w", err)
	}

	_, err = conn.ExecContext(
		ctx,
		"UPDATE `teams` SET `leader_id` = ?, `student` = ? WHERE `id` = ? LIMIT 1",
		contestant.ID,
		req.IsStudent,
		teamID,
	)
	if err != nil {
		return fmt.Errorf("update team: %w", err)
	}

	return writeProto(e, http.StatusOK, &registrationpb.CreateTeamResponse{
		TeamId: teamID,
	})
}

func (*RegistrationService) JoinTeam(e echo.Context) error {
	var req registrationpb.JoinTeamRequest
	if err := e.Bind(&req); err != nil {
		return err
	}
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if ok, err := loginRequired(e, tx, &loginRequiredOption{Lock: true}); !ok {
		return wrapError("check session", err)
	}
	if ok, err := contestStatusRestricted(e, tx, resourcespb.Contest_REGISTRATION, "チーム登録期間ではありません"); !ok {
		return wrapError("check contest status", err)
	}
	var team xsuportal.Team
	err = tx.GetContext(CleanContext(e.Request().Context()), &team,
		"SELECT * FROM `teams` WHERE `id` = ? AND `invite_token` = ? AND `withdrawn` = FALSE LIMIT 1 FOR UPDATE",
		req.TeamId,
		req.InviteToken,
	)
	if err == sql.ErrNoRows {
		return halt(e, http.StatusBadRequest, "招待URLが不正です", nil)
	}
	if err != nil {
		return fmt.Errorf("get team with lock: %w", err)
	}
	var members []xsuportal.Contestant
	err = tx.SelectContext(CleanContext(e.Request().Context()), &members,
		"SELECT * FROM `contestants` WHERE `team_id` = ?",
		req.TeamId,
	)
	if err != nil {
		return fmt.Errorf("count team member: %w", err)
	}
	if len(members) >= 3 {
		return halt(e, http.StatusBadRequest, "チーム人数の上限に達しています", nil)
	}

	contestant, _ := getCurrentContestant(e, tx, false)
	_, err = tx.ExecContext(CleanContext(e.Request().Context()), "UPDATE `contestants` SET `team_id` = ?, `name` = ?, `student` = ? WHERE `id` = ? LIMIT 1",
		req.TeamId,
		req.Name,
		req.IsStudent,
		contestant.ID,
	)
	if err != nil {
		return fmt.Errorf("update contestant: %w", err)
	}
	isStudent := req.IsStudent
	for _, c := range members {
		if !c.Student {
			isStudent = false
		}
	}
	_, err = tx.ExecContext(CleanContext(e.Request().Context()), "UPDATE `teams` SET `student` = ? WHERE `id` = ? LIMIT 1",
		isStudent,
		req.TeamId,
	)
	if err != nil {
		return fmt.Errorf("update contestant: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return writeProto(e, http.StatusOK, &registrationpb.JoinTeamResponse{})
}

func (*RegistrationService) UpdateRegistration(e echo.Context) error {
	var req registrationpb.UpdateRegistrationRequest
	if err := e.Bind(&req); err != nil {
		return err
	}
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()
	if ok, err := loginRequired(e, tx, &loginRequiredOption{Team: true, Lock: true}); !ok {
		return wrapError("check session", err)
	}
	team, _ := getCurrentTeam(e, tx, false)
	contestant, _ := getCurrentContestant(e, tx, false)
	if team.LeaderID.Valid && team.LeaderID.String == contestant.ID {
		_, err := tx.ExecContext(CleanContext(e.Request().Context()), "UPDATE `teams` SET `name` = ?, `email_address` = ? WHERE `id` = ? LIMIT 1",
			req.TeamName,
			req.EmailAddress,
			team.ID,
		)
		if err != nil {
			return fmt.Errorf("update team: %w", err)
		}
	}
	_, err = tx.ExecContext(CleanContext(e.Request().Context()), "UPDATE `contestants` SET `name` = ?, `student` = ? WHERE `id` = ? LIMIT 1",
		req.Name,
		req.IsStudent,
		contestant.ID,
	)
	if err != nil {
		return fmt.Errorf("update contestant: %w", err)
	}
	if team.LeaderID.Valid {
		var members []xsuportal.Contestant
		err = tx.SelectContext(CleanContext(e.Request().Context()), &members,
			"SELECT * FROM `contestants` WHERE `team_id` = ?",
			contestant.TeamID,
		)
		if err != nil {
			return fmt.Errorf("count team member: %w", err)
		}
		isStudent := req.IsStudent
		for _, c := range members {
			if !c.Student {
				isStudent = false
			}
		}
		_, err = tx.ExecContext(CleanContext(e.Request().Context()), "UPDATE `teams` SET `student` = ? WHERE `id` = ? LIMIT 1",
			isStudent,
			contestant.TeamID,
		)
		if err != nil {
			return fmt.Errorf("update contestant: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return writeProto(e, http.StatusOK, &registrationpb.UpdateRegistrationResponse{})
}

func (*RegistrationService) DeleteRegistration(e echo.Context) error {
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()
	if ok, err := loginRequired(e, tx, &loginRequiredOption{Team: true, Lock: true}); !ok {
		return wrapError("check session", err)
	}
	if ok, err := contestStatusRestricted(e, tx, resourcespb.Contest_REGISTRATION, "チーム登録期間外は辞退できません"); !ok {
		return wrapError("check contest status", err)
	}
	team, _ := getCurrentTeam(e, tx, false)
	contestant, _ := getCurrentContestant(e, tx, false)
	if team.LeaderID.Valid && team.LeaderID.String == contestant.ID {
		_, err := tx.ExecContext(CleanContext(e.Request().Context()), "UPDATE `teams` SET `withdrawn` = TRUE, `leader_id` = NULL WHERE `id` = ? LIMIT 1",
			team.ID,
		)
		if err != nil {
			return fmt.Errorf("withdrawn team(id=%v): %w", team.ID, err)
		}
		_, err = tx.ExecContext(CleanContext(e.Request().Context()), "UPDATE `contestants` SET `team_id` = NULL WHERE `team_id` = ?",
			team.ID,
		)
		if err != nil {
			return fmt.Errorf("withdrawn members(team_id=%v): %w", team.ID, err)
		}
	} else {
		_, err := tx.ExecContext(CleanContext(e.Request().Context()), "UPDATE `contestants` SET `team_id` = NULL WHERE `id` = ? LIMIT 1",
			contestant.ID,
		)
		if err != nil {
			return fmt.Errorf("withdrawn contestant(id=%v): %w", contestant.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return writeProto(e, http.StatusOK, &registrationpb.DeleteRegistrationResponse{})
}

type AudienceService struct{}

func (*AudienceService) ListTeams(e echo.Context) error {
	var teams []xsuportal.Team
	err := db.SelectContext(CleanContext(e.Request().Context()), &teams, "SELECT * FROM `teams` WHERE `withdrawn` = FALSE ORDER BY `created_at` DESC")
	if err != nil {
		return fmt.Errorf("select teams: %w", err)
	}
	res := &audiencepb.ListTeamsResponse{}
	for _, team := range teams {
		var members []xsuportal.Contestant
		err := db.SelectContext(CleanContext(e.Request().Context()), &members,
			"SELECT * FROM `contestants` WHERE `team_id` = ? ORDER BY `created_at`",
			team.ID,
		)
		if err != nil {
			return fmt.Errorf("select members(team_id=%v): %w", team.ID, err)
		}
		var memberNames []string
		isStudent := true
		for _, member := range members {
			memberNames = append(memberNames, member.Name.String)
			isStudent = isStudent && member.Student
		}
		res.Teams = append(res.Teams, &audiencepb.ListTeamsResponse_TeamListItem{
			TeamId:      team.ID,
			Name:        team.Name,
			MemberNames: memberNames,
			IsStudent:   isStudent,
		})
	}
	return writeProto(e, http.StatusOK, res)
}

func (*AudienceService) Dashboard(e echo.Context) error {
	leaderboard, err := getCachedLeaderboard(e)
	if err != nil {
		return fmt.Errorf("make leaderboard: %w", err)
	}
	return e.Blob(http.StatusOK, "application/vnd.google.protobuf", leaderboard)
}

type XsuportalContext struct {
	Contestant *xsuportal.Contestant
	Team       *xsuportal.Team
}

func getXsuportalContext(e echo.Context) *XsuportalContext {
	xc := e.Get("xsucon_context")
	if xc == nil {
		xc = &XsuportalContext{}
		e.Set("xsucon_context", xc)
	}
	return xc.(*XsuportalContext)
}

func getCurrentContestant(e echo.Context, db sqlx.QueryerContext, lock bool) (*xsuportal.Contestant, error) {
	xc := getXsuportalContext(e)
	if xc.Contestant != nil {
		return xc.Contestant, nil
	}
	sess, err := session.Get(SessionName, e)
	if err != nil {
		return nil, fmt.Errorf("get session: %w", err)
	}
	contestantID, ok := sess.Values["contestant_id"]
	if !ok {
		return nil, nil
	}
	var contestant xsuportal.Contestant
	query := "SELECT * FROM `contestants` WHERE `id` = ? LIMIT 1"
	if lock {
		query += " FOR UPDATE"
	}
	err = sqlx.GetContext(CleanContext(e.Request().Context()), db, &contestant, query, contestantID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query contestant: %w", err)
	}
	xc.Contestant = &contestant
	return xc.Contestant, nil
}

func getCurrentTeam(e echo.Context, db sqlx.QueryerContext, lock bool) (*xsuportal.Team, error) {
	xc := getXsuportalContext(e)
	if xc.Team != nil {
		return xc.Team, nil
	}
	contestant, err := getCurrentContestant(e, db, false)
	if err != nil {
		return nil, fmt.Errorf("current contestant: %w", err)
	}
	if contestant == nil {
		return nil, nil
	}
	var team xsuportal.Team
	query := "SELECT * FROM `teams` WHERE `id` = ? LIMIT 1"
	if lock {
		query += " FOR UPDATE"
	}
	err = sqlx.GetContext(CleanContext(e.Request().Context()), db, &team, query, contestant.TeamID.Int64)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query team: %w", err)
	}
	xc.Team = &team
	return xc.Team, nil
}

func getCurrentContestStatus(e echo.Context, db sqlx.QueryerContext) (*xsuportal.ContestStatus, error) {
	var contestStatus xsuportal.ContestStatus
	err := sqlx.GetContext(CleanContext(e.Request().Context()), db, &contestStatus, "SELECT *, NOW(6) AS `current_time`, CASE WHEN NOW(6) < `registration_open_at` THEN 'standby' WHEN `registration_open_at` <= NOW(6) AND NOW(6) < `contest_starts_at` THEN 'registration' WHEN `contest_starts_at` <= NOW(6) AND NOW(6) < `contest_ends_at` THEN 'started' WHEN `contest_ends_at` <= NOW(6) THEN 'finished' ELSE 'unknown' END AS `status`, IF(`contest_starts_at` <= NOW(6) AND NOW(6) < `contest_freezes_at`, 1, 0) AS `frozen` FROM `contest_config`")
	if err != nil {
		return nil, fmt.Errorf("query contest status: %w", err)
	}
	statusStr := contestStatus.StatusStr
	if e.Echo().Debug {
		b, err := ioutil.ReadFile(DebugContestStatusFilePath)
		if err == nil {
			statusStr = string(b)
		}
	}
	switch statusStr {
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

type loginRequiredOption struct {
	Team bool
	Lock bool
}

func loginRequired(e echo.Context, db sqlx.QueryerContext, option *loginRequiredOption) (bool, error) {
	contestant, err := getCurrentContestant(e, db, option.Lock)
	if err != nil {
		return false, fmt.Errorf("current contestant: %w", err)
	}
	if contestant == nil {
		return false, halt(e, http.StatusUnauthorized, "ログインが必要です", nil)
	}
	if option.Team {
		t, err := getCurrentTeam(e, db, option.Lock)
		if err != nil {
			return false, fmt.Errorf("current team: %w", err)
		}
		if t == nil {
			return false, halt(e, http.StatusForbidden, "参加登録が必要です", nil)
		}
	}
	return true, nil
}

func contestStatusRestricted(e echo.Context, db sqlx.QueryerContext, status resourcespb.Contest_Status, message string) (bool, error) {
	contestStatus, err := getCurrentContestStatus(e, db)
	if err != nil {
		return false, fmt.Errorf("get current contest status: %w", err)
	}
	if contestStatus.Status != status {
		return false, halt(e, http.StatusForbidden, message, nil)
	}
	return true, nil
}

func writeProto(e echo.Context, code int, m proto.Message) error {
	res, _ := proto.Marshal(m)
	return e.Blob(code, "application/vnd.google.protobuf", res)
}

func halt(e echo.Context, code int, humanMessage string, err error) error {
	message := &xsuportalpb.Error{
		Code: int32(code),
	}
	if err != nil {
		message.Name = fmt.Sprintf("%T", err)
		message.HumanMessage = err.Error()
		message.HumanDescriptions = strings.Split(fmt.Sprintf("%+v", err), "\n")
	}
	if humanMessage != "" {
		message.HumanMessage = humanMessage
		message.HumanDescriptions = []string{humanMessage}
	}
	res, _ := proto.Marshal(message)
	return e.Blob(code, "application/vnd.google.protobuf; proto=xsuportal.proto.Error", res)
}

func makeClarificationPB(ctx context.Context, db sqlx.QueryerContext, c *xsuportal.Clarification, t *xsuportal.Team, cs []xsuportal.Contestant) (*resourcespb.Clarification, error) {
	team, err := makeTeamPB(ctx, db, t, false, cs == nil)
	if err != nil {
		return nil, fmt.Errorf("make team: %w", err)
	}
	if cs != nil {
		for _, m := range cs {
			m := m
			if m.ID == team.LeaderId {
				team.Leader = makeContestantPB(&m)
			}
			team.Members = append(team.Members, makeContestantPB(&m))
			team.MemberIds = append(team.MemberIds, m.ID)
		}
	}
	pb := &resourcespb.Clarification{
		Id:        c.ID,
		TeamId:    c.TeamID,
		Answered:  c.AnsweredAt.Valid,
		Disclosed: c.Disclosed.Bool,
		Question:  c.Question.String,
		Answer:    c.Answer.String,
		CreatedAt: timestamppb.New(c.CreatedAt),
		Team:      team,
	}
	if c.AnsweredAt.Valid {
		pb.AnsweredAt = timestamppb.New(c.AnsweredAt.Time)
	}
	return pb, nil
}

func makeTeamPB(ctx context.Context, db sqlx.QueryerContext, t *xsuportal.Team, detail bool, enableMembers bool) (*resourcespb.Team, error) {
	pb := &resourcespb.Team{
		Id:        t.ID,
		Name:      t.Name,
		LeaderId:  t.LeaderID.String,
		Withdrawn: t.Withdrawn,
	}
	if detail {
		pb.Detail = &resourcespb.Team_TeamDetail{
			EmailAddress: t.EmailAddress,
			InviteToken:  t.InviteToken,
		}
	}
	if enableMembers {
		if t.LeaderID.Valid {
			var leader xsuportal.Contestant
			if err := sqlx.GetContext(CleanContext(ctx), db, &leader, "SELECT * FROM `contestants` WHERE `id` = ? LIMIT 1", t.LeaderID.String); err != nil {
				return nil, fmt.Errorf("get leader: %w", err)
			}
			pb.Leader = makeContestantPB(&leader)
		}
		var members []xsuportal.Contestant
		if err := sqlx.SelectContext(CleanContext(ctx), db, &members, "SELECT * FROM `contestants` WHERE `team_id` = ? ORDER BY `created_at`", t.ID); err != nil {
			return nil, fmt.Errorf("select members: %w", err)
		}
		for _, member := range members {
			pb.Members = append(pb.Members, makeContestantPB(&member))
			pb.MemberIds = append(pb.MemberIds, member.ID)
		}
	}
	if t.Student.Valid {
		pb.Student = &resourcespb.Team_StudentStatus{
			Status: t.Student.Bool,
		}
	}
	return pb, nil
}

func makeContestantPB(c *xsuportal.Contestant) *resourcespb.Contestant {
	return &resourcespb.Contestant{
		Id:        c.ID,
		TeamId:    c.TeamID.Int64,
		Name:      c.Name.String,
		IsStudent: c.Student,
		IsStaff:   c.Staff,
	}
}

func makeContestPB(e echo.Context) (*resourcespb.Contest, error) {
	contestStatus, err := getCurrentContestStatus(e, db)
	if err != nil {
		return nil, fmt.Errorf("get current contest status: %w", err)
	}
	return &resourcespb.Contest{
		RegistrationOpenAt: timestamppb.New(contestStatus.RegistrationOpenAt),
		ContestStartsAt:    timestamppb.New(contestStatus.ContestStartsAt),
		ContestFreezesAt:   timestamppb.New(contestStatus.ContestFreezesAt),
		ContestEndsAt:      timestamppb.New(contestStatus.ContestEndsAt),
		Status:             contestStatus.Status,
		Frozen:             contestStatus.Frozen,
	}, nil
}

func makeBenchmarkJobPB(job *xsuportal.BenchmarkJob) *resourcespb.BenchmarkJob {
	pb := &resourcespb.BenchmarkJob{
		Id:             job.ID,
		TeamId:         job.TeamID,
		Status:         resourcespb.BenchmarkJob_Status(job.Status),
		TargetHostname: job.TargetHostName,
		CreatedAt:      timestamppb.New(job.CreatedAt),
		UpdatedAt:      timestamppb.New(job.UpdatedAt),
	}
	if job.StartedAt.Valid {
		pb.StartedAt = timestamppb.New(job.StartedAt.Time)
	}
	if job.FinishedAt.Valid {
		pb.FinishedAt = timestamppb.New(job.FinishedAt.Time)
		pb.Result = makeBenchmarkResultPB(job)
	}
	return pb
}

func makeBenchmarkResultPB(job *xsuportal.BenchmarkJob) *resourcespb.BenchmarkResult {
	hasScore := job.ScoreRaw.Valid && job.ScoreDeduction.Valid
	pb := &resourcespb.BenchmarkResult{
		Finished: job.FinishedAt.Valid,
		Passed:   job.Passed.Bool,
		Reason:   job.Reason.String,
	}
	if hasScore {
		pb.Score = int64(job.ScoreRaw.Int32 - job.ScoreDeduction.Int32)
		pb.ScoreBreakdown = &resourcespb.BenchmarkResult_ScoreBreakdown{
			Raw:       int64(job.ScoreRaw.Int32),
			Deduction: int64(job.ScoreDeduction.Int32),
		}
	}
	return pb
}

func makeBenchmarkJobsPB(e echo.Context, db sqlx.QueryerContext, limit int) ([]*resourcespb.BenchmarkJob, error) {
	team, _ := getCurrentTeam(e, db, false)
	query := "SELECT * FROM `benchmark_jobs` WHERE `team_id` = ? ORDER BY `created_at` DESC"
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	var jobs []xsuportal.BenchmarkJob
	if err := sqlx.SelectContext(CleanContext(e.Request().Context()), db, &jobs, query, team.ID); err != nil {
		return nil, fmt.Errorf("select benchmark jobs: %w", err)
	}
	var benchmarkJobs []*resourcespb.BenchmarkJob
	for _, job := range jobs {
		benchmarkJobs = append(benchmarkJobs, makeBenchmarkJobPB(&job))
	}
	return benchmarkJobs, nil
}

func makeNotificationsPB(notifications []*xsuportal.Notification) ([]*resourcespb.Notification, error) {
	var ns []*resourcespb.Notification
	for _, notification := range notifications {
		decoded, err := base64.StdEncoding.DecodeString(notification.EncodedMessage)
		if err != nil {
			return nil, fmt.Errorf("decode message: %w", err)
		}
		var message resourcespb.Notification
		if err := proto.Unmarshal(decoded, &message); err != nil {
			return nil, fmt.Errorf("unmarshal message: %w", err)
		}
		message.Id = notification.ID
		message.CreatedAt = timestamppb.New(notification.CreatedAt)
		ns = append(ns, &message)
	}
	return ns, nil
}

func wrapError(message string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}

func toTimestamp(t sql.NullTime) *timestamppb.Timestamp {
	if t.Valid {
		return timestamppb.New(t.Time)
	}
	return nil
}
