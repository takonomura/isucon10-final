package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"google.golang.org/protobuf/types/known/timestamppb"

	xsuportal "github.com/isucon/isucon10-final/webapp/golang"
	resourcespb "github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/resources"
)

var (
	lbCacheMutex  = new(sync.Mutex)
	lbCacheExpire time.Time
	lbCacheData   *resourcespb.Leaderboard
)

func getCachedLeaderboard(e echo.Context) (*resourcespb.Leaderboard, error) {
	now := time.Now()
	lbCacheMutex.Lock()
	defer lbCacheMutex.Unlock()
	if !lbCacheExpire.IsZero() && lbCacheExpire.After(now) {
		return lbCacheData, nil
	}
	var err error
	lbCacheExpire = time.Now().Add(time.Second)
	lbCacheData, err = makeLeaderboardPB(e, 0)
	if err != nil {
		lbCacheExpire = time.Time{}
		return nil, err
	}
	return lbCacheData, nil
}

func makeLeaderboardPB(e echo.Context, teamID int64) (*resourcespb.Leaderboard, error) {
	contestStatus, err := getCurrentContestStatus(e, db)
	if err != nil {
		return nil, fmt.Errorf("get current contest status: %w", err)
	}
	contestFinished := contestStatus.Status == resourcespb.Contest_FINISHED
	contestFreezesAt := contestStatus.ContestFreezesAt

	tx, err := db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()
	var leaderboard []xsuportal.LeaderBoardTeam
	query := "SELECT\n" +
		"  `teams`.`id` AS `id`,\n" +
		"  `teams`.`name` AS `name`,\n" +
		"  `teams`.`leader_id` AS `leader_id`,\n" +
		"  `teams`.`withdrawn` AS `withdrawn`,\n" +
		"  `teams`.`student` AS `student`,\n" +
		"  `teams`.`best_score` AS `best_score`,\n" +
		"  `teams`.`best_score_started_at` AS `best_score_started_at`,\n" +
		"  `teams`.`best_score_marked_at` AS `best_score_marked_at`,\n" +
		"  `teams`.`latest_score` AS `latest_score`,\n" +
		"  `teams`.`latest_score_started_at` AS `latest_score_started_at`,\n" +
		"  `teams`.`latest_score_marked_at` AS `latest_score_marked_at`,\n" +
		"  `teams`.`finish_count` AS `finish_count`,\n" +
		"  `teams`.`real_best_score` AS `real_best_score`,\n" +
		"  `teams`.`real_best_score_started_at` AS `real_best_score_started_at`,\n" +
		"  `teams`.`real_best_score_marked_at` AS `real_best_score_marked_at`,\n" +
		"  `teams`.`real_latest_score` AS `real_latest_score`,\n" +
		"  `teams`.`real_latest_score_started_at` AS `real_latest_score_started_at`,\n" +
		"  `teams`.`real_latest_score_marked_at` AS `real_latest_score_marked_at`,\n" +
		"  `teams`.`real_finish_count` AS `real_finish_count`\n" +
		"FROM\n" +
		"  `teams`\n" +
		"ORDER BY\n" +
		"  `latest_score` DESC,\n" +
		"  `latest_score_marked_at` ASC\n"
	err = tx.SelectContext(CleanContext(e.Request().Context()), &leaderboard, query)
	if err != sql.ErrNoRows && err != nil {
		return nil, fmt.Errorf("select leaderboard: %w", err)
	}
	jobResultsQuery := "SELECT\n" +
		"  `team_id` AS `team_id`,\n" +
		"  (`score_raw` - `score_deduction`) AS `score`,\n" +
		"  `started_at` AS `started_at`,\n" +
		"  `finished_at` AS `finished_at`\n" +
		"FROM\n" +
		"  `benchmark_jobs`\n" +
		"WHERE\n" +
		"  `started_at` IS NOT NULL\n" +
		"  AND (\n" +
		"    `finished_at` IS NOT NULL\n" +
		"    -- score freeze\n" +
		"    AND (`team_id` = ? OR (`team_id` != ? AND (? = TRUE OR `finished_at` < ?)))\n" +
		"  )\n" +
		"ORDER BY\n" +
		"  `finished_at`"
	var jobResults []xsuportal.JobResult
	err = tx.SelectContext(CleanContext(e.Request().Context()), &jobResults, jobResultsQuery, teamID, teamID, contestFinished, contestFreezesAt)
	if err != sql.ErrNoRows && err != nil {
		return nil, fmt.Errorf("select job results: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}
	teamGraphScores := make(map[int64][]*resourcespb.Leaderboard_LeaderboardItem_LeaderboardScore)
	for _, jobResult := range jobResults {
		teamGraphScores[jobResult.TeamID] = append(teamGraphScores[jobResult.TeamID], &resourcespb.Leaderboard_LeaderboardItem_LeaderboardScore{
			Score:     jobResult.Score,
			StartedAt: timestamppb.New(jobResult.StartedAt),
			MarkedAt:  timestamppb.New(jobResult.FinishedAt),
		})
	}
	pb := &resourcespb.Leaderboard{}
	for _, team := range leaderboard {
		if team.ID == teamID || contestFinished {
			team.FinishCount = team.RealFinishCount
			team.BestScore = team.RealBestScore
			team.LatestScore = team.RealLatestScore
			//var finishCount int64
			//err = tx.GetContext(CleanContext(e.Request().Context()), &finishCount, "SELECT COUNT(1) AS `count` FROM `benchmark_jobs` WHERE `team_id` = ?", teamID)
			//if err != nil {
			//        return nil, fmt.Errorf("getting self finish count: %w", err)
			//}
			//team.FinishCount.Int64 = finishCount
			//var latest xsuportal.BenchmarkJob
			//err = tx.GetContext(CleanContext(e.Request().Context()), &latest, "SELECT * FROM `benchmark_jobs` WHERE `team_id` = ? ORDER BY `id` DESC LIMIT 1", teamID)
			//if err != nil {
			//        return nil, fmt.Errorf("getting latest job: %w", err)
			//}
			//if latest.ScoreRaw.Valid {
			//        team.LatestScore = sql.NullInt64{
			//                Valid: true,
			//                Int64: int64(latest.ScoreRaw.Int32 - latest.ScoreDeduction.Int32),
			//        }
			//}
			//var best xsuportal.BenchmarkJob
			//err = tx.GetContext(CleanContext(e.Request().Context()), &best, "SELECT * FROM `benchmark_jobs` WHERE `team_id` = ? ORDER BY `score_raw` - `score_deduction` DESC LIMIT 1", teamID)
			//if err != nil {
			//        return nil, fmt.Errorf("getting best job: %w", err)
			//}
			//if best.ScoreRaw.Valid {
			//        team.LatestScore = sql.NullInt64{
			//                Valid: true,
			//                Int64: int64(best.ScoreRaw.Int32 - best.ScoreDeduction.Int32),
			//        }
			//}
		}
		t, _ := makeTeamPB(e.Request().Context(), db, team.Team(), false, false)
		item := &resourcespb.Leaderboard_LeaderboardItem{
			Scores: teamGraphScores[team.ID],
			BestScore: &resourcespb.Leaderboard_LeaderboardItem_LeaderboardScore{
				Score:     team.BestScore.Int64,
				StartedAt: toTimestamp(team.BestScoreStartedAt),
				MarkedAt:  toTimestamp(team.BestScoreMarkedAt),
			},
			LatestScore: &resourcespb.Leaderboard_LeaderboardItem_LeaderboardScore{
				Score:     team.LatestScore.Int64,
				StartedAt: toTimestamp(team.LatestScoreStartedAt),
				MarkedAt:  toTimestamp(team.LatestScoreMarkedAt),
			},
			Team:        t,
			FinishCount: team.FinishCount.Int64,
		}
		if team.Student.Valid && team.Student.Bool {
			pb.StudentTeams = append(pb.StudentTeams, item)
		} else {
			pb.GeneralTeams = append(pb.GeneralTeams, item)
		}
		pb.Teams = append(pb.Teams, item)
	}
	return pb, nil
}
