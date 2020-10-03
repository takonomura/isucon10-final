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
	lbCacheMutex.Lock()
	defer lbCacheMutex.Unlock()
	if !lbCacheExpire.IsZero() && lbCacheExpire.After(time.Now()) {
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
		"  `team_student_flags`.`student` AS `student`,\n" +
		"  (`best_score_jobs`.`score_raw` - `best_score_jobs`.`score_deduction`) AS `best_score`,\n" +
		"  `best_score_jobs`.`started_at` AS `best_score_started_at`,\n" +
		"  `best_score_jobs`.`finished_at` AS `best_score_marked_at`,\n" +
		"  (`latest_score_jobs`.`score_raw` - `latest_score_jobs`.`score_deduction`) AS `latest_score`,\n" +
		"  `latest_score_jobs`.`started_at` AS `latest_score_started_at`,\n" +
		"  `latest_score_jobs`.`finished_at` AS `latest_score_marked_at`,\n" +
		"  `latest_score_job_ids`.`finish_count` AS `finish_count`\n" +
		"FROM\n" +
		"  `teams`\n" +
		"  -- latest scores\n" +
		"  LEFT JOIN (\n" +
		"    SELECT\n" +
		"      MAX(`id`) AS `id`,\n" +
		"      `team_id`,\n" +
		"      COUNT(*) AS `finish_count`\n" +
		"    FROM\n" +
		"      `benchmark_jobs`\n" +
		"    WHERE\n" +
		"      `finished_at` IS NOT NULL\n" +
		"      -- score freeze\n" +
		"      AND (`team_id` = ? OR (`team_id` != ? AND (? = TRUE OR `finished_at` < ?)))\n" +
		"    GROUP BY\n" +
		"      `team_id`\n" +
		"  ) `latest_score_job_ids` ON `latest_score_job_ids`.`team_id` = `teams`.`id`\n" +
		"  LEFT JOIN `benchmark_jobs` `latest_score_jobs` ON `latest_score_job_ids`.`id` = `latest_score_jobs`.`id`\n" +
		"  -- best scores\n" +
		"  LEFT JOIN (\n" +
		"    SELECT\n" +
		"      MAX(`j`.`id`) AS `id`,\n" +
		"      `j`.`team_id` AS `team_id`\n" +
		"    FROM\n" +
		"      (\n" +
		"        SELECT\n" +
		"          `team_id`,\n" +
		"          MAX(`score_raw` - `score_deduction`) AS `score`\n" +
		"        FROM\n" +
		"          `benchmark_jobs`\n" +
		"        WHERE\n" +
		"          `finished_at` IS NOT NULL\n" +
		"          -- score freeze\n" +
		"          AND (`team_id` = ? OR (`team_id` != ? AND (? = TRUE OR `finished_at` < ?)))\n" +
		"        GROUP BY\n" +
		"          `team_id`\n" +
		"      ) `best_scores`\n" +
		"      LEFT JOIN `benchmark_jobs` `j` ON (`j`.`score_raw` - `j`.`score_deduction`) = `best_scores`.`score`\n" +
		"        AND `j`.`team_id` = `best_scores`.`team_id`\n" +
		"    GROUP BY\n" +
		"      `j`.`team_id`\n" +
		"  ) `best_score_job_ids` ON `best_score_job_ids`.`team_id` = `teams`.`id`\n" +
		"  LEFT JOIN `benchmark_jobs` `best_score_jobs` ON `best_score_jobs`.`id` = `best_score_job_ids`.`id`\n" +
		"  -- check student teams\n" +
		"  LEFT JOIN (\n" +
		"    SELECT\n" +
		"      `team_id`,\n" +
		"      (SUM(`student`) = COUNT(*)) AS `student`\n" +
		"    FROM\n" +
		"      `contestants`\n" +
		"    GROUP BY\n" +
		"      `contestants`.`team_id`\n" +
		"  ) `team_student_flags` ON `team_student_flags`.`team_id` = `teams`.`id`\n" +
		"ORDER BY\n" +
		"  `latest_score` DESC,\n" +
		"  `latest_score_marked_at` ASC\n"
	err = tx.SelectContext(CleanContext(e.Request().Context()), &leaderboard, query, teamID, teamID, contestFinished, contestFreezesAt, teamID, teamID, contestFinished, contestFreezesAt)
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
