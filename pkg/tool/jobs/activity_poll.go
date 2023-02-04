package jobs

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"github.com/doug-martin/goqu/v9"
	strava "github.com/strava/go.strava"
	"time"

	internalStrava "github.com/charlieegan3/tool-activities/internal/pkg/strava"
)

// ActivityPoll is a job that imports recent activities.
type ActivityPoll struct {
	DB *sql.DB

	ScheduleOverride string

	StravaClientID     string
	StravaClientSecret string
	StravaRefreshToken string
}

func (a *ActivityPoll) Name() string {
	return "activity-poll"
}

func (a *ActivityPoll) Run(ctx context.Context) error {
	doneCh := make(chan bool)
	errCh := make(chan error)

	go func() {
		accessToken, err := internalStrava.GetAccessToken(a.StravaClientID, a.StravaClientSecret, a.StravaRefreshToken)
		if err != nil {
			errCh <- fmt.Errorf("failed to get access token: %w", err)
			return
		}

		client := strava.NewClient(accessToken)

		svc := strava.NewCurrentAthleteService(client)
		activities, err := svc.ListActivities().Do()
		if err != nil {
			errCh <- fmt.Errorf("failed to list activities: %w", err)
			return
		}

		var rows []goqu.Record
		for _, activity := range activities {
			rows = append(rows, goqu.Record{"id": activity.Id, "source": "polling"})
		}

		goquDB := goqu.New("postgres", a.DB)
		query := goquDB.Insert("activities.activities").
			Rows(rows).
			OnConflict(goqu.DoNothing())
		res, err := query.Executor().ExecContext(ctx)
		if err != nil {
			errCh <- fmt.Errorf("failed to insert: %v", err)
			return
		}

		rowCount, err := res.RowsAffected()
		if err != nil {
			errCh <- fmt.Errorf("failed to get row count: %v", err)
			return
		}

		fmt.Println("New activities:", rowCount)

		doneCh <- true
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case e := <-errCh:
		return fmt.Errorf("job failed with error: %a", e)
	case <-doneCh:
		return nil
	}
}

func (a *ActivityPoll) Timeout() time.Duration {
	return 30 * time.Second
}

func (a *ActivityPoll) Schedule() string {
	if a.ScheduleOverride != "" {
		return a.ScheduleOverride
	}
	return "0 30 * * * *"
}
