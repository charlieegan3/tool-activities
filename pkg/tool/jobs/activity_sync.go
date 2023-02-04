package jobs

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/doug-martin/goqu/v9"
	strava "github.com/strava/go.strava"
	"google.golang.org/api/option"

	internalStrava "github.com/charlieegan3/tool-activities/internal/pkg/strava"
	"github.com/charlieegan3/tool-activities/pkg/tool/utils"
)

// ActivitySync is a job that syncs activity data
type ActivitySync struct {
	DB *sql.DB

	ScheduleOverride string

	StravaClientID     string
	StravaClientSecret string
	StravaRefreshToken string

	GoogleCredentialsJSON string
	GoogleBucketName      string
}

func (a *ActivitySync) Name() string {
	return "activity-sync"
}

func (a *ActivitySync) Run(ctx context.Context) error {
	accessToken, err := internalStrava.GetAccessToken(
		a.StravaClientID,
		a.StravaClientSecret,
		a.StravaRefreshToken,
	)

	if err != nil {
		return fmt.Errorf("failed to get access token: %w", err)
	}
	stravaClient := strava.NewClient(accessToken)
	stravaActivities := strava.NewActivitiesService(stravaClient)

	storageClient, err := storage.NewClient(
		ctx,
		option.WithCredentialsJSON([]byte(a.GoogleCredentialsJSON)),
	)
	if err != nil {
		return fmt.Errorf("failed to create google storage client: %v", err)
	}
	defer storageClient.Close()

	bucket := storageClient.Bucket(a.GoogleBucketName)

	doneCh := make(chan bool)
	errCh := make(chan error)

	go func() {
		goquDB := goqu.New("postgres", a.DB)
		query := goquDB.Select("id").
			From("activities.activities").
			Where(
				goqu.Or(
					goqu.C("data_digest").Eq(""),
					goqu.C("created_at").Gt(time.Now().Add(-10*24*time.Hour)),
				),
			).
			Order(goqu.C("id").Asc())

		var rows []struct {
			ID int64 `db:"id"`
		}

		err := query.Executor().ScanStructs(&rows)
		if err != nil {
			errCh <- fmt.Errorf("failed to get activity IDs: %v", err)
			return
		}

		for _, row := range rows {
			activity, err := stravaActivities.Get(row.ID).Do()
			if err, ok := err.(strava.Error); ok {
				if err.Message == "Record Not Found" {
					fmt.Println(row.ID, "not found, skipping")
					continue
				}
			}
			if err != nil {
				errCh <- fmt.Errorf("failed to get activity: %w", err)
				return
			}

			jsonActivityData, err := json.MarshalIndent(activity, "", "  ")
			if err != nil {
				errCh <- fmt.Errorf("failed to unmarshal activity: %w", err)
				return
			}

			var compressedBuf bytes.Buffer
			zw := gzip.NewWriter(&compressedBuf)
			zw.Name = fmt.Sprintf("%d.json", activity.Id)

			_, err = zw.Write(jsonActivityData)
			if err != nil {
				errCh <- fmt.Errorf("failed to compress activity: %w", err)
				return
			}

			if err := zw.Close(); err != nil {
				errCh <- fmt.Errorf("failed to close gzip writer: %w", err)
				return
			}
			digest := utils.CRC32Hash(compressedBuf.Bytes())

			objectUpdate := false
			obj := bucket.Object(fmt.Sprintf("activities/activities/%d.json.gz", activity.Id))
			_, err = obj.Attrs(ctx)
			if err == storage.ErrObjectNotExist {
				objectUpdate = true
			} else if err != nil {
				errCh <- fmt.Errorf("failed to get object attributes: %w", err)
				return
			}

			if !objectUpdate {
				r, err := obj.NewReader(ctx)
				if err != nil {
					errCh <- fmt.Errorf("failed to create reader to read from google storage: %w", err)
					return
				}

				readData, err := io.ReadAll(r)
				if err != nil {
					errCh <- fmt.Errorf("failed to read from google storage: %w", err)
					return
				}

				if utils.CRC32Hash(readData) != digest {
					objectUpdate = true
				}
			}

			if objectUpdate {
				w := obj.NewWriter(ctx)

				_, err = io.Copy(w, bytes.NewReader(compressedBuf.Bytes()))
				if err != nil {
					errCh <- fmt.Errorf("failed to write to google storage: %w", err)
					return
				}
				err = w.Close()
				if err != nil {
					errCh <- fmt.Errorf("failed to close google storage writer: %w", err)
					return
				}
				fmt.Println(row.ID, "object was updated")
			}

			query := goquDB.Update("activities.activities").
				Where(goqu.C("id").Eq(fmt.Sprintf("%d", row.ID))).
				Set(goqu.Record{
					"data_digest": digest,
					"type":        activity.Type,
					"gear_id":     activity.GearId,
					"timestamp":   activity.StartDate,
				})
			_, err = query.Executor().Exec()
			if err != nil {
				errCh <- fmt.Errorf("failed to get activity IDs: %v", err)
				return
			}
		}

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

func (a *ActivitySync) Timeout() time.Duration {
	return 30 * time.Second
}

func (a *ActivitySync) Schedule() string {
	if a.ScheduleOverride != "" {
		return a.ScheduleOverride
	}
	return "0 30 * * * *"
}
