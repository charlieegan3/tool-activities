package manual

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/doug-martin/goqu/v9"
)

// FromExport is a job that imports data from a GDPR export. It is
// intended to be run manually on a local machine where a download
// has been downloaded.
type FromExport struct {
	DB *sql.DB

	ScheduleOverride string
}

func (f *FromExport) Name() string {
	return "from-export"
}

func (f *FromExport) Run(ctx context.Context) error {
	doneCh := make(chan bool)
	errCh := make(chan error)

	go func() {
		if len(os.Args) < 3 {
			errCh <- fmt.Errorf("expected a path to the export file")
			return
		}
		activitiesPath := filepath.Join(os.Args[2], "activities.csv")

		file, err := os.Open(activitiesPath)
		if err != nil {
			errCh <- fmt.Errorf("failed to open file: %v", err)
			return
		}

		r := csv.NewReader(file)

		var ids []goqu.Record
		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				errCh <- fmt.Errorf("failed to read record: %v", err)
				return
			}

			if len(record) < 1 {
				continue
			}

			ids = append(ids, goqu.Record{
				"id":     record[0],
				"source": "export",
			})
		}

		goquDB := goqu.New("postgres", f.DB)
		query := goquDB.Insert("activities.activities").
			Rows(ids).
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
		return fmt.Errorf("job failed with error: %f", e)
	case <-doneCh:
		return nil
	}
}

func (f *FromExport) Timeout() time.Duration {
	return 30 * time.Second
}

func (f *FromExport) Schedule() string {
	if f.ScheduleOverride != "" {
		return f.ScheduleOverride
	}
	return "0 0 6 * * *"
}
