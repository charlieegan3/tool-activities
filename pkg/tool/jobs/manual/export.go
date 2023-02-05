package manual

import (
	"bytes"
	"cloud.google.com/go/storage"
	"compress/gzip"
	"context"
	"database/sql"
	_ "embed"
	"encoding/csv"
	"fmt"
	"github.com/charlieegan3/tool-activities/pkg/tool/utils"
	"google.golang.org/api/option"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"
)

// FromExport is a job that imports data from a GDPR export. It is
// intended to be run manually on a local machine where a download
// has been downloaded. This will import activity ids and original
// activity files from the export.
type FromExport struct {
	DB *sql.DB

	GoogleCredentialsJSON string
	GoogleBucketName      string
}

func (f *FromExport) Name() string {
	return "from-export"
}

func (f *FromExport) Run(ctx context.Context) error {
	doneCh := make(chan bool)
	errCh := make(chan error)

	storageClient, err := storage.NewClient(
		ctx,
		option.WithCredentialsJSON([]byte(f.GoogleCredentialsJSON)),
	)
	if err != nil {
		return fmt.Errorf("failed to create google storage client: %v", err)
	}
	defer storageClient.Close()

	goquDB := goqu.New("postgres", f.DB)
	bucket := storageClient.Bucket(f.GoogleBucketName)

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
		idOriginalFileMap := make(map[string]string)
		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				errCh <- fmt.Errorf("failed to read record: %v", err)
				return
			}
			if record[0] == "Activity ID" {
				continue
			}

			if len(record) < 1 {
				continue
			}

			ids = append(ids, goqu.Record{
				"id":     record[0],
				"source": "export",
			})

			idOriginalFileMap[record[0]] = record[12]
		}

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

		for id, originalFile := range idOriginalFileMap {
			if originalFile == "" {
				continue
			}
			file, err := os.Open(filepath.Join(os.Args[2], originalFile))
			if err != nil {
				errCh <- err
				return
			}

			var rawBytes []byte
			if strings.HasSuffix(file.Name(), ".gz") {
				zr, err := gzip.NewReader(file)
				if err != nil {
					errCh <- err
					return
				}

				rawBytes, err = io.ReadAll(zr)
				if err != nil {
					errCh <- err
					return
				}
			} else {
				rawBytes, err = io.ReadAll(file)
				if err != nil {
					errCh <- err
					return
				}
			}

			var compressedBuf bytes.Buffer
			zw := gzip.NewWriter(&compressedBuf)
			zw.Name = id

			_, err = zw.Write(rawBytes)
			if err != nil {
				errCh <- fmt.Errorf("failed to compress original: %w", err)
				return
			}
			if err := zw.Close(); err != nil {
				errCh <- fmt.Errorf("failed to close gzip writer: %w", err)
				return
			}

			format := "unknown"
			fmt.Println(file.Name())
			if strings.Contains(file.Name(), ".fit") {
				format = "fit"
			} else if strings.Contains(file.Name(), ".gpx") {
				format = "gpx"
			} else if strings.Contains(file.Name(), ".tcx") {
				format = "tcx"
			}
			digest := utils.CRC32Hash(compressedBuf.Bytes())

			obj := bucket.Object(fmt.Sprintf("activities/original/%s.%s.gz", id, format))
			w := obj.NewWriter(ctx)
			_, err = io.Copy(w, bytes.NewReader(compressedBuf.Bytes()))
			if err != nil {
				errCh <- fmt.Errorf("failed to write to google storage: %w", err)
				return
			}
			w.Close()

			query := goquDB.Update("activities.activities").
				Where(goqu.C("id").Eq(id)).
				Set(goqu.Record{
					"original_digest": digest,
					"original_format": format,
				})

			_, err = query.Executor().Exec()
			if err != nil {
				errCh <- err
				return
			}

			fmt.Println(id)
		}

		doneCh <- true
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case e := <-errCh:
		return fmt.Errorf("job failed with error: %s", e)
	case <-doneCh:
		return nil
	}
}

func (f *FromExport) Timeout() time.Duration {
	return 30 * time.Second
}

func (f *FromExport) Schedule() string {
	return "0 0 6 * * *"
}
