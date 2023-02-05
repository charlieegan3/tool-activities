package jobs

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/PuerkitoBio/goquery"
	"github.com/charlieegan3/tool-activities/pkg/tool/utils"
	"github.com/doug-martin/goqu/v9"
	"google.golang.org/api/option"
)

// ActivityOriginal is a job that saves the original activity data
type ActivityOriginal struct {
	DB *sql.DB

	ScheduleOverride string

	StravaClientID     string
	StravaClientSecret string
	StravaRefreshToken string
	Host               string
	Email              string
	Password           string

	GoogleCredentialsJSON string
	GoogleBucketName      string
}

func (a *ActivityOriginal) Name() string {
	return "activity-original"
}

func (a *ActivityOriginal) Run(ctx context.Context) error {
	doneCh := make(chan bool)
	errCh := make(chan error)

	storageClient, err := storage.NewClient(
		ctx,
		option.WithCredentialsJSON([]byte(a.GoogleCredentialsJSON)),
	)
	if err != nil {
		return fmt.Errorf("failed to create google storage client: %v", err)
	}
	defer storageClient.Close()

	bucket := storageClient.Bucket(a.GoogleBucketName)

	go func() {
		sessionURL := fmt.Sprintf("https://%s/session", a.Host)
		loginURL := fmt.Sprintf("https://%s/login", a.Host)

		// get login form token
		client := &http.Client{}
		req, err := http.NewRequest("GET", loginURL, nil)

		req.Header.Add("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15")
		req.Header.Add("Upgrade-Insecure-Requests", "1")
		req.Header.Add("DNT", "1")
		req.Header.Add("Accept-Language", "en-GB,en;q=0.7,en-US;q=0.3")

		res, err := client.Do(req)
		if err != nil {
			errCh <- err
			return
		}

		cookie := fmt.Sprintf("%s", res.Header.Get("Set-Cookie"))

		body, err := io.ReadAll(res.Body)
		if err != nil {
			errCh <- err
			return
		}

		doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
		if err != nil {
			errCh <- err
			return
		}

		var authenticityToken string
		doc.Find(`form input[name=authenticity_token]`).Each(func(i int, s *goquery.Selection) {
			var found bool
			authenticityToken, found = s.Attr("value")
			if !found {
				log.Fatal("authenticity token not found")
			}
		})

		// get session token
		data := url.Values{}
		data.Add("utf8", "✓")
		data.Add("authenticity_token", authenticityToken)
		data.Add("plan", "")
		data.Add("email", a.Email)
		data.Add("password", a.Password)
		data.Add("remember_me", "on")

		req, err = http.NewRequest("POST", sessionURL, strings.NewReader(data.Encode()))

		req.Header.Add("User-Agent", "Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0")
		req.Header.Add("Upgrade-Insecure-Requests", "1")
		req.Header.Add("DNT", "1")
		req.Header.Add("Accept-Language", "en-GB,en;q=0.7,en-US;q=0.3")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Cookie", cookie)

		client = &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}

		res, err = client.Do(req)
		if err != nil {
			errCh <- err
			return
		}

		cookie = fmt.Sprintf("%s", res.Header.Get("Set-Cookie"))

		if res.StatusCode != http.StatusFound {
			errCh <- fmt.Errorf("login attempt failed with status code %d", res.StatusCode)
			return
		}
		location := res.Header.Get("Location")
		if location != fmt.Sprintf("https://%s/dashboard", a.Host) {
			errCh <- fmt.Errorf("login attempt was not correctly redirected to dashboard (%s)", location)
			return
		}

		goquDB := goqu.New("postgres", a.DB)
		query := goquDB.Select("id", "original_digest").
			From("activities.activities").
			Where(
				goqu.And(
					goqu.Or(
						goqu.C("original_digest").Eq(""),
						goqu.C("created_at").Gt(time.Now().Add(-5*24*time.Hour)),
					),
					goqu.C("original_format").Neq("missing"),
				),
			).
			Order(goqu.C("id").Asc())

		var rows []struct {
			ID             string `db:"id"`
			OriginalDigest string `db:"original_digest"`
		}

		err = query.Executor().ScanStructs(&rows)
		if err != nil {
			errCh <- err
			return
		}

		fmt.Println("processing", len(rows))

		// get data
		client = &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		for _, row := range rows {
			fmt.Println(row.ID)

			url := fmt.Sprintf("https://%s/activities/%s/export_original", a.Host, row.ID)

			req, err = http.NewRequest(
				"GET",
				url,
				nil,
			)
			if err != nil {
				errCh <- err
				return
			}
			req.Header.Add("Cookie", cookie)

			res, err = client.Do(req)
			if err != nil {
				errCh <- err
				return
			}

			if res.StatusCode != http.StatusOK {
				if res.StatusCode == 302 {
					query := goquDB.Update("activities.activities").
						Where(goqu.C("id").Eq(row.ID)).
						Set(goqu.Record{
							"original_format": "missing",
						})

					_, err = query.Executor().Exec()
					if err != nil {
						errCh <- err
						return
					}
					continue
				}

				errCh <- fmt.Errorf("failed to get activity %s with status code %d", row.ID, res.StatusCode)
				return
			}

			contentDisposition := res.Header.Get("Content-Disposition")
			format := "unknown"
			if strings.Contains(contentDisposition, `.fit"`) {
				format = "fit"
			} else if strings.Contains(contentDisposition, `.gpx"`) {
				format = "gpx"
			} else if strings.Contains(contentDisposition, `.tcx"`) {
				format = "tcx"
			}

			body, err = io.ReadAll(res.Body)
			if err != nil {
				errCh <- err
				return
			}

			var compressedBuf bytes.Buffer
			zw := gzip.NewWriter(&compressedBuf)
			zw.Name = fmt.Sprintf("%s.%s", row.ID, format)

			_, err = zw.Write(body)
			if err != nil {
				errCh <- fmt.Errorf("failed to compress activity: %w", err)
				return
			}
			if err := zw.Close(); err != nil {
				errCh <- fmt.Errorf("failed to close gzip writer: %w", err)
				return
			}
			digest := utils.CRC32Hash(compressedBuf.Bytes())

			// only update the bucket object if the original has changed
			if digest != row.OriginalDigest {
				obj := bucket.Object(fmt.Sprintf("activities/original/%s.%s.gz", row.ID, format))
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

				query := goquDB.Update("activities.activities").
					Where(goqu.C("id").Eq(row.ID)).
					Set(goqu.Record{
						"original_digest": digest,
						"original_format": format,
					})

				_, err = query.Executor().Exec()
				if err != nil {
					errCh <- err
					return
				}
			}
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

func (a *ActivityOriginal) Timeout() time.Duration {
	return 30 * time.Second
}

func (a *ActivityOriginal) Schedule() string {
	if a.ScheduleOverride != "" {
		return a.ScheduleOverride
	}
	return "0 30 * * * *"
}
