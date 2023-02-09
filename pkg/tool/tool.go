package tool

import (
	"database/sql"
	"embed"
	"fmt"
	"github.com/Jeffail/gabs/v2"
	"github.com/charlieegan3/tool-activities/pkg/tool/jobs"
	"github.com/charlieegan3/toolbelt/pkg/apis"
	"github.com/gorilla/mux"
)

//go:embed migrations
var migrations embed.FS

// Activities is a tool that manages activity data
type Activities struct {
	db     *sql.DB
	config *gabs.Container

	stravaClientID     string
	stravaClientSecret string
	stravaRefreshToken string
	host               string
	email              string
	password           string

	googleServiceAccountJSON string
	googleProject            string
	googleBucketName         string

	scheduleActivityPoll     string
	scheduleActivitySync     string
	scheduleActivityOriginal string
}

func (a *Activities) Name() string {
	return "activities"
}

func (a *Activities) FeatureSet() apis.FeatureSet {
	return apis.FeatureSet{
		HTTP:     false,
		Config:   true,
		Jobs:     true,
		Database: true,
	}
}

func (a *Activities) DatabaseMigrations() (*embed.FS, string, error) {
	return &migrations, "migrations", nil
}

func (a *Activities) DatabaseSet(db *sql.DB) {
	a.db = db
}

func (a *Activities) SetConfig(config map[string]any) error {
	var path string
	var ok bool

	a.config = gabs.Wrap(config)

	path = "strava.client_id"
	a.stravaClientID, ok = a.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("missing required config path: %s", path)
	}

	path = "strava.host"
	a.host, ok = a.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("missing required config path: %s", path)
	}

	path = "strava.email"
	a.email, ok = a.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("missing required config path: %s", path)
	}

	path = "strava.password"
	a.password, ok = a.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("missing required config path: %s", path)
	}

	path = "strava.client_secret"
	a.stravaClientSecret, ok = a.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("missing required config path: %s", path)
	}

	path = "strava.refresh_token"
	a.stravaRefreshToken, ok = a.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("missing required config path: %s", path)
	}

	path = "jobs.activity_poll.schedule"
	a.scheduleActivityPoll, ok = a.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("missing required config path: %s", path)
	}

	path = "google.json"
	a.googleServiceAccountJSON, ok = a.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("missing required config path: %s", path)
	}

	path = "google.project"
	a.googleProject, ok = a.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("missing required config path: %s", path)
	}

	path = "google.bucket"
	a.googleBucketName, ok = a.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("missing required config path: %s", path)
	}

	path = "jobs.activity_poll.schedule"
	a.scheduleActivityPoll, ok = a.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("missing required config path: %s", path)
	}
	path = "jobs.activity_sync.schedule"
	a.scheduleActivitySync, ok = a.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("missing required config path: %s", path)
	}
	path = "jobs.activity_original.schedule"
	a.scheduleActivityOriginal, ok = a.config.Path(path).Data().(string)
	if !ok {
		return fmt.Errorf("missing required config path: %s", path)
	}

	return nil
}

func (a *Activities) Jobs() ([]apis.Job, error) {
	return []apis.Job{
		&jobs.ActivityPoll{
			DB:                 a.db,
			StravaClientID:     a.stravaClientID,
			StravaClientSecret: a.stravaClientSecret,
			StravaRefreshToken: a.stravaRefreshToken,
			ScheduleOverride:   a.scheduleActivityPoll,
		},
		&jobs.ActivitySync{
			DB:                    a.db,
			StravaClientID:        a.stravaClientID,
			StravaClientSecret:    a.stravaClientSecret,
			StravaRefreshToken:    a.stravaRefreshToken,
			GoogleCredentialsJSON: a.googleServiceAccountJSON,
			GoogleBucketName:      a.googleBucketName,
			ScheduleOverride:      a.scheduleActivitySync,
		},
		&jobs.ActivityOriginal{
			DB:                    a.db,
			StravaClientID:        a.stravaClientID,
			StravaClientSecret:    a.stravaClientSecret,
			StravaRefreshToken:    a.stravaRefreshToken,
			Host:                  a.host,
			Email:                 a.email,
			Password:              a.password,
			GoogleCredentialsJSON: a.googleServiceAccountJSON,
			GoogleBucketName:      a.googleBucketName,
			ScheduleOverride:      a.scheduleActivityOriginal,
		},
	}, nil
}

func (a *Activities) HTTPAttach(router *mux.Router) error                    { return nil }
func (a *Activities) HTTPHost() string                                       { return "" }
func (a *Activities) HTTPPath() string                                       { return "" }
func (a *Activities) ExternalJobsFuncSet(f func(job apis.ExternalJob) error) {}
