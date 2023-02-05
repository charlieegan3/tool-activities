package main

import (
	"context"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/charlieegan3/toolbelt/pkg/database"
	"github.com/charlieegan3/toolbelt/pkg/tool"

	activitiesTool "github.com/charlieegan3/tool-activities/pkg/tool"
)

func main() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Fatal error config file: %s \n", err)
	}

	cfg, ok := viper.Get("tools").(map[string]interface{})
	if !ok {
		log.Fatalf("failed to read tools config in map[string]interface{} format")
		os.Exit(1)
	}

	// configure global cancel context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-c:
			cancel()
		}
	}()

	// load the database configuration
	params := viper.GetStringMapString("database.params")
	connectionString := viper.GetString("database.connectionString")
	db, err := database.Init(connectionString, params, params["dbname"], false)
	if err != nil {
		log.Fatalf("failed to init DB: %s", err)
	}
	// we have 5 connections, it's important to be able to manually connect to the db too
	db.SetMaxOpenConns(4)
	defer db.Close()

	// init the toolbelt, connecting the database, config and external runner
	tb := tool.NewBelt()
	tb.SetConfig(cfg)
	tb.SetDatabase(db)

	mt := activitiesTool.Activities{}
	err = tb.AddTool(ctx, &mt)
	if err != nil {
		log.Fatalf("failed to add tool: %v", err)
	}

	if len(os.Args) > 1 {
		jobs, err := mt.Jobs()
		if err != nil {
			log.Fatalf("failed to get jobs: %v", err)
		}

		switch os.Args[1] {
		case "from_export":
			err = jobs[0].Run(ctx)
			if err != nil {
				log.Fatalf("failed to run job: %v", err)
			}
		case "activity_poll":
			err = jobs[1].Run(ctx)
			if err != nil {
				log.Fatalf("failed to run job: %v", err)
			}
		case "activity_sync":
			err = jobs[2].Run(ctx)
			if err != nil {
				log.Fatalf("failed to run job: %v", err)
			}
		case "activity_original":
			err = jobs[3].Run(ctx)
			if err != nil {
				log.Fatalf("failed to run job: %v", err)
			}
		}

		os.Exit(0)
	}

	// go tb.RunJobs(ctx)
	tb.RunServer(ctx, "0.0.0.0", "3000")
}
