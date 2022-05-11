package main

import (
	"fmt"
	app "github.com/hhzhhzhhz/k8s-job-scheduler/pkg/server"
	"github.com/hhzhhzhhz/k8s-job-scheduler/server"
	"os"
)

func main() {
	if err := app.Run(&server.JobExecutor{}); err != nil {
		fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}
}
