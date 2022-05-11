package entity

import v1 "k8s.io/api/batch/v1"

type JobType int

const (
	Once JobType = iota
	Cron
)

// JobAction exec clint
type JobAction string

const (
	Create JobAction = "Create"
	Delete JobAction = "Delete"
	Cancel JobAction = "Cancel"
)

// JobMessage job message
type JobMessage struct {
	JobType   JobType     `json:"job_type"`
	JobAction JobAction   `json:"job_action"`
	JobId     string      `json:"job_id"`
	JobName   string      `json:"job_name"`
	Namespace string      `json:"namespace"`
	OnceJob   *v1.Job     `json:"once_job"`
	CronJob   *v1.CronJob `json:"cron_job"`
}

// JobExecStatusMessage job exec status info
type JobExecStatusMessage struct {
	JobId              string            `json:"job_id"`
	JobName            string            `json:"job_name"`
	PodName            string            `json:"pod_name"`
	JobType            JobType           `json:"job_type"`
	JobState           JobState          `json:"job_state"`
	Parent             JobType           `json:"parent"`
	JobExecInformation map[string]string `json:"job_exec_information"`
	Logs               []byte            `json:"logs"`
	Time               int64             `json:"time"`
}
