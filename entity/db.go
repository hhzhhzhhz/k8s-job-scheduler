package entity

type JobState int

const (
	Submit JobState = iota - 1
	CreateSuccess
	Failed
	JobSuccess
	DeleteSuccess
	RunLogs
)

// OnceJob table cron_job
type OnceJob struct {
	Id                 int64    `json:"id" db:"id"`
	JobId              string   `json:"job_id" db:"job_id"`
	JobName            string   `json:"job_name" db:"job_name"`
	Namespace          string   `json:"namespace" db:"namespace"`
	JobExecTime        int64    `json:"job_exec_time" db:"job_exec_time"`
	Metadata           string   `json:"metadata" db:"metadata"`
	Image              string   `json:"image" db:"image"`
	Command            string   `json:"command" db:"command"`
	JobTimes           int64    `json:"job_times" db:"job_times"`
	JobState           JobState `json:"job_state" db:"job_state"`
	JobSuccessTimes    int64    `json:"job_success_times" db:"job_success_times"`
	JobExecInformation string   `json:"job_exec_information" db:"job_exec_information"`
	CreateTime         int64    `json:"create_time" db:"create_time"`
	UpdateTime         int64    `json:"update_time" db:"update_time"`
}

// CronJob table cron_job
type CronJob struct {
	Id                 int64    `json:"id" db:"id"`
	JobId              string   `json:"job_id" db:"job_id"`
	JobName            string   `json:"job_name" db:"job_name"`
	JobExecTime        int64    `json:"job_exec_time" db:"job_exec_time"`
	Namespace          string   `json:"namespace" db:"namespace"`
	Schedule           string   `json:"schedule" db:"schedule"`
	Metadata           string   `json:"metadata" db:"metadata"`
	Image              string   `json:"image" db:"image"`
	Command            string   `json:"command" db:"command"`
	JobState           JobState `json:"job_state" db:"job_state"`
	JobTimes           int64    `json:"job_times" db:"job_times"`
	JobSuccessTimes    int64    `json:"job_success_times" db:"job_success_times"`
	JobExecInformation string   `json:"job_exec_information" db:"job_exec_information"`
	CreateTime         int64    `json:"create_time" db:"create_time"`
	UpdateTime         int64    `json:"update_time" db:"update_time"`
}

// JobStatus table job_status
type JobStatus struct {
	Id                 int64    `json:"id" db:"id"`
	JobId              string   `json:"job_id" db:"job_id"`
	JobName            string   `json:"job_name" db:"job_name"`
	JobType            JobType  `json:"job_type" db:"job_type"`
	JobState           JobState `json:"job_state" db:"job_state"`
	JobTimes           int64    `json:"job_times" db:"job_times"`
	JobSuccessTimes    int64    `json:"job_success_times" db:"job_success_times"`
	JobExecInformation string   `json:"job_exec_information" db:"job_exec_information"`
	CreateTime         int64    `json:"create_time" db:"create_time"`
	UpdateTime         int64    `json:"update_time" db:"update_time"`
}

// JobLogs JobLogs table JobLogs
type JobLogs struct {
	Id         int64  `json:"id" db:"id"`
	JobId      string `json:"job_id" db:"job_id"`
	PodName    string `json:"pod_name" db:"pod_name"`
	RunLogs    []byte `json:"-" db:"run_logs"`
	Logs       string `json:"logs" db:"-"`
	CreateTime int64  `json:"create_time" db:"create_time"`
}
