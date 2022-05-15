package entity

type CreateOnceJob struct {
	JobName     string            `json:"job_name"`
	JobExecTime int64             `json:"job_exec_time"`
	Namespace   string            `json:"namespace"`
	Metadata    map[string]string `json:"metadata"`
	Image       string            `json:"image"`
	Command     string            `json:"command"`
	Timeout     int64             `json:"timeout"`
	JobId       string            `json:"-"`
	Route       string            `json:"route"`
}

type UpdateOnceJob struct {
	JobId       string            `json:"job_id"`
	JobExecTime int64             `json:"job_exec_time"`
	Namespace   string            `json:"namespace"`
	Metadata    map[string]string `json:"metadata"`
	Image       string            `json:"image"`
	Command     string            `json:"command"`
	Timeout     int64             `json:"timeout"`
	Route       string            `json:"route"`
}

type PageOnceJob struct {
	Total    int64      `json:"total"`
	PageNum  int        `json:"page_num"`
	PageSize int        `json:"page_size"`
	Data     []*OnceJob `json:"data"`
}

type PageCronJob struct {
	Total    int64      `json:"total"`
	PageNum  int        `json:"page_num"`
	PageSize int        `json:"page_size"`
	Data     []*CronJob `json:"data"`
}

type CreateCronJob struct {
	JobName     string            `json:"job_name"`
	Schedule    string            `json:"schedule"`
	JobExecTime int64             `json:"job_exec_time"`
	Namespace   string            `json:"namespace"`
	Metadata    map[string]string `json:"metadata"`
	Image       string            `json:"image"`
	Command     string            `json:"command"`
	Timeout     int64             `json:"timeout"`
	JobId       string            `json:"-"`
	Route       string            `json:"route"`
}

type UpdateCronJob struct {
	JobId       string            `json:"job_id"`
	Schedule    string            `json:"schedule"`
	JobExecTime int64             `json:"job_exec_time"`
	Namespace   string            `json:"namespace"`
	Metadata    map[string]string `json:"metadata"`
	Image       string            `json:"image"`
	Command     string            `json:"command"`
	Timeout     int               `json:"timeout"`
	Route       string            `json:"route"`
}

type JobCreateSuccess struct {
	JobId string `json:"job_id"`
}
