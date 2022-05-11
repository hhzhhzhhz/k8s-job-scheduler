package client

type CallbackMessage struct {
	JobId   string `json:"job_id"`
	Success bool   `json:"success"`
	Message string `json:"message"`
}
