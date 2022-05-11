package constant

type Topic string

const (
	LabelFrom     = "k8s_job_scheduler"
	LabelTypeOnce = "once"
	LabelTypeCron = "cron"
)

type K8sKind string

func (k K8sKind) String() string {
	return string(k)
}

const (
	Job K8sKind = "Job"
	Pod K8sKind = "Pod"
)

type K8sEventType string

func (k K8sEventType) String() string {
	return string(k)
}

const (
	Normal  K8sEventType = "Normal"
	Warning K8sEventType = "Warning"
)

type K8sEventSource string

func (k K8sEventSource) String() string {
	return string(k)
}

const (
	JobController K8sEventSource = "job-controller"
)

type K8sJobControllerReason string

func (k K8sJobControllerReason) String() string {
	return string(k)
}

const (
	SuccessfulCreateK8sJobCon     K8sJobControllerReason = "SuccessfulCreate"
	BackoffLimitExceededK8sJobCon K8sJobControllerReason = "BackoffLimitExceeded"
	CompletedK8sJobCon            K8sJobControllerReason = "Completed"
	SuccessfulDeleteK8sJobCron    K8sJobControllerReason = "SuccessfulDelete"
	DeadlineExceededK8sJobCron    K8sJobControllerReason = "DeadlineExceeded"
)

type K8sCoreEventReason string

func (k K8sCoreEventReason) String() string {
	return string(k)
}

const (
	// SuccessfulCreate Created pod: xxx
	SuccessfulCreate K8sCoreEventReason = "SuccessfulCreate"
	// Pulling image "xx"
	Pulling K8sCoreEventReason = "Pulling"
	// Pulled Successfully pulled image "xxx" in 15.111s
	Pulled K8sCoreEventReason = "Pulled"
	// Created container xxx
	Created K8sCoreEventReason = "Created"
	// Started container xxx
	Started K8sCoreEventReason = "Started"
	// Completed Job completed
	Completed K8sCoreEventReason = "Completed"
	// SuccessfulDelete Deleted pod: xxx
	SuccessfulDelete K8sCoreEventReason = "SuccessfulDelete"
	// DeadlineExceeded Job was active longer than specified deadline
	DeadlineExceeded K8sCoreEventReason = "DeadlineExceeded"
	// Killing Stopping container xxx
	Killing K8sCoreEventReason = "Killing"

	// FailedMount -----
	// MountVolume.SetUp failed ...
	FailedMount K8sCoreEventReason = "FailedMount"
)
