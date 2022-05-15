package verify

import (
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
)

// VerifyCreateOnceJob verify the data
func VerifyCreateCronJob(job *entity.CreateCronJob) error {
	switch {
	case job.JobName == "":
		return fmt.Errorf("job_name is empty")
	case job.Namespace == "":
		return fmt.Errorf("namespace is empty")
	case job.Image == "":
		return fmt.Errorf("image is empty")
	case job.Schedule == "":
		return fmt.Errorf("schedule is empty")
	case job.Command == "":
		return fmt.Errorf("command is empty")
	case len(job.Metadata) > 0:
		if err := VarifyMetadata(job.Metadata); err != nil {
			return err
		}
	}
	if err := OtherCron(job); err != nil {
		return err
	}
	return nil
}

// OtherCron Other TODO Verify
func OtherCron(job *entity.CreateCronJob) error {
	if !namespaceRule.MatchString(job.Namespace) {
		return fmt.Errorf("Invalid value %s e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?'", job.Namespace)
	}
	return nil
}

// VerifyUpdateOnceJob verify the data
func VerifyUpdateCronJob(job *entity.UpdateOnceJob) error {
	switch {
	case job.Namespace == "":
		return fmt.Errorf("namespace is empty")
	case job.Image == "":
		return fmt.Errorf("image is empty")
	case job.Command == "":
		return fmt.Errorf("command is empty")
	case job.Timeout <= 0:
		return fmt.Errorf("timeout is empty")
	case job.JobId == "":
		return fmt.Errorf("job_id is empty")
	case len(job.Metadata) > 0:
		if err := VarifyMetadata(job.Metadata); err != nil {
			return err
		}
	}
	return nil
}
