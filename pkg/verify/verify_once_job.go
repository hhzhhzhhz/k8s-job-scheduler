package verify

import (
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/spec"
	json "github.com/json-iterator/go"
	"regexp"
	"strconv"
	"strings"
)

var (
	namespaceRule, _ = regexp.Compile(`[a-z0-9]([-a-z0-9]*[a-z0-9])?`)
	jobNameRule, _   = regexp.Compile(`[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)`)
)

// VerifyCreateOnceJob verify the data
func VerifyCreateOnceJob(job *entity.CreateOnceJob) error {
	switch {
	case job.JobName == "":
		return fmt.Errorf("job_name is empty.")
	case job.Namespace == "":
		return fmt.Errorf("namespace is empty.")
	case job.Image == "":
		return fmt.Errorf("image is empty.")
	case job.Command == "":
		return fmt.Errorf("command is empty.")
	case len(job.Metadata) > 0:
		if err := VarifyMetadata(job.Metadata); err != nil {
			return err
		}
	}
	if err := Other(job); err != nil {
		return err
	}
	return nil
}

// Other TODO Verify
func Other(job *entity.CreateOnceJob) error {
	if !namespaceRule.MatchString(job.Namespace) {
		return fmt.Errorf("Invalid value %s e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?'", job.Namespace)
	}
	return nil
}

// VerifyUpdateOnceJob verify the data
func VerifyUpdateOnceJob(job *entity.UpdateOnceJob) error {
	switch {
	case job.Namespace == "":
		return fmt.Errorf("namespace is empty.")
	case job.Image == "":
		return fmt.Errorf("image is empty.")
	case job.Command == "":
		return fmt.Errorf("command is empty.")
	case job.JobId == "":
		return fmt.Errorf("job_id is empty.")
	case len(job.Metadata) > 0:
		if err := VarifyMetadata(job.Metadata); err != nil {
			return err
		}
	}
	return nil
}

func VarifyMetadata(metadata map[string]string) error {
	for k, meta := range metadata {
		JobSpec, ok := spec.JobSpec[k]
		if !ok {
			return fmt.Errorf("metadata value %s does not exist in %+v", k, spec.JobSpec)
		}
		switch k {
		case "labels", "annotations":
			labels := make(map[string]string)
			if err := json.Unmarshal([]byte(meta), &labels); err != nil {
				return fmt.Errorf("metadata.(labels|annotations) is not map[sting]string cause: %s", err.Error())
			}
		case "parallelism", "completions", "backoffLimit":
			var n int
			var err error
			if n, err = strconv.Atoi(meta); err != nil {
				return fmt.Errorf("metadata.(parallelism|completions|backoffLimit) is not map[sting]string cause: %s", err.Error())
			}
			if n <= 0 {
				return fmt.Errorf("metadata.(parallelism|completions|backoffLimit) is %d", n)
			}
		case "activeDeadlineSeconds", "ttlSecondsAfterFinished":
			var n int
			var err error
			if n, err = strconv.Atoi(meta); err != nil {
				return fmt.Errorf("metadata.(activeDeadlineSeconds|ttlSecondsAfterFinished) is not map[sting]string cause: %s", err.Error())
			}
			if n <= 0 {
				return fmt.Errorf("metadata.(activeDeadlineSeconds|ttlSecondsAfterFinished) is %d", n)
			}
		case "restartPolicy":
			rps := strings.Split(JobSpec, ",")
			var exist bool
			for _, r := range rps {
				if meta == r {
					exist = true
				}
			}
			if !exist {
				return fmt.Errorf("metadata.restartPolicy %s not in %s", meta, rps)
			}
		}
	}
	return nil
}
