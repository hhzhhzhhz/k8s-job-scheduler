package utils

import (
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/constant"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/spec"
	json "github.com/json-iterator/go"
	v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
	"strings"
)

const (
	commandSplit      = ","
	podSplit          = ":"
	defaultJobTtl     = 600
	defaultJobTimeout = 300
)

const (
	JobId       = "job_id"
	JobFrom     = "job_from"
	JobName     = "job_name"
	JobType     = "job_type"
	LogCollect  = "log_collect"
	CallbackUrl = "callback_url"
	Args        = "args"
)

// GenOnceJob by rule
func GenOnceJob(base *entity.CreateOnceJob) (*v1.Job, error) {
	var envs []corev1.EnvVar
	var args []string
	var jobTtl, parallelism, completions, backoffLimit int32
	var restartPolicy corev1.RestartPolicy
	var completionMode v1.CompletionMode
	annotations := make(map[string]string)
	labels := make(map[string]string)
	jobTtl = int32(defaultJobTtl)
	timeout := int64(defaultJobTimeout)
	restartPolicy = corev1.RestartPolicyNever
	completionMode = v1.NonIndexedCompletion
	parallelism = 1
	completions = 1
	backoffLimit = 1
	if timeout > 0 {
		timeout = base.Timeout
	}

	labels[JobId] = base.JobId
	labels[JobFrom] = constant.LabelFrom
	labels[JobName] = base.JobName
	labels[JobType] = constant.LabelTypeOnce

	for k, meta := range base.Metadata {
		JobSpec, ok := spec.JobSpec[k]
		if !ok {
			continue
		}
		switch k {
		case "labels":
			lb := make(map[string]string)
			if err := json.Unmarshal([]byte(meta), &lb); err != nil {
				continue
			}
			for k, v := range lb {
				labels[k] = v
			}
		case "annotations":
			as := make(map[string]string)
			if err := json.Unmarshal([]byte(meta), &as); err != nil {
				continue
			}
			for k, v := range as {
				annotations[k] = v
			}
		case "log_collect":
			labels[LogCollect] = meta
		case "args":
			labels[Args] = meta
		case "callback_url":
			if CallbackUrl != "" {
				annotations[CallbackUrl] = meta
			}
		case "envs":
			as := make(map[string]string)
			if err := json.Unmarshal([]byte(meta), &as); err != nil {
				continue
			}
			for k, v := range as {
				envs = append(envs, corev1.EnvVar{Name: k, Value: v})
			}
		case "parallelism":
			var n int32
			var err error
			if n, err = ParseInt32(meta); err != nil {
				continue
			}
			if n <= 0 {
				continue
			}
			parallelism = n
		case "completions":
			var n int32
			var err error
			if n, err = ParseInt32(meta); err != nil {
				continue
			}
			if n <= 0 {
				continue
			}
			completions = n
		case "backoff_limit":
			var n int32
			var err error
			if n, err = ParseInt32(meta); err != nil {
				continue
			}
			if n <= 0 {
				continue
			}
			backoffLimit = n
		case "active_deadline_seconds":
			var n int64
			var err error
			if n, err = strconv.ParseInt(meta, 10, 64); err != nil {
				continue
			}
			if n <= 0 {
				continue
			}
			timeout = n
		case "ttl_seconds_after_finished":
			var n int32
			var err error
			if n, err = ParseInt32(meta); err != nil {
				continue
			}
			if n <= 0 {
				continue
			}
			jobTtl = n
		case "restart_policy":
			rps := strings.Split(JobSpec, ",")
			var exist bool
			for _, r := range rps {
				if meta == r {
					exist = true
				}
			}
			if !exist {
				restartPolicy = corev1.RestartPolicy(meta)
			}
		case "completion_mode":
			rps := strings.Split(JobSpec, ",")
			var exist bool
			for _, r := range rps {
				if meta == r {
					exist = true
				}
			}
			if !exist {
				restartPolicy = corev1.RestartPolicy(meta)
			}
		}
	}

	job := &v1.Job{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Job",
			APIVersion: "batch/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        base.JobName,
			Namespace:   base.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: v1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: restartPolicy,
					Containers: []corev1.Container{
						{
							Name:    base.JobName,
							Image:   base.Image,
							Command: strings.Split(base.Command, commandSplit),
							Env:     envs,
							Args:    args,
						},
					},
				},
				ObjectMeta: metav1.ObjectMeta{
					Annotations: annotations,
					Labels:      labels,
				},
			},
			Parallelism:             &parallelism,
			Completions:             &completions,
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &jobTtl,
			ActiveDeadlineSeconds:   &timeout,
			CompletionMode:          &completionMode,
		},
	}
	return job, nil
}

// UpdateJob spec.Completions, spec.Selector，spec.Template is not allowed to be updated.
func UpdateJob(base *entity.CreateOnceJob, job *v1.Job) (*v1.Job, error) {
	jobn, err := GenOnceJob(base)
	if err != nil {
		return nil, err
	}
	flag := true
	job.Spec.ManualSelector = &flag
	job.Spec.Parallelism = jobn.Spec.Parallelism
	job.Spec.Completions = jobn.Spec.Completions
	job.Spec.BackoffLimit = jobn.Spec.BackoffLimit
	job.Spec.TTLSecondsAfterFinished = jobn.Spec.TTLSecondsAfterFinished
	job.Spec.ActiveDeadlineSeconds = jobn.Spec.ActiveDeadlineSeconds
	job.Spec.CompletionMode = jobn.Spec.CompletionMode
	return job, nil
}

func GenCronJob(base *entity.CreateCronJob) (*v1.CronJob, error) {
	var envs []corev1.EnvVar
	var jobTtl, parallelism, completions, backoffLimit int32
	var restartPolicy corev1.RestartPolicy
	var completionMode v1.CompletionMode
	var args []string
	annotations := make(map[string]string)
	labels := make(map[string]string)
	jobTtl = int32(defaultJobTtl)
	timeout := int64(defaultJobTimeout)
	restartPolicy = corev1.RestartPolicyNever
	completionMode = v1.NonIndexedCompletion
	parallelism = 1
	completions = 1
	backoffLimit = 1

	labels[JobId] = base.JobId
	labels[JobFrom] = constant.LabelFrom
	labels[JobName] = base.JobName
	labels[JobType] = constant.LabelTypeCron

	if timeout > 0 {
		timeout = base.Timeout
	}

	for k, meta := range base.Metadata {
		JobSpec, ok := spec.JobSpec[k]
		if !ok {
			continue
		}
		switch k {
		case "labels":
			lb := make(map[string]string)
			if err := json.Unmarshal([]byte(meta), &lb); err != nil {
				continue
			}
			for k, v := range lb {
				labels[k] = v
			}
		case "log_collect":
			labels[LogCollect] = meta
		case "args":
			labels[Args] = meta
		case "callback_url":
			if CallbackUrl != "" {
				annotations[CallbackUrl] = meta
			}
		case "annotations":
			as := make(map[string]string)
			if err := json.Unmarshal([]byte(meta), &as); err != nil {
				continue
			}
			for k, v := range as {
				annotations[k] = v
			}
		case "envs":
			as := make(map[string]string)
			if err := json.Unmarshal([]byte(meta), &as); err != nil {
				continue
			}
			for k, v := range as {
				envs = append(envs, corev1.EnvVar{Name: k, Value: v})
			}
		case "parallelism":
			var n int32
			var err error
			if n, err = ParseInt32(meta); err != nil {
				continue
			}
			if n <= 0 {
				continue
			}
			parallelism = n
		case "completions":
			var n int32
			var err error
			if n, err = ParseInt32(meta); err != nil {
				continue
			}
			if n <= 0 {
				continue
			}
			completions = n
		case "backoff_limit":
			var n int32
			var err error
			if n, err = ParseInt32(meta); err != nil {
				continue
			}
			if n <= 0 {
				continue
			}
			backoffLimit = n
		case "active_deadline_seconds":
			var n int64
			var err error
			if n, err = strconv.ParseInt(meta, 10, 64); err != nil {
				continue
			}
			if n <= 0 {
				continue
			}
			timeout = n
		case "ttl_seconds_after_finished":
			var n int32
			var err error
			if n, err = ParseInt32(meta); err != nil {
				continue
			}
			if n <= 0 {
				continue
			}
			jobTtl = n
		case "restart_policy":
			rps := strings.Split(JobSpec, ",")
			var exist bool
			for _, r := range rps {
				if meta == r {
					exist = true
				}
			}
			if !exist {
				restartPolicy = corev1.RestartPolicy(meta)
			}
		case "completion_mode":
			rps := strings.Split(JobSpec, ",")
			var exist bool
			for _, r := range rps {
				if meta == r {
					exist = true
				}
			}
			if !exist {
				restartPolicy = corev1.RestartPolicy(meta)
			}
		}
	}

	cronJob := &v1.CronJob{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Job",
			APIVersion: "batch/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        base.JobName,
			Namespace:   base.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: v1.CronJobSpec{
			JobTemplate: v1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:        base.JobName,
					Namespace:   base.Namespace,
					Labels:      labels,
					Annotations: annotations,
				},
				Spec: v1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: restartPolicy,
							Containers: []corev1.Container{
								{
									Name:    base.JobName,
									Image:   base.Image,
									Command: strings.Split(base.Command, commandSplit),
									Env:     envs,
									Args:    args,
								},
							},
						},
						ObjectMeta: metav1.ObjectMeta{
							Labels:      labels,
							Annotations: annotations,
						},
					},
					Parallelism:             &parallelism,
					Completions:             &completions,
					BackoffLimit:            &backoffLimit,
					TTLSecondsAfterFinished: &jobTtl,
					ActiveDeadlineSeconds:   &timeout,
					CompletionMode:          &completionMode,
				},
			},

			Schedule: base.Schedule,
		},
	}

	return cronJob, nil
}
