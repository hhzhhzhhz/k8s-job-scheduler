package server

import (
	"context"
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/apiserver"
	"github.com/hhzhhzhhz/k8s-job-scheduler/client"
	"github.com/hhzhhzhhz/k8s-job-scheduler/config"
	"github.com/hhzhhzhhz/k8s-job-scheduler/election"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure/kubernetes"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure/storage"
	"github.com/hhzhhzhhz/k8s-job-scheduler/log"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/protocol"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/utils"
	"github.com/hhzhhzhhz/k8s-job-scheduler/version"
	"go.uber.org/multierr"
)

type JobExecutor struct {
	ctx      context.Context
	cancel   context.CancelFunc
	exec     *client.Excellent
	election election.Election
}

func (j *JobExecutor) Init() error {
	j.ctx, j.cancel = context.WithCancel(context.Background())
	if err := config.Init(); err != nil {
		return err
	}
	utils.Wrap(func() {
		log.Logger().Info("pprof/metrics service start addr: %s", config.GetCfg().PprofPort)
		if err := protocol.NewPprofMetricServer(config.GetCfg().PprofPort); err != nil {
			log.Logger().Error("JobExecutor.start NewPprofMetricServer error cause=%s", err.Error())
		}
	})
	if err := kubernetes.InitKubeClient(config.GetKubeCfg()); err != nil {
		return fmt.Errorf("JobExecutor.InitMysql InitKubeClient failed cause: %s", err.Error())
	}
	if err := storage.InitMysql(config.GetCfg().Dsn); err != nil {
		return fmt.Errorf("JobExecutor.InitMysql InitMysql failed cause: %s", err.Error())
	}
	var err error
	if err = infrastructure.InitDelaymq(config.GetCfg().MQaddr); err != nil {
		return fmt.Errorf("JobExecutor.InitDelayQueueProducer failed cause: %s", err.Error())
	}
	api := apiserver.NewApiserver(kubernetes.KubernetesClient())
	election := election.NewElection(j.ctx, "", kubernetes.KubernetesClient(), config.GetCfg().Client.LeaseLockNamespace)
	j.election = election
	ecli := client.NewExcellent(j.ctx, infrastructure.GetDelayMq(), api, j.election, config.GetCfg())
	j.exec = ecli
	return err
}

func (j *JobExecutor) Start() error {
	log.Logger().Info("server starting")
	log.Logger().Info("version info: %+v", version.Version)
	if err := j.exec.Run(); err != nil {
		return err
	}
	j.election.Run()
	log.Logger().Info("server started")
	return nil
}

func (j *JobExecutor) Stop() error {
	log.Logger().Info("server ready to close")
	j.cancel()
	var errs error
	errs = multierr.Append(errs, j.exec.Close())
	errs = multierr.Append(errs, j.election.Close())
	errs = multierr.Append(errs, storage.Close())
	errs = multierr.Append(errs, infrastructure.GetDelayMq().Close())
	log.Logger().Info("server is closed")
	log.Logger().Close()
	return errs
}
