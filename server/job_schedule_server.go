package server

import (
	"context"
	"fmt"
	"github.com/hhzhhzhhz/k8s-job-scheduler/config"
	"github.com/hhzhhzhhz/k8s-job-scheduler/election"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure/kubernetes"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure/rabbitmq"
	"github.com/hhzhhzhhz/k8s-job-scheduler/infrastructure/storage"
	"github.com/hhzhhzhhz/k8s-job-scheduler/log"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/protocol"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/utils"
	"github.com/hhzhhzhhz/k8s-job-scheduler/recipient"
	"github.com/hhzhhzhhz/k8s-job-scheduler/version"
	"go.uber.org/multierr"
	"net"
	_ "net/http/pprof"
)

type JobScheduleServer struct {
	ctx       context.Context
	cancel    context.CancelFunc
	app       *AppServer
	recipient *recipient.Recipient
	election  election.Election
	identity  string
}

func (s *JobScheduleServer) Init() error {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	if err := config.Init(); err != nil {
		return err
	}
	if err := kubernetes.InitKubeClient(config.GetKubeCfg()); err != nil {
		return fmt.Errorf("JobScheduleServer.InitMysql InitKubeClient failed cause=%s", err.Error())
	}
	if err := storage.InitMysql(config.GetCfg().Dsn); err != nil {
		return fmt.Errorf("JobScheduleServer.InitMysql InitMysql failed cause=%s", err.Error())
	}
	var err error
	if err = rabbitmq.InitRabbitMq(config.GetCfg().AmqpUrl); err != nil {
		return fmt.Errorf("JobScheduleServer.InitDelayQueueProducer failed cause=%s", err.Error())
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", config.GetCfg().ApiPort)
	if err != nil {
		return fmt.Errorf("net.ResolveTCPAddr %s failed cause=%s", config.GetCfg().ApiPort, err.Error())
	}
	ip, err := utils.GetPodIP()
	if err != nil {
		return fmt.Errorf("JobScheduleServer.GetPodIP failed cause=%s", err.Error())
	}
	s.identity = fmt.Sprintf("%s:%d", ip, tcpAddr.Port)
	election := election.NewElection(s.ctx, s.identity, kubernetes.KubernetesClient(), config.GetCfg().LeaseLockNamespace)
	s.election = election
	s.recipient = recipient.NewRecipient(s.ctx, rabbitmq.GetCommonQueue(), election, config.GetCfg())

	return err
}

func (s *JobScheduleServer) Start() error {
	log.Logger().Info("server starting")
	log.Logger().Info("version info: %+v", version.Version)
	s.recipient.Run()
	s.election.Run()
	app := NewAppServer(config.GetCfg().ApiPort, s.election)
	utils.Wrap(func() {
		log.Logger().Info("pprof/metrics service start addr=%s", config.GetCfg().PprofPort)
		if err := protocol.NewPprofMetricServer(config.GetCfg().PprofPort); err != nil {
			log.Logger().Error("JobScheduleServer.start NewPprofMetricServer error cause=%s", err.Error())
		}
	})
	utils.Wrap(func() {
		log.Logger().Info("http api service start addr=%s", config.GetCfg().ApiPort)
		if err := app.Run(); err != nil {
			log.Logger().Error("JobScheduleServer.start NewJobApiserver error cause=%s", err.Error())
		}
	})
	s.app = app
	log.Logger().Info("server started")
	return nil
}

func (s *JobScheduleServer) Stop() error {
	log.Logger().Info("server ready to close")
	var errs error
	s.cancel()
	errs = multierr.Append(errs, s.app.Close())
	errs = multierr.Append(errs, s.recipient.Close())
	errs = multierr.Append(errs, s.election.Close())
	errs = multierr.Append(errs, storage.Close())
	errs = multierr.Append(errs, rabbitmq.Close())
	log.Logger().Info("server is closed")
	log.Logger().Close()
	return errs
}
