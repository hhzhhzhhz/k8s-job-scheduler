package server

import (
	"os"
	"os/signal"
	"syscall"
)

type Server interface {
	Init() error
	Start() error
	Stop() error
}

func Run(s Server, sigs ...os.Signal) error {

	if err := s.Init(); err != nil {
		return err
	}
	if err := s.Start(); err != nil {
		return err
	}

	if len(sigs) == 0 {
		sigs = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL}
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, sigs...)

	<-c
	return s.Stop()
}
