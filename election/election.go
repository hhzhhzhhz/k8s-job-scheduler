package election

import (
	"context"
	"github.com/google/uuid"
	"github.com/hhzhhzhhz/k8s-job-scheduler/log"
	"github.com/hhzhhzhhz/k8s-job-scheduler/pkg/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"sync"
	"time"
)

type FunType string

const (
	Follower FunType = "Follower"
	Leader   FunType = "Leader"
)

type Election interface {
	Leader() bool
	Leadership() string
	Register(ft FunType, f func())
	Run()
	Close() error
}

func NewElection(ctx context.Context, identity string, kubeCli kubernetes.Interface, namespace string) *election {
	if identity == "" {
		identity = uuid.New().String()
	}
	ctx, cancle := context.WithCancel(ctx)
	return &election{
		ctx:       ctx,
		cancel:    cancle,
		namespace: namespace,
		identity:  identity,
		kubeCli:   kubeCli,
		unmap:     make(map[FunType][]func(), 0),
	}
}

type election struct {
	ctx    context.Context
	cancel context.CancelFunc
	leader bool
	// Have you ever been a leader
	once           bool
	unmap          map[FunType][]func()
	mux            sync.Mutex
	identity       string
	leaderIdentity string
	namespace      string
	kubeCli        kubernetes.Interface
}

func (e *election) Register(ft FunType, f func()) {
	fs, ok := e.unmap[ft]
	if ok {
		fs = append(fs, f)
		return
	}
	e.unmap[ft] = []func(){f}
}

func (e *election) Leader() bool {
	e.mux.Lock()
	defer e.mux.Unlock()
	return e.leader
}

func (e *election) Run() {
	utils.Wrap(func() {
		leaseLock := &resourcelock.LeaseLock{
			LeaseMeta: metav1.ObjectMeta{
				Name:      "job",
				Namespace: e.namespace,
			},
			Client: e.kubeCli.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				Identity: e.identity,
			},
		}
		leaderelection.RunOrDie(e.ctx, leaderelection.LeaderElectionConfig{
			Lock: leaseLock,
			// IMPORTANT: you MUST ensure that any code you have that
			// is protected by the lease must terminate **before**
			// you call cancel. Otherwise, you could have a background
			// loop still running and another process could
			// get elected before your background loop finished, violating
			// the stated goal of the lease.
			ReleaseOnCancel: true,
			LeaseDuration:   10 * time.Second,
			RenewDeadline:   8 * time.Second,
			RetryPeriod:     5 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) {
					log.Logger().Info("Election.OnStartedLeading identity=%s", e.identity)
					// we're notified when we start - this is where you would
					// usually put your code
				},
				OnStoppedLeading: func() {
					log.Logger().Info("Election.OnStoppedLeading identity=%s", e.identity)
					// we can do cleanup here
					e.mux.Lock()
					e.leader = false
					e.mux.Unlock()
				},
				OnNewLeader: func(identity string) {
					e.mux.Lock()
					defer e.mux.Unlock()
					e.leaderIdentity = identity
					// we're notified when new leader elected
					log.Logger().Info("Election.OnNewLeader isleader=%t leader=%s current=%s ", identity == e.identity, identity, e.identity)
					if identity == e.identity {
						e.once = true
						e.leader = true
						fs, ok := e.unmap[Leader]
						if ok {
							for _, fun := range fs {
								fun()
							}
						}
						return
					}
					fs, ok := e.unmap[Follower]
					if ok && e.once {
						for _, fun := range fs {
							fun()
						}
					}
				},
			},
		})
	})
}

func (e *election) Leadership() string {
	e.mux.Lock()
	defer e.mux.Unlock()
	if e.leader {
		return ""
	}
	return e.leaderIdentity
}

func (e *election) Close() error {
	e.mux.Lock()
	defer e.mux.Unlock()
	e.cancel()
	return nil
}
