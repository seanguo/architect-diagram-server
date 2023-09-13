package report

import (
	"go.uber.org/zap"
	"log"
)

var l *zap.SugaredLogger

func init() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}
	l = logger.Sugar()
}

type Reporter struct {
	endpoint        string
	progressChannel chan *Progress
}

func NewReporter(serverAddress string) *Reporter {
	return &Reporter{
		endpoint:        serverAddress,
		progressChannel: make(chan *Progress),
	}
}

func (r *Reporter) Producer() chan *Progress {
	return r.progressChannel
}

func (*Reporter) Report(progress *Progress) error {
	l.Infof(progress.String())
	return nil
}

func (r *Reporter) Start() error {
	go func() {
		for p := range r.progressChannel {
			err := r.Report(p)
			if err != nil {
				l.Error("Got error" + err.Error())
			}
		}
	}()
	return nil
}

func (r *Reporter) Stop() error {
	close(r.progressChannel)
	return nil
}
