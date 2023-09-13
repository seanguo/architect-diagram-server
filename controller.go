package main

import (
	"com.architectdiagram/m/driver"
	"com.architectdiagram/m/transport"
	"encoding/json"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"log"
	"net/http"
)

var l *zap.SugaredLogger
var upgraderC = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
} // use default options

func init() {
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}
	l = logger.Sugar()
}

type Controller struct {
	driverClient *driver.KubeClient
}

func New() *Controller {
	return &Controller{}
}

func (c *Controller) Start() {
	// validate connection to default driven system(etc. minikube)
	c.driverClient = driver.NewKubeClient()
	_, err := c.driverClient.GetPods()
	if err != nil {
		log.Fatalf("can't connecto minikube driver: %v", err)
	}

	// start the Websocket server
	http.HandleFunc("/commands", c.process)
	log.Fatal(http.ListenAndServe("localhost:8080", nil))
}

func (c *Controller) process(w http.ResponseWriter, r *http.Request) {
	conn, err := upgraderC.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer conn.Close()
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			break
		}
		log.Printf("recv: %s", message)
		msg := &transport.Message{}
		err = json.Unmarshal(message, msg)
		if err != nil {
			log.Println("decode:", err)
			break
		}
		switch msg.Type {
		case transport.COMMAND:
			l.Infof("got command from front end: %v", msg)
			c.handleCommand(msg)
		case transport.REPORT:
			l.Infof("got report from front end: %v", msg)
		default:
			l.Errorf("got unkown message: %v", msg)
		}
		//kubeClient := driver.NewKubeClient()
		//pods, err := kubeClient.GetPods()
		//if err != nil {
		//	log.Println("write:", err)
		//	break
		//}
		//marshal, err := json.Marshal(pods)
		//if err != nil {
		//	log.Println("marshall:", err)
		//	break
		//}
		//err = conn.WriteMessage(mt, marshal)
		//if err != nil {
		//	log.Println("write:", err)
		//	break
		//}
	}
}

// handles the command received from frontend via Websocket connection
// need to support batch command handling on import(TODO)
func (c *Controller) handleCommand(command *transport.Message) {
	//sent to internal Kafka topic (as event sourcing, TODO)
	c.processCommand(command)
}

// process command received from Kafka
func (c *Controller) processCommand(command *transport.Message) {
	// create resource in the driven system
	switch command.Content {
	case "list pods":
		{
			pods, err := c.driverClient.GetPods()
			if err != nil {
				l.Error("write:", err)
				return
			}
			for _, pod := range pods.Items {
				l.Infof("got pod: %v", pod)
			}
		}
	case "create pod":
		{
			err := c.driverClient.CreateDeployment()
			if err != nil {
				l.Error("create pod failed:", err)
				return
			}
		}
	}

	// drive resource to do action

	// or destroy resource (TODO resource cleanup mechanism)
}

// process report of resources in driven system from Kafka(e.g. progress report from Kafka Java cosonsumer)
func (*Controller) processReport() {
	// message the report and send to frontend via websocket
}

func (*Controller) Stop() {
	// stop the Websocket server

	// other resource cleanup if any
}

func main() {
	c := &Controller{}
	defer c.Stop()

	c.Start()
}
