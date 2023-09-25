package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	// "com.architectdiagram/m/driver"
	"com.architectdiagram/m/node"
	"com.architectdiagram/m/transport"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
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
	// driverClient *driver.KubeClient
	nodesManager *node.Manager
}

func New() *Controller {
	return &Controller{
		nodesManager: node.NewManager(),
	}
}

func (c *Controller) Start() {
	// validate connection to default driven system(etc. minikube)
	//c.driverClient = driver.NewKubeClient()
	//_, err := c.driverClient.GetPods()
	//if err != nil {
	//	log.Fatalf("can't connecto minikube driver: %v", err)
	//}

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
			c.handleCommand(msg, conn)
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
func (c *Controller) handleCommand(command *transport.Message, conn *websocket.Conn) {
	//sent to internal Kafka topic (as event sourcing, TODO)
	c.processCommand(command, conn)
}

func sendReport(content string, conn *websocket.Conn) error {
	log.Println(fmt.Sprintf("writing message: %s", content))
	marshal, err := json.Marshal(&transport.Message{
		Type:      transport.REPORT,
		Timestamp: time.Now(),
		Content: map[string]interface{}{
			"content": content,
		},
	})
	if err != nil {
		log.Println("marshall:", err)
		return err
	}
	err = conn.WriteMessage(websocket.TextMessage, marshal)
	if err != nil {
		log.Println("write message:", err)
	}
	return err
}

// process command received from Kafka
func (c *Controller) processCommand(message *transport.Message, conn *websocket.Conn) {
	cmd := &transport.Command{}
	marshal, err := json.Marshal(message.Content)
	if err != nil {
		log.Println("decode command content failed:", err)
		return
	}
	err = json.Unmarshal(marshal, cmd)
	if err != nil {
		log.Println("decode command failed:", err)
		return
	}
	switch cmd.Type {
	case transport.CONNECT:
		{
			snode := c.nodesManager.Get(cmd.Source)
			if snode == nil {
				_ = sendReport(fmt.Sprintf("can't find source node %s", cmd.Source), conn)
			}
			tnode := c.nodesManager.Get(cmd.Target)
			if tnode == nil {
				_ = sendReport(fmt.Sprintf("can't find target node %s", cmd.Target), conn)
			}
			l.Infof("connecting source node[%s] to target node[%s]", cmd.Source, cmd.Target)
			snode.OnConnect(tnode)
		}
	case transport.CREATE:
		{
			t := node.Type(cmd.Params["nodeType"])
			fmt.Println("creating " + t)
			n := node.New(t, cmd.Target)
			c.nodesManager.Add(n)
			switch t {
			case node.KAFKA_SERVER:
				n.Start()
			}
			//deployName := "arc-diagram-server-kafka"
			//err := c.driverClient.CreateDeployment(deployName)
			//if err != nil {
			//	errStr := fmt.Sprintf("create pod failed: %s", err)
			//	l.Error(errStr)
			//	_ = sendReport(errStr, conn)
			//} else {
			//	go func() {
			//		ticker := time.NewTicker(time.Second * 5)
			//		defer ticker.Stop()
			//		for range ticker.C {
			//			fmt.Print("每隔5秒执行任务")
			//			get, err2 := c.driverClient.DeploymentClient().Get(context.TODO(), deployName, metav1.GetOptions{})
			//			if err2 != nil {
			//				log.Println("get deployment failed:", err)
			//			}
			//			report := fmt.Sprintf("deployment status: %s", get.Status)
			//			_ = sendReport(report, conn)
			//		}
			//	}()
			//}
		}
	case transport.EXECUTE:
		{
			node := c.nodesManager.Get(cmd.Target)
			l.Infof("found node %v by ID", node)
			err := node.Execute()
			if err != nil {
				_ = sendReport(err.Error(), conn)
			} else {
				_ = sendReport("", conn)
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
	c := New()
	defer c.Stop()

	c.Start()
}

//func main() {
//	addr := "localhost:9092"
//	topic := "test"
//	//producer := component.NewKafkaProducer(addr, )
//	//defer producer.Close()
//	//producer.Produce("test1")
//
//	consumer := component.NewKafkaConsumer(addr, topic)
//	defer consumer.Close()
//	consumer.Consume()
//}
