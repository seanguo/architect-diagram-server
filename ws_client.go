package main

//var addr = flag.String("addr", "localhost:8080", "http service address")

//func main2() {
//	flag.Parse()
//	log.SetFlags(0)
//
//	interrupt := make(chan os.Signal, 1)
//	signal.Notify(interrupt, os.Interrupt)
//
//	u := url.URL{Scheme: "ws", Host: *addr, Path: "/echo"}
//	log.Printf("connecting to %s", u.String())
//
//	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
//	if err != nil {
//		log.Fatal("dial:", err)
//	}
//	defer c.Close()
//
//	done := make(chan struct{})
//
//	go func() {
//		defer close(done)
//		for {
//			_, message, err := c.ReadMessage()
//			if err != nil {
//				log.Println("read:", err)
//				return
//			}
//			log.Printf("recv: %s", message)
//		}
//	}()
//
//	ticker := time.NewTimer(time.Second)
//	defer ticker.Stop()
//
//	for {
//		select {
//		case <-done:
//			return
//		case t := <-ticker.C:
//			msg, err := json.Marshal(&transport.Message{
//				Timestamp: t,
//				ID:        "test",
//				Content:   "test content",
//			})
//			if err != nil {
//				log.Println("decode:", err)
//				return
//			}
//
//			err = c.WriteMessage(websocket.TextMessage, msg)
//			if err != nil {
//				log.Println("write:", err)
//				return
//			}
//		case <-interrupt:
//			log.Println("interrupt")
//
//			// Cleanly close the connection by sending a close message and then
//			// waiting (with timeout) for the server to close the connection.
//			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
//			if err != nil {
//				log.Println("write close:", err)
//				return
//			}
//			select {
//			case <-done:
//			case <-time.After(time.Second):
//			}
//			return
//		}
//	}
//}
