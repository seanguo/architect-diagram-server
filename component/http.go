package component

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
)

type HttpClient struct {
	address []string
	client  *http.Client
}

func (c *HttpClient) Produce(content string) error {
	var jsonData = []byte(fmt.Sprintf(`{
		"content": %s,
	}`, content))
	l.Infof("sending request to %s", c.address[0])
	req, err := http.NewRequest("POST", c.address[0], bytes.NewBuffer(jsonData))
	if err != nil {
		l.Errorf("can't create POST request: %s", err)
		return err
	}
	// ...
	req.Header.Add("Cache-Control", "no-cache")
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode > 200 {
		return fmt.Errorf("got error response: %v", resp)
	}
	return nil
}

func (c *HttpClient) initialize() error {
	c.client = &http.Client{}
	return nil
}

func NewHttpClient(address string) *HttpClient {
	client := &HttpClient{
		address: []string{address},
	}
	err := client.initialize()
	if err != nil {
		return nil
	}
	return client
}

func (c *HttpClient) Close() {
	if c.client != nil {
		c.client.CloseIdleConnections()
	}
}

type HttpServer struct {
	port int
}

func NewHttpServer(port int, onRequest func(body []byte)) *HttpServer {
	handler := func(w http.ResponseWriter, req *http.Request) {
		reqBody, err := io.ReadAll(req.Body)
		if onRequest != nil {
			onRequest(reqBody)
		}
		if err != nil {
			io.WriteString(w, fmt.Sprintf("%s\n", err))
		} else {
			io.WriteString(w, "Success\n")
		}
	}
	mux := http.NewServeMux()

	mux.HandleFunc("/", handler)

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(port), mux))
	return nil
}
