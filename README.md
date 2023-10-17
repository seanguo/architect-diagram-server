This is the backend prototype of the arch-diagrams. It's a toy project created while learning React.


# Start
To start the backend server. 
1. First go the driver folder and startup the Kafka and RabbitMQ server by using docker-compose. 
First run 
```
ipconfig getifaddr en0
```
to get your own IP and replace 192.168.31.245 with it in kafka-server.yaml.

Then run
```
docker-compose -f kafka-server.yaml up
```
If you want to try out the RabbitMQ function then open another terminal and run
```
docker-compose -f rabbitmq-server.yaml up
```
2. Run the following command.
```
go run controller.go
```
It will try to create a Websocket server running at port 8080.