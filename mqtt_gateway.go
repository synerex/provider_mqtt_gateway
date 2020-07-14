package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"

	sxmqtt "github.com/synerex/proto_mqtt"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	topic           = flag.String("topic", "#", "MQTT Topic, default(#)")
	mqttsrv         = flag.String("mqtt", "", "MQTT Broker address")
	idlist          []uint64
	spMap           map[uint64]*sxutil.SupplyOpts
	mu              sync.Mutex
	sxServerAddress string
	mqChan          chan *sxmqtt.MQTTRecord
	msgCh           chan mqtt.Message
	loop            = true
)

func init() {
	mqChan = make(chan *sxmqtt.MQTTRecord)
	msgCh = make(chan mqtt.Message)
}

func handleMQTTMessage(clt mqtt.Client) { // Synerex -> MQTT
	// wait until loop
	for loop {
		rcd := <-mqChan
		dt := string(rcd.Record)
		tkn := clt.Publish(rcd.Topic, 0, false, dt)
		ld := fmt.Sprintf("Send MQTT to %s:%s", rcd.Topic, dt)
		log.Print(ld)
		tkn.Wait()
	}
}

func supplyMQTTCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	rcd := &sxmqtt.MQTTRecord{}
	err := proto.Unmarshal(sp.Cdata.Entity, rcd)
	if err == nil { // get MQTT Record
		mqChan <- rcd
	}
}

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock()
	if client.Client != nil {
		client.Client = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.Client == nil {
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.Client = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server\n", sxServerAddress)
	}
	mu.Unlock()
}

func subscribeMQTTSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {                       // make it continuously working..
		client.SubscribeSupply(ctx, supplyMQTTCallback)
		log.Print("Error on subscribe")
		reconnectClient(client)
	}
}

func mqttBrokerHandler(client *sxutil.SXServiceClient) { // MQTT -> Synerex

	for {
		msg := <-msgCh
		log.Printf("From Broker get Topic:%s  Msg: %s\n", msg.Topic(), string(msg.Payload()))
		rcd := sxmqtt.MQTTRecord{
			Topic:  msg.Topic(),
			Time:   ptypes.TimestampNow(),
			Record: msg.Payload(),
		}

		out, _ := proto.Marshal(&rcd) // TODO: handle error
		cont := api.Content{Entity: out}
		smo := sxutil.SupplyOpts{
			Name:  "MQTT_Gateway",
			Cdata: &cont,
		}

		_, nerr := client.NotifySupply(&smo)
		if nerr != nil { // connection failuer with current client
			log.Printf("Connection failure", nerr)
		}
	}
}

// listening MQTT topics.
func listenMQTTBroker() mqtt.Client {
	var myHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
		msgCh <- msg
	}
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + *mqttsrv + ":1883") // currently only 1883 port.
	// opts.SetUsername("username")
	// opts.SetPassword("password")
	//opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
	//	choke <- [2]string{msg.Topic(), string(msg.Payload())}
	//m})

	clt := mqtt.NewClient(opts)

	if token := clt.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("MQTT connection error: %s", token.Error())
	}

	if subscribeToken := clt.Subscribe(*topic, 0, myHandler); subscribeToken.Wait() && subscribeToken.Error() != nil {
		log.Fatalf("MQTT subscribe error: %s", subscribeToken.Error())
	}

	return clt

}

func main() {
	log.Printf("MQTT_Gateway(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	if len(*mqttsrv) == 0 {
		log.Fatal("Please specify MQTT Broker address using -mqtt.")
	}

	channelTypes := []uint32{pbase.MQTT_GATEWAY_SVC}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(*nodesrv, "MQTT-Gateway", channelTypes, nil)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	wg := sync.WaitGroup{} // for syncing other goroutines
	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJson := fmt.Sprintf("{Clt:MQTT-Gtwy}")
	sclient := sxutil.NewSXServiceClient(client, pbase.MQTT_GATEWAY_SVC, argJson)

	go mqttBrokerHandler(sclient) // msgCh -> Synerex
	wg.Add(1)
	clt := listenMQTTBroker() // MQTT -> msgCh

	go handleMQTTMessage(clt) //  mqChan -> MQTT
	wg.Add(1)
	go subscribeMQTTSupply(sclient) // Synerex -> mqChan
	wg.Add(1)

	wg.Wait()
	sxutil.CallDeferFunctions() // cleanup!
	loop = false                // no meaning
}
