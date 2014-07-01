package main

import (
	"fmt"
	"code.google.com/p/goprotobuf/proto"
	"net"
	"os"
	"ProtobufTest"
	"encoding/csv"
	"strconv"
)

func main() {
	fmt.Printf("Started ProtoBuf Server")
	c := make(chan *ProtobufTest.TestMessage)
	go func(){
		for{
			message := <-c
			writeValuesTofile(message)

		}
	}()
	//Listen to the TCP port
	listener, err := net.Listen("tcp", "127.0.0.1:2110")
	checkError(err)
	for{
		if conn, err := listener.Accept(); err == nil{
			//If err is nil then that means that data is available for us so we take up this data and pass it to a new goroutine
			go handleProtoClient(conn, c)
		} else{
			continue
		}
	}
}

func handleProtoClient(conn net.Conn, c chan *ProtobufTest.TestMessage){
	fmt.Println("Connection established")
	//Close the connection when the function exits
	defer conn.Close()
	//Create a data buffer of type byte slice with capacity of 4096
	data := make([]byte, 4096)
	//Read the data waiting on the connection and put it in the data buffer
	n,err:= conn.Read(data)
	checkError(err)
	fmt.Println("Decoding Protobuf message")
	//Create an struct pointer of type ProtobufTest.TestMessage struct
	protodata := new(ProtobufTest.TestMessage)
	//Convert all the data retrieved into the ProtobufTest.TestMessage struct type
	err = proto.Unmarshal(data[0:n], protodata)
	checkError(err)
	//Push the protobuf message into a channel
	c <- protodata
}

func writeValuesTofile(datatowrite *ProtobufTest.TestMessage){

	//Retreive client information from the protobuf message
	ClientName := datatowrite.GetClientName()
	ClientDescription := datatowrite.GetDescription()
	ClientID := strconv.Itoa(int(datatowrite.GetClientId()))

	// retrieve the message items list
	items := datatowrite.GetMessageitems()
	fmt.Println("Writing value to CSV file")
	//Open file for writes, if the file does not exist then create it
	file,err := os.OpenFile("CSVValues.csv", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	checkError(err)
	//make sure the file gets closed once the function exists
	defer file.Close()
	//Go through the list of message items, insert them into a string array then write them to the CSV file.
	writer := csv.NewWriter(file)
	for _,item := range items{
		record := []string{ClientID, ClientName, ClientDescription, strconv.Itoa(int(item.GetId())), item.GetItemName(), strconv.Itoa(int(item.GetItemValue())),strconv.Itoa(int(item.GetItemType()))}
		writer.Write(record)
		fmt.Println(record)
	}
	//flush data to the CSV file
	writer.Flush()
	fmt.Println("Finished Writing value to CSV file")
}

func checkError(err error){
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
