package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

const (
	connHost = "localhost"
	connPort = "1434"
	connType = "tcp"
)

//Our database is server!
func ServeRequests() {
	fmt.Println("Starting DATABASE " + connHost + ":" + connPort)
	l, _ := net.Listen(connType, connHost+":"+connPort)

	defer l.Close()

	for {
		c, _ := l.Accept()
		fmt.Println("Client with addr" + c.RemoteAddr().String() + " Connected.")

		go handleConnection(c)
	}
}

func handleQuery(query string) {
	fmt.Printf("Query %v\n", query)
}

// Client Connected time to shine!
func handleConnection(conn net.Conn) {
	buffer, err := bufio.NewReader(conn).ReadBytes('\n')

	if err != nil {
		fmt.Printf("err %v\n", err)
		conn.Close()
		return
	}

	clientQuery := string(buffer[:len(buffer)-1])
	log.Println("Client message:", clientQuery)

	conn.Write(buffer)

	handleConnection(conn)
}
