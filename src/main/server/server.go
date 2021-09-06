package server

import (
	"bufio"
	"cs/src/main/manager"
	"cs/src/main/parser"
	"fmt"
	"log"
	"net"
	"strings"
)

//Our database is server!
const (
	connHost = "localhost"
	connPort = "1434"
	connType = "tcp"
)

func ServeRequests(fs manager.TableData) {

	fmt.Println("Starting DATABASE " + connHost + ":" + connPort)
	l, _ := net.Listen(connType, connHost+":"+connPort)
	defer l.Close()

	for {
		c, _ := l.Accept()
		fmt.Println("Client with addr" + c.RemoteAddr().String() + " Connected.")

		go handleConnection(c, fs)
	}
}

// Client Connected time to shine!
func handleConnection(conn net.Conn, fs manager.TableData) {
	buffer, err := bufio.NewReader(conn).ReadBytes('\n')

	if err != nil {
		fmt.Printf("err %v\n", err)
		conn.Close()
		return
	}

	clientQuery := string(buffer[:len(buffer)-1])
	log.Println("Client message:", clientQuery)
	response := handleQuery(clientQuery, fs)
	conn.Write([]byte(response))

	handleConnection(conn, fs)
}

// Main method for handling client query
func handleQuery(query string, fs manager.TableData) string {
	fmt.Printf("Query %v\n", query)
	tokens := parser.FullTokenize(query)
	if strings.ToUpper(tokens[0]) == "INDEX" && strings.ToUpper(tokens[1]) == "BY" {
		variableType := "STRING"
		if len(tokens) >= 2 {
			variableType = tokens[3]
		}
		if b := handleIndexBy(tokens[2], fs, strings.ToUpper(variableType)); b {
			return "Index By successful"
		} else {
			return "Didn't index by " + tokens[2]
		}
	} else {
		q, _ := parser.Parse(query)
		q.PrintQuery()

	}
	return "NOT COMMAND"
}

func handleIndexBy(column string, fs manager.TableData, indexType string) bool {

	if indexType == "STRING" {
		manager.IndexBy(column, "data/myFile/"+column, fs, manager.StringType)
	} else {
		manager.IndexBy(column, "data/myFile/"+column, fs, manager.IntType)
	}
	return true
}
