package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

// Starts the server
func (s *Server) Run() {
	addr := "0.0.0.0:" + s.listenAddr
	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	role := (*s.info)["role"] // server role
	if role == "slave" {
		host := (*s.info)["master_host"]
		port := (*s.info)["master_port"]
		masterAddr := host + ":" + port
		masterConn, err := net.Dial("tcp", masterAddr)
		if err != nil {
			s.logger.Error("error trying to set connection to master server...")
			panic(err)
		}
		err = CreateHandshake(context.Background(), masterConn)
		if err != nil {
			s.logger.Error("error trying to create a handshake with master server...")
			panic(err)
		}
	}
	s.logger.Info("Server started successfully", "port", s.listenAddr)
	defer l.Close()
	for { // loop that accepts new connections and handles them
		conn, err := l.Accept()
		if err != nil {
			s.logger.Error("error during handle connection", "error", err.Error())
		}
		client := NewClient(conn)
		go s.handleConnections(client)
	}
}

// Creates connection with master server
func CreateHandshake(ctx context.Context, conn net.Conn) error {
	err := PingMasterServer(ctx, conn)
	if err != nil {
		return err
	}
	err = ReplconfMasterServer(ctx, conn)
	if err != nil {
		return err
	}
	err = PsyncMasterServer(ctx, conn)
	if err != nil {
		return err
	}
	return nil
}

func PingMasterServer(ctx context.Context, conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}

	_, err := conn.Write([]byte(PingCommand))
	if err != nil {
		return err
	}
	expectedResponse := []byte("+PONG\r\n")
	response := make([]byte, len(expectedResponse))
	responseLoop(ctx, conn, response, expectedResponse)
	return nil
}

func responseLoop(ctx context.Context, conn net.Conn, response, expectedResponse []byte) error {
	for {
		select {
		case <-ctx.Done():
			return errors.New("timeout")
		default:
			_, err := conn.Read(response)
			if err != nil {
				return err
			}
			if bytes.Equal(response, expectedResponse) {
				fmt.Printf("response: %v\n", response)
				return nil
			}
		}
	}
}

func ReplconfMasterServer(ctx context.Context, conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}

	_, err := conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
	if err != nil {
		return err
	}
	expectedResponse := []byte("+OK\r\n")
	response := make([]byte, len(expectedResponse))
	err = responseLoop(ctx, conn, response, expectedResponse)
	if err != nil {
		return err
	}
	_, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if err != nil {
		return err
	}
	expectedResponse = []byte("+OK\r\n")
	response = make([]byte, len(expectedResponse))
	err = responseLoop(ctx, conn, response, expectedResponse)
	if err != nil {
		return err
	}
	return nil
}

func PsyncMasterServer(ctx context.Context, conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}
	// This command tells master server that it doesn't have any data yet,
	// and needs to be fully resynchronized.
	cmd := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	fmt.Printf("cmd: %v\n", cmd)
	_, err := conn.Write([]byte(cmd))
	if err != nil {
		return err
	}
	expectedResponse := []byte(StatusOK)
	response := make([]byte, len(expectedResponse))
	responseLoop(ctx, conn, response, expectedResponse)
	return nil
}

// Allows to check if connection is ok
func (s *Server) Ping(ctx context.Context, cl *Client) {
	_, err := cl.conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		s.logger.Error(err.Error())
		return
	}
	cl.data = make([]byte, 1024)
}

// Implementation of "echo" command that resends user message
func (s *Server) Echo(ctx context.Context, input []string, cl *Client) error {
	resString := resp.CreateBulkStringFromArray(input)
	b := []byte(resString)
	_, err := cl.conn.Write(b)
	if err != nil {
		return err
	}
	cl.data = make([]byte, 1024)
	return nil
}

// Set given key value pair into storage
func (s *Server) Set(ctx context.Context, input []string, cl *Client) error {
	key := input[0]
	value := input[1]
	args := make(map[string]string)
	if len(input) > 3 {
		if input[2] == "px" { // px allows setting a key with expiry
			args["px"] = input[3]
		}
	}
	s.storage.SetVariable(key, value, args)
	cl.conn.Write([]byte(StatusOK))
	cl.data = make([]byte, 1024)
	return nil
}

// Get value from key value storage
func (s *Server) Get(ctx context.Context, key string, cl *Client) {
	var arr []string
	v, err := s.storage.GetVariable(key)
	if err != nil {
		s.logger.Error("error while getting value", "error", err.Error())
		cl.conn.Write([]byte(nullBulkString))
		return
	}
	arr = append(arr, v)
	ecnodedV := resp.CreateBulkStringFromArray(arr)
	_, err = cl.conn.Write([]byte(ecnodedV))
	if err != nil {
		s.logger.Error("error while writing response", "error", err.Error())
		return
	}
	cl.data = make([]byte, 1024)
}

func (s *Server) InitialMessage(cl *Client) {
	_, err := cl.conn.Write([]byte("+Connected!\r\n"))
	if err != nil {
		s.logger.Error(err.Error())
		return
	}
	cl.data = make([]byte, 1024)
}

// Info command
func (s *Server) Info(ctx context.Context, args []string, cl *Client) {
	cmd := args[0]
	switch cmd {
	case "replication":
		{
			role := (*s.info)["role"]
			id := (*s.info)["master_replid"]
			offset := (*s.info)["master_repl_offset"]
			role = "role:" + role
			id = "master_replid:" + id
			offset = "master_repl_offset:" + offset
			b := resp.CreateBulkString(role + "\r\n" + id + "\r\n" + offset + "\r\n")
			_, err := cl.conn.Write([]byte(b))
			if err != nil {
				s.logger.Error("error while handling info command", "error", err)
			}
		}
	}
}

func (s *Server) HandleReplconfCommand(ctx context.Context, args []string, cl *Client) {
	if (*s.info)["role"] == "master" {
		s.WriteResponse(cl, "+OK\r\n")
	}
}

func (s *Server) HandlePsyncCommand(ctx context.Context, args []string, cl *Client) {
	if (*s.info)["role"] == "master" {
		repl_id := (*s.info)["master_replid"]
		response := fmt.Sprintf("+FULLRESYNC %s 0\r\n", repl_id)
		err := s.WriteResponse(cl, response)
		if err != nil {
			s.logger.Error("error handling psync command", "error", err)
		}
		s.SendRDBFile(cl)
	}
}

func (s *MasterServer) HandleMasterCommand(ctx context.Context, args []string, cl *Client) {
}

func (s *Server) SendRDBFile(cl *Client) {
	if (*s.info)["role"] == "master" {

		emptyRDBFile, err := rdb.DecodeDBFile()
		if err != nil {
			s.logger.Error("error reading rdb file", "error", err)
		}
		base64ToBinary, err := base64.StdEncoding.DecodeString(string(emptyRDBFile))
		if err != nil {
			s.logger.Error("error decoding base64 string", "error", err)
		}
		response := "$" + strconv.Itoa(len(base64ToBinary)) + "\r\n" + string(base64ToBinary)
		cl.conn.Write([]byte(response))
	}
}

// Write Response
func (s *Server) WriteResponse(cl *Client, data string) error {
	_, err := cl.conn.Write([]byte(data))
	if err != nil {
		return err
	}
	cl.data = make([]byte, 1024)
	return nil
}

// Handles client connection in own goroutine
func (s *Server) handleConnections(cl *Client) {
	defer cl.conn.Close()
	s.logger.Info("New connection accepted", "address", cl.conn.RemoteAddr())
	for {
		n, err := cl.conn.Read(cl.data)
		if err != nil {
			if err == io.EOF {
				s.logger.Error("connection closed by client")
				break
			}
			s.logger.Error("error reading from connection: %v", err)
			return
		}
		received := string(cl.data[:n])
		if len(received) > 2 {
			parsedString, err := resp.ParseRequestString(received)
			if err != nil {
				s.logger.Error(err.Error())
			}
			command := strings.ToLower(parsedString[0])
			ctx := context.Background()
			fmt.Printf("command: %v\n", command)
			switch command {
			// Common commands
			case "command":
				s.InitialMessage(cl)
			case "set":
				s.Set(ctx, parsedString[1:], cl)
			case "get":
				s.Get(ctx, parsedString[1], cl)
			case "echo":
				s.Echo(ctx, parsedString[1:], cl)
			case "info":
				s.Info(ctx, parsedString[1:], cl)
			// Master server commands
			case "replconf":
				s.HandleReplconfCommand(ctx, parsedString[1:], cl)
			case "psync":
				s.HandlePsyncCommand(ctx, parsedString[1:], cl)

			default:
				s.Ping(ctx, cl)
			}
		}
	}
}
