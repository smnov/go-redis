package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/storage"
)

type SlaveServer struct {
	Port       int
	MasterHost string
	MasterPort string
	MasterID   string
	Logger     *slog.Logger
	KeyValue   storage.KeyValue
}

type MasterConn struct {
	conn net.Conn
	data []byte
}

func NewMasterConn(conn net.Conn) MasterConn {
	return MasterConn{
		conn: conn,
		data: make([]byte, 256),
	}
}

func NewSlaveServer(cfg *Config) *SlaveServer {
	return &SlaveServer{
		Port:       cfg.port,
		MasterPort: cfg.replica.MasterPort,
		MasterHost: cfg.replica.MasterHost,
		Logger:     cfg.logger,
		KeyValue:   cfg.kv,
	}
}

func (s SlaveServer) Start() error {
	masterAddr := s.MasterHost + ":" + s.MasterPort
	masterConn, err := net.Dial("tcp", masterAddr)
	host := "0.0.0.0:"
	addr := host + fmt.Sprintf("%d", s.Port)
	if err != nil {
		s.Logger.Error("error trying to set connection to master server...")
		return err
	}
	err = s.createHandshake(context.Background(), masterConn)
	if err != nil {
		s.Logger.Error("error trying to create a handshake with master server...", "error", err)
		return err
	}
	s.Logger.Info("Server started successfully", "port", s.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	masterClient := NewClient(masterConn)
	go s.handleMasterConnection(*masterClient)
	for {
		conn, err := listener.Accept()
		if err != nil {
			s.Logger.Error("error during handle connection", "error", err.Error())
		}

		client := NewClient(conn)
		go s.handleConnections(*client)
	}
}

func (s SlaveServer) handleConnections(cl Client) {
	defer cl.conn.Close()
	s.Logger.Info("New connection accepted", "address", cl.conn.RemoteAddr())
	for {
		n, err := cl.conn.Read(cl.data)
		if err != nil {
			if err == io.EOF {
				s.Logger.Error("connection closed by client")
				return
			}
			s.Logger.Error("error reading from connection: %v", err)
			return
		}
		receivedData := string(cl.data[:n])
		if len(receivedData) > 2 {
			parsedString, err := resp.ParseRequestString(receivedData)
			if err != nil {
				s.Logger.Error(err.Error())
			}
			command := strings.ToLower(parsedString[0])
			ctx := context.Background()
			fmt.Printf("command: %v\n", command)
			switch command {
			// Common commands
			case "echo":
				s.Echo(ctx, parsedString[1:], cl)
			case "info":
				s.Info(ctx, parsedString[1:], cl)
			default:
				s.Ping(ctx, cl)
			}
		}
	}
}

func (s SlaveServer) handleMasterConnection(cl Client) {
	ctx := context.Background()
	defer cl.conn.Close()
	for {
		n, err := cl.conn.Read(cl.data)
		if err != nil {
			if err == io.EOF {
				s.Logger.Error("connection closed by client")
				return
			}
			s.Logger.Error("error reading from connection: %v", err)
			return
		}
		receivedData := string(cl.data[:n])
		receivedData = strings.ReplaceAll(receivedData, `"`, "")
		receivedData = strings.ReplaceAll(receivedData, `\r\n`, "\r\n")
		if len(receivedData) > 2 {
			parsedData, err := resp.ParseRequestString(receivedData)
			if err != nil {
				s.Logger.Error("error", "error", err)
			}
			command := strings.ToLower(parsedData[0])
			if strings.HasPrefix(command, "REDIS") {
				err := s.SaveRDBFile(command)
				if err != nil {
					s.Logger.Error("Cannot save rdb file")
					cl.conn.Close()
				}
			}
			switch command {
			case "set":
				s.Set(ctx, parsedData[1:], cl)
			case "info":
				s.Info(ctx, parsedData[1:], cl)
			}
		} else {
			s.Logger.Error("Received zero length data")
		}
		cl.data = make([]byte, 256)
	}
}

func (s SlaveServer) SaveRDBFile(f string) error {
	err := os.WriteFile("app/storage/replica/db.rdb", []byte(f), 0777)
	if err != nil {
		return err
	}
	return nil
}

// Creates connection with master server
func (s SlaveServer) createHandshake(ctx context.Context, conn net.Conn) error {
	err := s.PingMasterServer(ctx, conn)
	if err != nil {
		return err
	}
	err = s.ReplconfMasterServer(ctx, conn)
	if err != nil {
		return err
	}
	err = s.PsyncMasterServer(ctx, conn)
	if err != nil {
		return err
	}
	return nil
}

func (s SlaveServer) PingMasterServer(ctx context.Context, conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}

	_, err := conn.Write([]byte(PingCommand))
	if err != nil {
		return err
	}
	expectedResponse := []byte("+PONG\r\n")
	response := make([]byte, len(expectedResponse))
	s.responseLoop(ctx, conn, response, expectedResponse)
	return nil
}

func (s SlaveServer) responseLoop(ctx context.Context, conn net.Conn, response, expectedResponse []byte) error {
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
				return nil
			}
		}
	}
}

func (s SlaveServer) ReplconfMasterServer(ctx context.Context, conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}

	_, err := conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
	if err != nil {
		return err
	}
	expectedResponse := []byte("+OK\r\n")
	response := make([]byte, len(expectedResponse))
	err = s.responseLoop(ctx, conn, response, expectedResponse)
	if err != nil {
		return err
	}
	_, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if err != nil {
		return err
	}
	expectedResponse = []byte("+OK\r\n")
	response = make([]byte, len(expectedResponse))
	err = s.responseLoop(ctx, conn, response, expectedResponse)
	if err != nil {
		return err
	}
	return nil
}

func (s SlaveServer) PsyncMasterServer(ctx context.Context, conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}
	// This command tells master server that it doesn't have any data yet,
	// and needs to be fully resynchronized.
	cmd := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	_, err := conn.Write([]byte(cmd))
	if err != nil {
		return err
	}
	expectedPrefix := "+FULLRESYNC "
	expectedSuffix := " 0\r\n"
	var response []byte
	buffer := make([]byte, 1)
	for {
		_, err := conn.Read(buffer)
		if err != nil {
			return err
		}
		response = append(response, buffer[0])
		if len(response) >= len(expectedPrefix) && string(response[len(response)-len(expectedSuffix):]) == expectedSuffix {
			break
		}
	}

	startIndex := len(expectedPrefix)
	endIndex := len(response) - len(expectedSuffix)
	masterID := string(response[startIndex:endIndex])

	s.MasterID = masterID
	fmt.Printf("s.MasterID: %v\n", s.MasterID)

	return nil
}

func (s SlaveServer) Ping(context.Context, Client) (bool, error) {
	return true, nil
}

func (s SlaveServer) Echo(ctx context.Context, input []string, cl Client) error {
	resString := resp.CreateBulkStringFromArray(input)
	b := []byte(resString)
	_, err := cl.conn.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func (s SlaveServer) HandleRDBFile(input []string) {
	fmt.Printf("input: %v\n", input)
}

func (s SlaveServer) Set(ctx context.Context, input []string, cl Client) error {
	key := input[0]
	value := input[1]
	args := make(map[string]string)
	if len(input) > 3 {
		if input[2] == "px" { // px allows setting a key with expiry
			args["px"] = input[3]
		}
	}
	s.KeyValue.SetVariable(key, value, args)
	_, err := cl.conn.Write([]byte(StatusOK))
	if err != nil {
		return err
	}
	cl.conn.Write([]byte(StatusOK))
	return nil
}

func (s SlaveServer) Get(context.Context, string, Client) {
}

func (s SlaveServer) Info(ctx context.Context, args []string, cl Client) {
	cmd := args[0]
	switch cmd {
	case "replication":
		{
			role := "role:" + s.Role()
			b := resp.CreateBulkString(role)
			_, err := cl.conn.Write([]byte(b))
			if err != nil {
				s.Logger.Error("error while handling info command", "error", err)
			}
		}
	}
}

func (s SlaveServer) CreateHandshake() {
}

func (s SlaveServer) Role() string {
	return "slave"
}
