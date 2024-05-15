package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"

	"github.com/codecrafters-io/redis-starter-go/app/server"
)

func TestServer_Set(t *testing.T) {
	// Создаем объект сервера для теста
	server := server.NewServer(6379, "master", nil, NewKeyValue())

	// Создаем фейкового клиента для теста
	clientConn, _ := net.Pipe()
	client := NewClient(clientConn)

	// Вызываем метод Set сервера
	err := server.Set(context.Background(), []string{"key", "value"}, client)
	// Проверяем, что ошибки нет
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Проверяем, что клиенту был отправлен статус OK
	expectedResponse := "+OK\r\n"
	if string(client.data) != expectedResponse {
		t.Errorf("Unexpected response. Expected: %s, Got: %s", expectedResponse, client.data)
	}
}

func TestServer_Get(t *testing.T) {
	// Создаем объект сервера для теста
	s := server.NewServer(6379, "master", nil, NewKeyValue())

	// Устанавливаем значение ключа для теста
	s.storage.SetVariable("key", "value", nil)

	// Создаем фейкового клиента для теста
	clientConn, _ := net.Pipe()
	client := NewClient(clientConn)

	// Вызываем метод Get сервера
	server.Get(context.Background(), "key", client)

	// Проверяем, что клиенту было отправлено значение ключа
	expectedResponse := "$5\r\nvalue\r\n"
	if string(client.data) != expectedResponse {
		t.Errorf("Unexpected response. Expected: %s, Got: %s", expectedResponse, client.data)
	}
}

func TestServer_Info(t *testing.T) {
	// Создаем объект сервера для теста
	server := NewServer(6379, "master", nil, NewKeyValue())

	// Создаем фейковое соединение для клиента
	clientConn, serverConn := net.Pipe()
	client := NewClient(clientConn)

	// Запускаем сервер в горутине
	go server.handleConnections(client)

	// Отправляем запрос на info
	serverConn.Write([]byte("info replication\r\n"))

	// Читаем ответ от сервера
	var buf bytes.Buffer
	io.Copy(&buf, serverConn)

	// Проверяем, что получили ожидаемый ответ
	expectedResponse := "+Replication info\r\n"
	if buf.String() != expectedResponse {
		t.Errorf("Unexpected response. Expected: %s, Got: %s", expectedResponse, buf.String())
	}
}
