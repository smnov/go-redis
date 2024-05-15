package server

const (
	StatusOK    = "$2\r\nOK\r\n"
	PingCommand = "*1\r\n$4\r\nping\r\n"
	PongCommand = "+PONG\r\n"
)

type NetworkError struct{}

func (e *NetworkError) Error() string {
	return ""
}
