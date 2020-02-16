package cluster

import (
	"io"
	"log"
	"time"

	pb "github.com/go-sif/sif/internal/rpc"
)

type logServer struct {
}

// createLogServer creates a log server
func createLogServer() *logServer {
	return &logServer{}
}

// Log messages to the console coming from workers
func (s *logServer) Log(stream pb.LogService_LogServer) error {
	var count int32
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			// Then we're out of messages to print and no errors have occurred, so Ack
			return stream.SendAndClose(&pb.MLogMsgAck{Time: time.Now().Unix(), Count: count})
		} else if err != nil {
			return err
		}
		count++
		log.Printf("%s: level [%d]: %s", message.GetSource(), message.GetLevel(), message.GetMessage())
	}
}
