package server

import (
	"net"
	"github.com/sajjad-MoBe/CloudKVStore/node/src/internal/storage"

	"log"
	"fmt"

)

type Server struct {
	store    *storage.SinglePartitionStore
	listener net.Listener
	address  string
}

func NewServer(store *storage.SinglePartitionStore, address string) *Server {
	return &Server{
		store:   store,
		address: address,
	}
}


func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.address, err)
	}
	log.Printf("Node server listening on %s", s.address)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue 
		}
		log.Printf("Accepted connection from %s", conn.RemoteAddr())
		go s.handleConnection(conn)
	}
	// return nil
}


func (s *Server) Stop() {
	if s.listener != nil {
		s.listener.Close()
	}
	log.Println("Server stopped.")
}

func (s *Server) handleConnection(conn net.Conn) {
   defer conn.Close()
   log.Printf("Handling connection from %s", conn.RemoteAddr())
   // TODO: Implement read loop, decode, process, encode, send response ... later 
   // for now, we just read a message and print it
   buffer := make([]byte, 1024)
   n, err := conn.Read(buffer)
   if err != nil {
        log.Printf("Error reading from connection: %v", err)
        return
   }
    log.Printf("Received %d bytes: %s", n, string(buffer[:n]))
    // dummy response for now
    _, _ = conn.Write([]byte("Message received by node (dummy response)\n"))
}
