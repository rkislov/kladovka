// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type secureStream struct {
	net.Listener
	advertise   net.Addr
	tlsConfig   *tls.Config
	secret      string
	mu          sync.Mutex
	stopped     bool
	connections map[net.Conn]bool
}

type trackedConn struct {
	net.Conn
	stream *secureStream
}

func (c *trackedConn) Close() error {
	c.stream.mu.Lock()
	delete(c.stream.connections, c.Conn)
	c.stream.mu.Unlock()
	return c.Conn.Close()
}
func (s *secureStream) track(c net.Conn) (net.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		c.Close()
		return nil, net.ErrClosed
	}
	if s.connections == nil {
		s.connections = map[net.Conn]bool{}
	}
	s.connections[c] = true
	return &trackedConn{Conn: c, stream: s}, nil
}
func (s *secureStream) Close() error {
	s.mu.Lock()
	s.stopped = true
	all := []net.Conn{}
	for c := range s.connections {
		all = append(all, c)
	}
	s.mu.Unlock()
	e := s.Listener.Close()
	for _, c := range all {
		c.Close()
	}
	return e
}
func signature(secret, label string, nonce []byte) []byte {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(label))
	h.Write(nonce)
	return h.Sum(nil)
}
func (s *secureStream) Addr() net.Addr { return s.advertise }
func (s *secureStream) Accept() (net.Conn, error) {
	for {
		c, e := s.Listener.Accept()
		if e != nil {
			return nil, e
		}
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		nonce := make([]byte, 32)
		_, e = rand.Read(nonce)
		if e == nil {
			_, e = c.Write(nonce)
		}
		proof := make([]byte, 32)
		if e == nil {
			_, e = io.ReadFull(c, proof)
		}
		if e != nil || !hmac.Equal(proof, signature(s.secret, "client", nonce)) {
			c.Close()
			continue
		}
		if _, e = c.Write(signature(s.secret, "server", nonce)); e != nil {
			c.Close()
			continue
		}
		_ = c.SetDeadline(time.Time{})
		return s.track(c)
	}
}
func (s *secureStream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	s.mu.Lock()
	closed := s.stopped
	s.mu.Unlock()
	if closed {
		return nil, net.ErrClosed
	}
	c, e := net.DialTimeout("tcp", string(address), timeout)
	if e != nil {
		return nil, e
	}
	fail := func(e error) (net.Conn, error) { c.Close(); return nil, e }
	_ = c.SetDeadline(time.Now().Add(timeout))
	if s.tlsConfig != nil {
		cfg := s.tlsConfig.Clone()
		host, _, _ := net.SplitHostPort(string(address))
		cfg.ServerName = host
		t := tls.Client(c, cfg)
		if e = t.Handshake(); e != nil {
			return fail(e)
		}
		c = t
	}
	nonce := make([]byte, 32)
	if _, e = io.ReadFull(c, nonce); e != nil {
		return fail(e)
	}
	if _, e = c.Write(signature(s.secret, "client", nonce)); e != nil {
		return fail(e)
	}
	proof := make([]byte, 32)
	if _, e = io.ReadFull(c, proof); e != nil {
		return fail(e)
	}
	if !hmac.Equal(proof, signature(s.secret, "server", nonce)) {
		return fail(errors.New("raft peer authentication failed"))
	}
	_ = c.SetDeadline(time.Time{})
	return s.track(c)
}
func newStream(bind, advertise, cert, key, ca, secret string) (*secureStream, error) {
	address, e := net.ResolveTCPAddr("tcp", advertise)
	if e != nil {
		return nil, e
	}
	var clientConfig *tls.Config
	var serverConfig *tls.Config
	if cert != "" {
		pair, e := tls.LoadX509KeyPair(cert, key)
		if e != nil {
			return nil, e
		}
		pem, e := os.ReadFile(ca)
		if e != nil {
			return nil, e
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("invalid raft CA")
		}
		clientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
		serverConfig = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	}
	listener, e := net.Listen("tcp", bind)
	if e != nil {
		return nil, e
	}
	if serverConfig != nil {
		listener = tls.NewListener(listener, serverConfig)
	}
	return &secureStream{Listener: listener, advertise: address, tlsConfig: clientConfig, secret: secret}, nil
}
