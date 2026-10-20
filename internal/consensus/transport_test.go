// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package consensus

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/hashicorp/raft"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTransportAuthenticationAndTLS(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "loopback", true: "TLS"}[secure], func(t *testing.T) {
			cert, key, ca := "", "", ""
			if secure {
				cert, key, ca = testCertificate(t)
			}
			address := tcpAddress(t)
			server, e := newStream(address, address, cert, key, ca, "correct-cluster")
			if e != nil {
				t.Fatal(e)
			}
			defer server.Close()
			accepted := make(chan net.Conn, 1)
			go func() {
				c, e := server.Accept()
				if e == nil {
					accepted <- c
				}
			}()
			badAddr := tcpAddress(t)
			bad, e := newStream(badAddr, badAddr, cert, key, ca, "wrong-cluster")
			if e != nil {
				t.Fatal(e)
			}
			defer bad.Close()
			if c, e := bad.Dial(raft.ServerAddress(address), time.Second); e == nil {
				c.Close()
				t.Fatal("unauthenticated raft stream accepted")
			}
			goodAddr := tcpAddress(t)
			good, e := newStream(goodAddr, goodAddr, cert, key, ca, "correct-cluster")
			if e != nil {
				t.Fatal(e)
			}
			defer good.Close()
			c, e := good.Dial(raft.ServerAddress(address), time.Second)
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			select {
			case peer := <-accepted:
				peer.Close()
			case <-time.After(2 * time.Second):
				t.Fatal("authenticated stream not accepted")
			}
			good.Close()
			if c, e := good.Dial(raft.ServerAddress(address), time.Second); e == nil {
				c.Close()
				t.Fatal("closed transport still dialed")
			}
		})
	}
}
func testCertificate(t *testing.T) (string, string, string) {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "loopback-test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	cert, e := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := x509.MarshalECPrivateKey(key)
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if e = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert}), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: raw}), 0600); e != nil {
		t.Fatal(e)
	}
	return certPath, keyPath, certPath
}
