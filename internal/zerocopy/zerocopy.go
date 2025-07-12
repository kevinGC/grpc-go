//go:build linux

/*
 *
 * Copyright 2014 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package zerocopy provides access to TCP sockets that utilize RX
// (TCP_ZEROCOPY_RECEIVE) and TX (MSG_ZEROCOPY) zerocopy. These sockets can
// increase throughput, decrease latency, and reduce memory bandwidth pressure.
//
// The types provided may perform worse than net.TCPConn under certain
// circumstances, notably when the number of goroutines is very high. The Go
// runtime uses an optimized [polling mechanism], tightly integrated into the
// runtime scheduler, to reduce thread count and context switching. Custom
// net.Conn implementers can't utilize that mechanism, and so we fallback to
// traditional goroutine scheduling.
//
// [polling scheme]: https://pkg.go.dev/internal/poll
package zerocopy

import (
	"errors"
	"fmt"
	"net"
	"os"
)

var _ net.Conn = (*TCPConn)(nil)

// TCPConn is a TCP net.Conn that performs RX and TX via zerocopy.
type TCPConn struct {
	// The underlying TCP connection.
	*net.TCPConn

	// The file corresponding to the TCP connection. Set during
	// initialization to avoid calling net.TCPConn.Fd (and thus dup)
	// multiple times.
	file *os.File

	// Whether RX zerocopy is enabled.
	rx bool

	// Whether TX zerocopy is enabled.
	tx bool
}

// FromTCPConn returns a TCPConn that supports zerocopy. Callers can selectively
// enable RX and/or TX zerocopy.
func FromTCPConn(conn *net.TCPConn, rx, tx bool) (TCPConn, error) {
	file, err := conn.File()
	if err != nil {
		return TCPConn{}, fmt.Errorf("failed to get for TCPConn: %v", err)
	}
	return TCPConn{
		TCPConn: conn,
		file:    file,
		rx:      rx,
		tx:      tx,
	}, nil
}

// FromConn is like FromTCPConn, but for convenience accepts net.Conn. It
// returns an error if the underlying type is not *net.TCPConn
func FromConn(conn net.Conn, rx, tx bool) (TCPConn, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return TCPConn{}, fmt.Errorf("zerocopy is only supported over TCP, but found %T", conn)
	}
	return FromTCPConn(tcpConn, rx, tx)
}

// Read implements net.Conn.Read.
func (cn *TCPConn) Read(b []byte) (n int, err error) {
	return cn.TCPConn.Read(b)
}

// Write implements net.Conn.Write.
func (cn *TCPConn) Write(b []byte) (n int, err error) {
	return cn.TCPConn.Write(b)
}

// Close implements net.Conn.Close.
func (cn *TCPConn) Close() error {
	return errors.Join(cn.TCPConn.Close(), cn.file.Close())
}
