//go:build linux

/*
 *
 * Copyright 2025 gRPC authors.
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
	"fmt"
	"net"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// TODO: Limit mss for testing? Like tcp_mmap -M?
// TODO: Check lo mtu is large enough
// TODO: errqueue
// TODO: TCP_MAXSEG, which might have to be set before connections establishment
// and thus force me to call socket() manually.
// TODO: check page size too?

// TestLocal sets up a client/server pair using both RX and TX zerocopy. It
// verifies that a minimum threshold of bytes are successfully received via
// zerocopy. It just verifies the ability to transfer raw bytes, not the extra
// protobufs/framing/etc that gRPC puts onto the transport.
//
// The client sends 4KiB payloads via tx0cp, while the server receives via
// rx0cp.
//
// This test requires a lot of environement setup, and so should only be invoked
// manually.
func TestLocal(t *testing.T) {
	const (
		timeout   = 5 * time.Second
		port      = 2401
		chunkSize = 512 * 1024
		totalSize = 8 * 1024 * 1024 * 1024
	)
	var addr = fmt.Sprintf("localhost:%d", port)

	// Start the server in another goroutine.
	go func() {
		listener, err := net.Listen("tcp4", addr)
		if err != nil {
			t.Fatalf("Failed to listen: %v", err)
		}
		basicConn, err := listener.Accept()
		if err != nil {
			t.Fatalf("Failed to accept: %v", err)
		}

		// Initialize bites needed for RX zerocopy.
		conn, err := FromConn(basicConn, true /* rx */, false /* tx */)
		if err != nil {
			t.Fatalf("Failed to create zerocopy connection on the server: %v", err)
		}

		// TODO: The server! BufferedReader!
		reader, err := NewBufferedReader(conn, chunkSize)
	}()

	// Establish a connection with a timeout. Retry if necessary.
	var basicConn net.Conn
	var err error
	start := time.Now()
	for {
		// Prefer IPv4 as it doesn't need quite as large an MTU as IPv6.
		// TODO: Does this have to be blocking?
		basicConn, err = net.DialTimeout("tcp4", addr)
		if time.Since(start) < timeout {
			t.Fatalf("Failed to establish a connection before timeout expiration. Last error: %v", err)
		}
	}

	// TODO: Uhh... most of this stuff should be *inside* zerocopy.Conn.

	// Initialize bits needed for TX zerocopy.
	conn, err := FromConn(basicConn, false /* rx */, true /* tx */)
	if err != nil {
		t.Fatalf("Failed to create zerocopy connection: %v", err)
	}

	chunkBuf, err := unix.Mmap(
		0,                              // FD
		0,                              // Offset
		chunkSize,                      // Length
		unix.PROT_READ|unix.PROT_WRITE, // Prot
		unix.MAP_PRIVATE|unix.MAP_ANONYMOUS|unix.MAP_POPULATE, // Flags
	)
	if err != nil {
		t.Fatalf("Failed mmap send buffer: %v", err)
	}
	defer unix.Munmap(chunkBuf)

	// Write in chunks.
	for sent := 0; sent < totalSize; {
		written, err := send(conn.file.Fd(), chunkBuf[sent%chunkSize:], unix.MSG_ZEROCOPY)
		if err != nil {
			if err == unix.EINTR || err == unix.EAGAIN {
				// TODO: Just use a blocking socket
				time.Sleep(time.Millisecond)
				continue
			}
			t.Fatalf("Failed to send after %d bytes: %v", sent, err)
		}
		sent += written
	}

	// TODO: Check % zc'd

	// Success!
	return

}

// send calls the send syscall, but unlike unix.Send has the decency to return
// the nubmer of bytes written.
func send(fd int, buf []byte, flags int) (int, error) {
	r1, _, err := unix.Syscall6(
		SYS_SEND,
		uintptr(fd),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(len(buf)),
		uintptr(flags),
		0, // Unused
		0, // Unused
	)
	return r1, err
}
