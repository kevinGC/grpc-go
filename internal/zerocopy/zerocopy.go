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
	"io"
	"log"
	"net"
	"os"
	"runtime/debug"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
	"golang.org/x/time/rate"
)

var dbg bool = false

func Printf(format string, v ...any) {
	if dbg {
		log.Printf(format, v...)
	}
}

// rateLimiter prevents logspam.
var rateLimiter = rate.Sometimes{
	Interval: time.Second,
}

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
	// TODO: TX will have to check page alignment
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
	if !cn.rx {
		return cn.TCPConn.Read(b)
	}
	Printf("====================================================================")
	Printf("reading %d bytes", len(b))
	if dbg {
		debug.PrintStack()
	}
	return cn.TCPConn.Read(b)
}

// Write implements net.Conn.Write.
func (cn *TCPConn) Write(b []byte) (n int, err error) {
	if !cn.tx {
		return cn.TCPConn.Write(b)
	}
	return cn.TCPConn.Write(b)
}

// Close implements net.Conn.Close.
func (cn *TCPConn) Close() error {
	return errors.Join(cn.TCPConn.Close(), cn.file.Close())
}

// A Reader implements a buffered io.Reader for
//
// TODO: We should internally have a bufio.Reader? Or do we need to roll our
// own?
// TODO: SO_RCVLOWAT in Read?
// TODO: How do the copybuf and skip hint interact?
// TODO: When to call unmap
type BufferedReader struct {
	conn *TCPConn

	// Buffers backing payload data.
	mmappedStorage []byte
	copybufStorage []byte

	// These overlap with with their *Storage counterparts and are used to
	// track offset and length into underlying storage.
	mmapped []byte
	copybuf []byte

	// skipHint stores the number of bytes to be read via standard recv.
	// Zerocopy should always prefer the copybuf to this, but that's not
	// guaranteed so we handle it.
	skipHint int
}

func NewBufferedReader(conn *TCPConn, size int) (*BufferedReader, error) {
	// return bufio.NewReaderSize(conn, size)

	//
	// PROBLEM: Socket is nonblocking. Do we have to do this read/epoll/read
	// song and dance?
	//

	// We manage 2 buffers: the mmap buffer and the copybuf. We have to be
	// careful to track which buffer the offset is pointing to. Presumably
	// the right way to do this is to make a single TCP_ZEROCOPY_RECEIVE
	// syscall, then not make another until every last byte from both
	// buffers is consumed.
	var err error
	rounded := roundUpToPage(size)
	br := BufferedReader{conn: conn}
	br.mmappedStorage, err = unix.Mmap(
		int(conn.file.Fd()), // File descriptor
		0,                   // Offset
		rounded,             // Length
		unix.PROT_READ,      // Protection
		unix.MAP_SHARED,     // Flags
	)
	if err != nil {
		return nil, err
	}
	if len(br.mmappedStorage) != rounded {
		return nil, fmt.Errorf("bad mmap: got %d, but expected %d", len(br.mmappedStorage), rounded)
	}
	br.mmapped = br.mmappedStorage[:0]
	log.Printf("mmappedStorage starts at address: %p", &br.mmappedStorage[0])

	br.copybufStorage = make([]byte, 4095)
	br.copybuf = br.copybufStorage[:0]
	log.Printf("copybufStorage starts at address: %p", &br.copybufStorage[0])

	return &br, nil
}

// tcpZerocopyReceive is struct tcp_zerocopy_receive in
// include/uapi/linux/tcp.h. See that file for up-to-date field descriptions.
type tcpZerocopyReceive struct {
	address        uint64
	length         uint32
	recvSkipHint   uint32
	inq            uint32
	err            int32
	copybufAddress uint64
	copybufLen     int32
	flags          uint32
	msgControl     uint64
	msgControllen  uint64
	msgFlags       uint32
	reserved       uint32
}

func (br *BufferedReader) Read(dst []byte) (int, error) {
	// Always return buffered, unread bytes first to avoid making extra
	// syscalls.
	// log.Printf("br.Read")
	n, err := br.readBuffered(dst)
	if n != 0 || err != nil {
		return n, err
	}

	// Perform zerocopy RX.
	// log.Printf("br.Read: going to call getsockopt")
	zc := tcpZerocopyReceive{
		address:        uint64(uintptr(unsafe.Pointer(&br.mmappedStorage[0]))),
		length:         uint32(len(br.mmappedStorage)),
		copybufAddress: uint64(uintptr(unsafe.Pointer(&br.copybufStorage[0]))),
		copybufLen:     int32(len(br.copybufStorage)),
	}
	zcSize := unsafe.Sizeof(tcpZerocopyReceive{})
	_, _, errno := unix.Syscall6(
		unix.SYS_GETSOCKOPT,
		br.conn.file.Fd(), // TODO: Store the fd so we don't have to keep calling this?
		unix.SOL_TCP,
		unix.TCP_ZEROCOPY_RECEIVE,
		uintptr(unsafe.Pointer(&zc)),
		uintptr(unsafe.Pointer(&zcSize)),
		0, // Unused
	)
	if errno != 0 && errno != unix.EINTR {
		log.Printf("zerocopy errno: %d", errno)
		return 0, errno // TODO: allocates?
	}

	// Go creates nonblocking sockets, so getting no data only
	// indicates EOF after a return from epoll.
	if n := zc.length + uint32(zc.copybufLen) + zc.recvSkipHint; n != 0 {
		// Account for mmapped, copybuf, and skip hint data.
		br.mmapped = br.mmappedStorage[:zc.length]
		br.copybuf = br.copybufStorage[:zc.copybufLen]
		br.skipHint = int(zc.recvSkipHint)
		// log.Printf("br.Read: returning %d from first getsockopt", int(n))
		// rateLimiter.Do(func() {
		// 	log.Printf("returning from first getsockopt")
		// })
		return br.readBuffered(dst)
	}

	epfd, err := unix.EpollCreate1(0 /* flags */) // TODO: put in struct, not here
	if err != nil {
		panic("create")
		return 0, fmt.Errorf("zerocopy.BufferedReader epoll_create: %w", err)
	}
	defer unix.Close(epfd)
	ev := unix.EpollEvent{
		Events: unix.EPOLLIN,
		// Fd:     br.conn.file.Fd(),
	}
	if err := unix.EpollCtl(epfd, unix.EPOLL_CTL_ADD, int(br.conn.file.Fd()), &ev); err != nil {
		return 0, fmt.Errorf("zerocopy.BufferedReader epoll_ctx: %w", err)
	}

	for {
		// log.Printf("br.Read: going to wait")
		var events [1]unix.EpollEvent // TODO: redundant with ev
		_, err := unix.EpollWait(epfd, events[:], -1 /* block indefinitely */)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			return 0, fmt.Errorf("zerocopy.BufferedReader epoll_wait: %w", err)
		}
		// log.Printf("br.Read: wait returned")

		// Re-initialize zc fields modified by previous getsockopt.
		zc.length = uint32(len(br.mmappedStorage))
		zc.copybufLen = int32(len(br.copybufStorage))

		rateLimiter.Do(func() {
			log.Printf("zc: %+v", zc)
			log.Printf("address: %x, copybufAddress: %x", zc.address, zc.copybufAddress)
		})

		_, _, errno := unix.Syscall6(
			unix.SYS_GETSOCKOPT,
			br.conn.file.Fd(), // TODO: Store the fd so we don't have to keep calling this?
			unix.SOL_TCP,
			unix.TCP_ZEROCOPY_RECEIVE,
			uintptr(unsafe.Pointer(&zc)),
			uintptr(unsafe.Pointer(&zcSize)),
			0, // Unused
		)
		// log.Printf("br.Read: getsockopt returned")
		if errno != 0 {
			// log.Printf("br.Read: getsockopt returned with an error %d", errno)
			if errno == unix.EINTR {
				continue
			}
			// log.Printf("zerocopy errno: %d", errno)
			return 0, errno // TODO: allocates?
		}

		// epoll only returns when there's data available or the
		// connection is closed.
		if n := zc.length + uint32(zc.copybufLen) + zc.recvSkipHint; n == 0 {
			// log.Printf("br.Read: getsockopt returned that we're done")
			return 0, io.EOF
		} else {
			// log.Printf("br.Read: getsockopt returned that we got %d bytes", n)
		}

		br.mmapped = br.mmappedStorage[:zc.length]
		br.copybuf = br.copybufStorage[:zc.copybufLen]
		br.skipHint = int(zc.recvSkipHint)

		break

		// rateLimiter.Do(func() {
		// 	// logger.Warningf("zerocopy skip hint set") // TODO:
		// 	// log how?
		// 	log.Printf("WARNING: zerocopy mmapped %d bytes, copybuffed %d bytes, and hinted %d bytes",
		// 		len(br.mmapped), len(br.copybuf), br.skipHint)
		// })
	}

	return br.readBuffered(dst)
}

// TODO: Peek and discard
// readBuffered returns any data that's already been buffered.
func (br *BufferedReader) readBuffered(dst []byte) (int, error) {
	// The mmapped bytes are always first.
	if len(br.mmapped) > 0 {
		n := copy(dst, br.mmapped)
		br.mmapped = br.mmapped[n:]
		Printf("returning %d mmapped bytes", n)
		return n, nil
	}

	// Then any stragglers copied into the copybuf.
	if len(br.copybuf) > 0 {
		n := copy(dst, br.copybuf)
		br.copybuf = br.copybuf[n:]
		Printf("returning %d copybuf bytes", n)
		return n, nil
	}

	// Finally, check for skip hint data. Ideally this shouldn't happen --
	// the kernel should utilize the copybuf -- but we still need to handle
	// it.
	if br.skipHint != 0 {
		rateLimiter.Do(func() {
			// logger.Warningf("zerocopy skip hint set") // TODO:
			// log how?
			log.Printf("zerocopy skip hint set to %d", br.skipHint)
		})
		toRead := min(len(dst), br.skipHint)
		n, err := br.conn.Read(dst[:toRead])
		br.skipHint -= n
		Printf("returning %d skip hint bytes", n)
		return n, err
	}

	Printf("no buffered bytes")
	return 0, nil
}

func roundUpToPage(size int) int {
	const pageSize = 4096
	return (size + pageSize - 1) &^ (pageSize - 1)
}
