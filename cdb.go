// Package cdb reads and writes cdb ("constant database") files.
//
// See the original cdb specification and C implementation by D. J. Bernstein
// at http://cr.yp.to/cdb.html.
package cdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"runtime"
)

const (
	headerSize = uint32(256 * 8)
)

type Cdb struct {
	r      io.ReaderAt
	closer io.Closer
}

type Context struct {
	buf    []byte
	loop   uint32 // number of hash slots searched under this key
	khash  uint32 // initialized if loop is nonzero
	kpos   uint32 // initialized if loop is nonzero
	hpos   uint32 // initialized if loop is nonzero
	hslots uint32 // initialized if loop is nonzero
	dpos   uint32 // initialized if FindNext() returns true
	dlen   uint32 // initialized if FindNext() returns true
}

// Open opens the named file read-only and returns a new Cdb object.  The file
// should exist and be a cdb-format database file.
func Open(name string) (*Cdb, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	c := New(f)
	c.closer = f
	runtime.SetFinalizer(c, (*Cdb).Close)
	return c, nil
}

// Close closes the cdb for any further reads.
func (c *Cdb) Close() (err error) {
	if c.closer != nil {
		err = c.closer.Close()
		c.closer = nil
		runtime.SetFinalizer(c, nil)
	}
	return err
}

// New creates a new Cdb from the given ReaderAt, which should be a cdb format database.
func New(r io.ReaderAt) *Cdb {
	c := new(Cdb)
	c.r = r
	return c
}

// NewContext returns a new context to be used in CDB calls.
func NewContext() *Context {
	return &Context{
		buf: make([]byte, 64),
	}
}

// Data returns the first data value for the given key.
// If no such record exists, it returns EOF.
func (c *Cdb) Data(key []byte, context *Context) (data []byte, err error) {
	c.FindStart(context)
	if err = c.find(key, context); err != nil {
		return nil, err
	}

	data = make([]byte, context.dlen)
	err = c.read(data, context.dpos)

	return
}

// FindStart resets the cdb to search for the first record under a new key.
func (c *Cdb) FindStart(context *Context) { context.loop = 0 }

// FindNext returns the next data value for the given key as a SectionReader.
// If there are no more records for the given key, it returns EOF.
// FindNext acts as an iterator: The iteration should be initialized by calling
// FindStart and all subsequent calls to FindNext should use the same key value.
func (c *Cdb) FindNext(key []byte,
	context *Context) (rdata *io.SectionReader, err error) {
	if err := c.find(key, context); err != nil {
		return nil, err
	}
	return io.NewSectionReader(c.r, int64(context.dpos),
		int64(context.dlen)), nil
}

// Find returns the first data value for the given key as a SectionReader.
// Find is the same as FindStart followed by FindNext.
func (c *Cdb) Find(key []byte,
	context *Context) (rdata *io.SectionReader, err error) {
	c.FindStart(context)
	return c.FindNext(key, context)
}

func (c *Cdb) find(key []byte, context *Context) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()

	var pos, h uint32

	klen := uint32(len(key))
	if context.loop == 0 {
		h = checksum(key)
		context.hpos, context.hslots = c.readNums((h<<3)&2047,
			context)
		if context.hslots == 0 {
			return io.EOF
		}
		context.khash = h
		h >>= 8
		h %= context.hslots
		h <<= 3
		context.kpos = context.hpos + h
	}

	for context.loop < context.hslots {
		h, pos = c.readNums(context.kpos, context)
		if pos == 0 {
			return io.EOF
		}
		context.loop++
		context.kpos += 8
		if context.kpos == context.hpos+(context.hslots<<3) {
			context.kpos = context.hpos
		}
		if h == context.khash {
			rklen, rdlen := c.readNums(pos, context)
			if rklen == klen {
				if c.match(key, pos+8, context) {
					context.dlen = rdlen
					context.dpos = pos + 8 + klen
					return nil
				}
			}
		}
	}

	return io.EOF
}

func (c *Cdb) read(buf []byte, pos uint32) error {
	_, err := c.r.ReadAt(buf, int64(pos))
	return err
}

func (c *Cdb) match(key []byte, pos uint32, context *Context) bool {
	buf := context.buf
	klen := len(key)
	for n := 0; n < klen; n += len(buf) {
		nleft := klen - n
		if len(buf) > nleft {
			buf = buf[:nleft]
		}
		if err := c.read(buf, pos); err != nil {
			panic(err)
		}
		if !bytes.Equal(buf, key[n:n+len(buf)]) {
			return false
		}
		pos += uint32(len(buf))
	}
	return true
}

func (c *Cdb) readNums(pos uint32, context *Context) (uint32, uint32) {
	if _, err := c.r.ReadAt(context.buf[:8], int64(pos)); err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint32(context.buf),
		binary.LittleEndian.Uint32(context.buf[4:])
}
