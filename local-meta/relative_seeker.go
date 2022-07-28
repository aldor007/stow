package local_meta

import (
	"os"
	"io"
)

// relativeSeeker aim to allow hide some data from beginning of file
type relativeSeeker struct {
	file *os.File
	shift int64
}

func (rs relativeSeeker) Close()  error {
	return rs.file.Close()
}

func (rs relativeSeeker) Read(b []byte) (n int, err error)  {
	return rs.file.Read(b)
}

func (rs relativeSeeker) ReadAt(b []byte, off int64) (n int, err error)  {
	return rs.file.ReadAt(b, off)
}

func (rs relativeSeeker) Write(b []byte) (n int, err error)  {
	return rs.file.Write(b)
}

func (rs relativeSeeker) WriteAt(b []byte, off int64) (n int, err error)  {
	return rs.file.WriteAt(b, off)
}

func (rs relativeSeeker) Seek(offset int64, whence int) (ret int64, err error)  {
	// move start of file plust shift
	if whence == io.SeekStart {
		offset = offset + rs.shift
	}

	if whence == io.SeekEnd {
		offset = offset - rs.shift
	}

	return rs.file.Seek(offset, whence)
}

func newRelativeSeeker(f *os.File, s int64) relativeSeeker {
	return relativeSeeker{f, s}
}