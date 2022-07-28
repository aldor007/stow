package local_meta

import (
	"io"
	"net/url"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/vmihailenco/msgpack"
	"crypto/md5"
	"encoding/hex"
	"github.com/aldor007/stow"
)

// Metadata constants describe the metadata available
// for a local Item.
const (
	MetadataPath       = "path"
	MetadataIsDir      = "is_dir"
	MetadataDir        = "dir"
	MetadataName       = "name"
	MetadataMode       = "mode"
	MetadataModeD      = "mode_d"
	MetadataPerm       = "perm"
	MetadataINode      = "inode"
	MetadataSize       = "size"
	MetadataIsHardlink = "is_hardlink"
	MetadataIsSymlink  = "is_symlink"
	MetadataLink       = "link"
)

type item struct {
	path     string // absolute path to file
	name     string // file name
	infoOnce sync.Once // protects info
	info     os.FileInfo
	infoErr  error
	metadata map[string]interface{}
	properties map[string]string
	metadataReaded bool
	metadataSize uint32
}

func (i *item) ID() string {
	return i.name
}

func (i *item) Name() string {
	return filepath.Base(i.path)
}

func (i *item) Size() (int64, error) {
	err := i.ensureInfo()
	if err != nil {
		return 0, err
	}
	return i.info.Size() - int64(i.metadataSize), nil
}

func (i *item) URL() *url.URL {
	return &url.URL{
		Scheme: "file-meta",
		Path:   filepath.Clean(i.path),
	}
}

func (i *item) ETag() (string, error) {
	err := i.ensureInfo()
	if err != nil {
		return "", nil
	}

	if etag, ok := i.properties["Etag"]; ok {
		return etag, nil
	}

	h := md5.New()
	io.WriteString(h, i.info.ModTime().String())

	etag := "W/\"" + hex.EncodeToString(h.Sum(nil)) + "\""

	return etag, nil
}

// Open opens the file for reading.
func (i *item) Open() (io.ReadCloser, error) {
	r, err := os.Open(i.path)
	if err != nil {
		return nil, err
	}

	var bufMeta [3]byte
	n, err := io.ReadFull(r, bufMeta[:])
	if err != nil {
		return nil, err
	}

	i.metadataReaded = true
	// File with metadata looks like
	// 3 bytes for header with version - it indicated that file has metadata
	// 4 bytes for  uint32 for meta len (metaLen)
	// metaLen bytes - msgpack JSON  with data
	if bytes.Compare(bufMeta[0:1], metaPointer[0:1]) == 0 {
		i.metadataSize = uint32(n)
		var metaLen [4]byte
		n, err := io.ReadFull(r, metaLen[:])
		if err != nil {
			return nil, err
		}
		i.metadataSize = i.metadataSize + uint32(n)
		mLen := binary.LittleEndian.Uint32(metaLen[:])

		var metaUnmarshall map[string]string
		metaUnmarshall = make(map[string]string)
		metaData := make([]byte, mLen)
		n, err = io.ReadFull(r, metaData)
		i.metadataSize = i.metadataSize + uint32(n)
		if err != nil {
			return nil, err
		}

		// compare file metadata version
		if bufMeta[2] == metaPointer[2] {
			if uint32(n) != mLen {
				return nil, errors.New("invalid metadata")
			}

			err = msgpack.Unmarshal(metaData[:], &metaUnmarshall)
			if err != nil {
				return nil, err
			}

			if i.metadata == nil {
				err = i.ensureInfo()
				if err != nil {
					return nil, err
				}
			}

			i.properties = metaUnmarshall
			for k, v := range i.properties {
				i.metadata[k] = v
			}

			return newRelativeSeeker(r, int64(i.metadataSize)), nil
		}

	} else {
		r.Seek(0, io.SeekStart)
	}

	return r, err
}

func (i *item) OpenParams(_ map[string]interface{}) (io.ReadCloser, error) {
	return i.Open()
}


func (i *item) ContentRange() (stow.ContentRangeData, error) {
	return stow.ContentRangeData{}, errors.New("not implemented")
}

func (i *item) LastMod() (time.Time, error) {
	err := i.ensureInfo()
	if err != nil {
		return time.Time{}, err
	}

	if lastMod, ok := i.properties["Last-Modified"]; ok {
		lastModTime, err := time.Parse(http.TimeFormat, lastMod)
		if err != nil {
			return time.Time{}, err
		}

		return lastModTime, nil
	}

	return i.info.ModTime(), nil
}

func (i *item) ensureInfo() error {
	i.infoOnce.Do(func() {
		if i.info == nil {
			i.info, i.infoErr = os.Lstat(i.path) // retrieve item file info

			i.infoErr = i.setMetadata(i.info) // merge file and metadata maps
			if i.infoErr != nil {
				return
			}
		}

		if i.properties == nil && !i.metadataReaded && i.info.IsDir() == false {
			r, err := i.Open()
			if err == nil {
				r.Close()
			} else {
				i.infoErr = err
			}
		}


	})
	return i.infoErr
}

func (i *item) setMetadata(info os.FileInfo) error {
	fileMetadata := getFileMetadata(i.path, info) // retrieve file metadata
	i.metadata = fileMetadata
	return nil
}

// Metadata gets stat information for the file.
func (i *item) Metadata() (map[string]interface{}, error) {
	err := i.ensureInfo()
	if err != nil {
		return nil, err
	}


	return i.metadata, nil
}