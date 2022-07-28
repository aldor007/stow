package local_meta

import (
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"fmt"

	"github.com/aldor007/stow"
	"github.com/vmihailenco/msgpack"
)

type container struct {
	name string
	path string
}

func (c *container) ID() string {
	return c.name
}

func (c *container) Name() string {
	return c.name
}

func (c *container) URL() *url.URL {
	return &url.URL{
		Scheme: "file",
		Path:   filepath.Clean(c.path),
	}
}

func (c *container) CreateItem(name string) (stow.Item, io.WriteCloser, error) {
	path := filepath.Join(c.path, name)
	item := &item{
		path: path,
		name: name,
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return item, f, nil
}

func (c *container) RemoveItem(id string) error {
	return os.RemoveAll(filepath.Join(c.path, id))
}

func (c *container) Put(name string, r io.Reader, size int64, metadata map[string]interface{}) (stow.Item, error) {
	path := filepath.Join(c.path, name)
	item := &item{
		path: path,
		name: name,
	}

	//info, err := os.Stat(path)
	//if !os.IsNotExist(err) {
	//	return nil, err
	//}
	//

	if size == 0  {
		err := os.MkdirAll(path, 0777)
		if err != nil {
			return nil, err
		}
		return item, nil
	}

	dirPath := filepath.Dir(path)
	err := os.MkdirAll(dirPath, 0777)
	if err != nil {
		return nil, err
	}


	f, err := os.Create(path)
	defer f.Close()
	if err != nil {
		return nil, err
	}

	md := parseMetadata(metadata)
	metaReader, err := prepareMetaReader(md)
	if err != nil {
		return nil, err
	}

	metaLen, err := io.Copy(f, metaReader)
	if err != nil {
		return nil, err
	}
	n, err := io.Copy(f, r)
	if err != nil {
		defer os.Remove(path)
		return nil, err
	}
	if n != size {
		defer os.Remove(path)
		return nil, errors.New(fmt.Sprintf("bad size %d != %d %d", n, size, metaLen))
	}
	return item, nil
}

func (c *container) Items(prefix, cursor string, count int) ([]stow.Item, string, error) {
	files, err := flatdirs(filepath.Join(c.path, prefix))
	if err != nil {
		return nil, "", err
	}

	if cursor != stow.CursorStart {
		// seek to the cursor
		ok := false
		for i, file := range files {
			if file.Name() == cursor {
				files = files[i:]
				ok = true
				break
			}
		}
		if !ok {
			return nil, "", stow.ErrBadCursor
		}
	}
	if len(files) > count {
		cursor = files[count].Name()
		files = files[:count]
	} else if len(files) <= count {
		cursor = "" // end
	}

	lenFiles := len(files)
	var items []stow.Item
	for _, f := range files {

		path, err := filepath.Abs(filepath.Join(c.path, prefix, f.Name()))
		if err != nil {
			return nil, "", err
		}

		if path == c.path && lenFiles != 1 {
			continue
		}

		item := &item{
			path: path,
			name: f.Name(),
		}
		items = append(items, item)
	}
	return items, cursor, nil
}

func (c *container) Item(id string) (stow.Item, error) {
	path := filepath.Join(c.path, id)
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, stow.ErrNotFound
	}

	if err != nil {
		return nil, err
	}

	item := &item{
		path: path,
		name: id,
		info: info,
	}

	item.setMetadata(info)

	return item, nil
}

// flatdirs walks the entire tree returning a list of
// os.FileInfo for all items encountered.
func flatdirs(path string) ([]os.FileInfo, error) {
	var list []os.FileInfo

	err := filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		flatname, err := filepath.Rel(path, p)
		if err != nil {
			return err
		}

		if info.IsDir() {
			flatname = flatname + "/"
		}
		list = append(list, fileinfo{
			FileInfo: info,
			name:     flatname,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return list, nil
}

type fileinfo struct {
	os.FileInfo
	name string
}

func (f fileinfo) Name() string {
	return f.name
}

func parseMetadata(meta map[string]interface{}) map[string]string {
	md := make(map[string]string, len(meta))
	for k, v := range meta {
		md[k] = v.(string)
	}

	//lastMod, _ := i.LastMod()
	//md["Last-Modified"] = lastMod.String()
	//md["ETag"] = i.ETag()

	return md
}

func prepareMetaReader(meta map[string]string)  (io.ReadCloser, error) {
	bufMsg, err := msgpack.Marshal(&meta)
	if err != nil {
		return nil, err
	}

	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(len(bufMsg)))
	metaHeader := append(metaPointer[:], bs...)
	lenMetaHeader := len(metaHeader)

	var bufMeta []byte
	bufMeta = make([]byte, lenMetaHeader + len(bufMsg))
	copy(bufMeta[:], metaHeader)
	copy(bufMeta[lenMetaHeader:], bufMsg)

	metadata := ioutil.NopCloser(bytes.NewReader(bufMeta[:]))
	return metadata, nil
}