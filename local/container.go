package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/aldor007/stow"
)

type container struct {
	name          string
	path          string
	allowMetadata bool
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

func (c *container) PreSignRequest(_ context.Context, _ stow.ClientMethod, _ string,
	_ stow.PresignRequestParams) (url string, err error) {
	return "", fmt.Errorf("unsupported")
}

func (c *container) CreateItem(name string) (stow.Item, io.WriteCloser, error) {
	path := filepath.Join(c.path, filepath.FromSlash(name))
	item := &item{
		path:          path,
		name:          name,
		contPrefixLen: len(c.path) + 1,
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return item, f, nil
}

func (c *container) RemoveItem(id string) error {
	return os.Remove(filepath.Join(c.path, id))
}

func (c *container) Put(name string, r io.Reader, size int64, metadata map[string]interface{}) (stow.Item, error) {
	if c.allowMetadata == false && len(metadata) > 0 {
		return nil, stow.NotSupported("metadata")
	}

	path := filepath.Join(c.path, filepath.FromSlash(name))
	item := &item{
		path:          path,
		name:          name,
		contPrefixLen: len(c.path) + 1,
	}
	err := os.MkdirAll(filepath.Dir(path), 0777)
	if err != nil {
		return nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	n, err := io.Copy(f, r)
	if err != nil {
		return nil, err
	}
	if n != size {
		return nil, errors.New("bad size")
	}
	return item, nil
}

func (c *container) Items(prefix, cursor string, count int) ([]stow.Item, string, error) {
	prefix = filepath.FromSlash(prefix)
	files, err := flatdirs(c.path)
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

	files = files[1:]
	var items []stow.Item
	for _, f := range files {
		path, err := filepath.Abs(filepath.Join(c.path, f.Name()))
		if err != nil {
			return nil, "", err
		}
		if !strings.HasPrefix(f.Name(), prefix) {
			continue
		}
		item := &item{
			path:          path,
			name:          f.Name(),
			contPrefixLen: len(c.path) + 1,
		}
		items = append(items, item)
	}
	return items, cursor, nil
}

func (c *container) Item(id string) (stow.Item, error) {
	path := filepath.Join(c.path, id)
	if !filepath.IsAbs(id) {
		path = filepath.Join(c.path, filepath.FromSlash(id))
	}
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, stow.ErrNotFound
	}
	if info.IsDir() {
		return nil, errors.New("unexpected directory")
	}
	_, err = filepath.Rel(c.path, path)
	if err != nil {
		return nil, err
	}
	item := &item{
		path:          path,
		name:          id,
		contPrefixLen: len(c.path) + 1,
	}
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
