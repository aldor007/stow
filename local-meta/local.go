package local_meta

import (
	"errors"
	"net/url"
	"os"

	"github.com/aldor007/stow"
)

// ConfigKeys are the supported configuration items for
// local storage.
const (
	ConfigKeyPath = "path"
)

// Kind is the kind of Location this package provides.
const Kind = "local-meta"

const (
	paramTypeValue = "item"
)

var metaPointer = [3]byte{0x12, 0x34, 0x01}


func init() {
	validatefn := func(config stow.Config) error {
		_, ok := config.Config(ConfigKeyPath)
		if !ok {
			return errors.New("missing path config")
		}
		return nil
	}
	makefn := func(config stow.Config) (stow.Location, error) {
		path, ok := config.Config(ConfigKeyPath)
		if !ok {
			return nil, errors.New("missing path config")
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, errors.New("path must be directory")
		}
		return &location{
			config: config,
		}, nil
	}
	kindfn := func(u *url.URL) bool {
		return u.Scheme == "file-meta"
	}
	stow.Register(Kind, makefn, kindfn, validatefn)
}
