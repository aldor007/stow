package local_meta_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/cheekybits/is"
	"github.com/aldor007/stow"
	"github.com/aldor007/stow/test"
)

func TestStow(t *testing.T) {
	is := is.New(t)

	dir, err := ioutil.TempDir("testdata", "stow")
	is.NoErr(err)
	defer os.RemoveAll(dir)
	cfg := stow.ConfigMap{"path": dir}

	test.All(t, "local", cfg)
}
