package stow_test

import (
	"errors"
	"net/url"
	"testing"

	"github.com/aldor007/stow"
	"github.com/cheekybits/is"
)

func TestKindByURL(t *testing.T) {
	is := is.New(t)
	u, err := url.Parse("test://container/item")
	is.NoErr(err)
	kind, err := stow.KindByURL(u)
	is.NoErr(err)
	is.Equal(kind, testKind)
}

func TestKinds(t *testing.T) {
	is := is.New(t)
	stow.Register("example", nil, nil, nil)
	is.Equal(stow.Kinds(), []string{"test", "example"})
}

func TestIsCursorEnd(t *testing.T) {
	is := is.New(t)
	is.True(stow.IsCursorEnd(""))
	is.False(stow.IsCursorEnd("anything"))
}

func TestErrNotSupported(t *testing.T) {
	is := is.New(t)
	err := errors.New("something")
	is.False(stow.IsNotSupported(err))
	err = stow.NotSupported("feature")
	is.True(stow.IsNotSupported(err))
}

func TestDuplicateKinds(t *testing.T) {
	is := is.New(t)
	stow.Register("example", nil, nil, nil)
	is.Equal(stow.Kinds(), []string{"test", "example"})
	stow.Register("example", nil, nil, nil)
	is.Equal(stow.Kinds(), []string{"test", "example"})
}
