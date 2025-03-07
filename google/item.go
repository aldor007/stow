package google

import (
	"context"
	"io"
	"net/url"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aldor007/stow"
	"errors"
)
var _ stow.Item = (*Item)(nil)

type Item struct {
	container    *Container       // Container information is required by a few methods.
	client       *storage.Client  // A client is needed to make requests.
	name         string
	hash         string
	etag         string
	size         int64
	url          *url.URL
	lastModified time.Time
	metadata     map[string]interface{}
	object       *storage.ObjectAttrs
	ctx          context.Context
}

// ID returns a string value that represents the name of a file.
func (i *Item) ID() string {
	return i.name
}

// Name returns a string value that represents the name of the file.
func (i *Item) Name() string {
	return i.name
}

// Size returns the size of an item in bytes.
func (i *Item) Size() (int64, error) {
	return i.size, nil
}

// URL returns a url which follows the predefined format
func (i *Item) URL() *url.URL {
	return i.url
}

// Open returns an io.ReadCloser to the object. Useful for downloading/streaming the object.
func (i *Item) Open() (io.ReadCloser, error) {
	obj := i.container.Bucket().Object(i.name)
	return obj.NewReader(i.ctx)
}

// OpenRange returns an io.Reader to the object for a specific byte range
func (i *Item) OpenRange(start, end uint64) (io.ReadCloser, error) {
	obj := i.container.Bucket().Object(i.name)
	return obj.NewRangeReader(i.ctx, int64(start), int64(end - start) + 1)
}

func (i *Item) ContentRange() (stow.ContentRangeData, error) {
	return stow.ContentRangeData{}, errors.New("not implemented")
}

func (i *Item) OpenParams(_ map[string]interface{}) (io.ReadCloser, error) {
	return i.Open()
}

// LastMod returns the last modified date of the item.
func (i *Item) LastMod() (time.Time, error) {
	return i.lastModified, nil
}

// Metadata returns a nil map and no error.
func (i *Item) Metadata() (map[string]interface{}, error) {
	return i.metadata, nil
}

// ETag returns the ETag value
func (i *Item) ETag() (string, error) {
	return i.etag, nil
}

// Object returns the Google Storage Object
func (i *Item) StorageObject() *storage.ObjectAttrs {
	return i.object
}

// prepUrl takes a MediaLink string and returns a url
func prepUrl(str string) (*url.URL, error) {
	u, err := url.Parse(str)
	if err != nil {
		return nil, err
	}
	u.Scheme = "google"

	// Discard the query string
	u.RawQuery = ""
	return u, nil
}
