package b2

import (
	"io"
	"net/url"
	"sync"
	"time"

	"github.com/aldor007/stow"
	"github.com/pkg/errors"
	"github.com/kurin/blazer/b2"
	"context"
)

// The item struct contains an id (also the name of the file/S3 Object/Item),
// a container which it belongs to (s3 Bucket), a client, and a URL. The last
// field, properties, contains information about the item, including the ETag,
// file name/id, size, owner, last modified date, and storage class.
// see Object type at http://docs.aws.amazon.com/sdk-for-go/api/service/s3/
// for more info.
// All fields are unexported because methods exist to facilitate retrieval.
type item struct {
	// Container information is required by a few methods.
	object *b2.Object
	attrs *b2.Attrs
	ctx context.Context
	container *container
	infoOnce   sync.Once
	infoErr    error
}

// ID returns a string value that represents the name of a file.
func (i *item) ID() string {
	return i.object.Name()
}

// Name returns a string value that represents the name of the file.
func (i *item) Name() string {
	return i.object.Name()
}

// Size returns the size of an item in bytes.
func (i *item) Size() (int64, error) {
	return i.attrs.Size, nil
}

// URL returns a formatted string which follows the predefined format
// that every S3 asset is given.
func (i *item) URL() *url.URL {
	return &url.URL{
		Scheme: "b2",
		Path:   i.object.URL(),
	}
}

// Open retrieves specic information about an item based on the container name
// and path of the file within the container. This response includes the body of
// resource which is returned along with an error.
func (i *item) Open() (io.ReadCloser, error) {
	r := i.object.NewReader(i.ctx)
	r.ConcurrentDownloads  = 2

	return r, nil
}

// LastMod returns the last modified date of the item. The response of an item that is PUT
// does not contain this field. Solution? Detect when the LastModified field (a *time.Time)
// is nil, then do a manual request for it via the Item() method of the container which
// does return the specified field. This more detailed information is kept so that we
// won't have to do it again.
func (i *item) LastMod() (time.Time, error) {
	err := i.ensureInfo()
	if err != nil {
		return time.Time{}, errors.Wrap(err, "retrieving Last Modified information of Item")
	}
	return i.attrs.LastModified, nil
}

// ETag returns the ETag value from the properies field of an item.
func (i *item) ETag() (string, error) {
	return i.attrs.SHA1, nil
}

func (i *item) Metadata() (map[string]interface{}, error) {
	err := i.ensureInfo()
	if err != nil {
		return nil, errors.Wrap(err, "retrieving metadata")
	}

	meta := make(map[string]interface{})
	for k, v := range i.attrs.Info {
		meta[k] = v
	}
	return meta, nil
}

func (i *item) ensureInfo() error {
	if i.attrs.Info == nil {
		i.infoOnce.Do(func() {
			// Retrieve Item information
			//itemInfo, infoErr := i.getInfo()
			//if infoErr != nil {
			//	i.infoErr = infoErr
			//	return
			//}


		})
	}
	return i.infoErr
}

func (i *item) getInfo() (stow.Item, error) {
	itemInfo, err := i.container.getItem(i.ID())
	if err != nil {
		return nil, err
	}
	return itemInfo, nil
}
