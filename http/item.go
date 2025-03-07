package http

import (
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/aldor007/stow"
	"github.com/pkg/errors"
	"fmt"
)
var _ stow.Item = (*item)(nil)

// The item struct contains an id (also the name of the file/S3 Object/Item),
// a container which it belongs to (s3 Bucket), a client, and a URL. The last
// field, properties, contains information about the item, including the ETag,
// file name/id, size, owner, last modified date, and storage class.
// see Object type at http://docs.aws.amazon.com/sdk-for-go/api/service/s3/
// for more info.
// All fields are unexported because methods exist to facilitate retrieval.
type item struct {
	container  *container
	url        string
	name       string
	client     *http.Client
	properties properties
	infoOnce   sync.Once
	infoErr    error
}

type properties struct {
	ETag         *string    `type:"string"`
	Key          *string    `min:"1" type:"string"`
	LastModified *time.Time `type:"timestamp" timestampFormat:"iso8601"`
	Size         *int64     `type:"integer"`
	Metadata     map[string]interface{}
}

// ID returns a string value that represents the name of a file.
func (i *item) ID() string {
	return *i.properties.Key
}

// Name returns a string value that represents the name of the file.
func (i *item) Name() string {
	return *i.properties.Key
}

// Size returns the size of an item in bytes.
func (i *item) Size() (int64, error) {
	return *i.properties.Size, nil
}

// URL returns a formatted string which follows the predefined format
// that every S3 asset is given.
func (i *item) URL() *url.URL {
	u, _ := url.Parse(i.url)
	return u
}

// Open retrieves specic information about an item based on the container name
// and path of the file within the container. This response includes the body of
// resource which is returned along with an error.
func (i *item) Open() (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", i.url, nil)
	if err != nil {
		return nil, err
	}

	for h, v := range i.container.headers {
		req.Header.Set(h, v)
	}

	response, err := i.client.Do(req)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("wrong response status code %d", response.StatusCode))
	}

	return response.Body, err
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
	return *i.properties.LastModified, nil
}

// ETag returns the ETag value from the properies field of an item.
func (i *item) ETag() (string, error) {
	return *(i.properties.ETag), nil
}

func (i *item) Metadata() (map[string]interface{}, error) {
	err := i.ensureInfo()
	if err != nil {
		return nil, errors.Wrap(err, "retrieving metadata")
	}
	return i.properties.Metadata, nil
}

func (i *item) ensureInfo() error {
	if i.properties.Metadata == nil || i.properties.LastModified == nil {
		i.infoOnce.Do(func() {
			// Retrieve Item information
			itemInfo, infoErr := i.getInfo()
			if infoErr != nil {
				i.infoErr = infoErr
				return
			}

			// Set metadata field
			i.properties.Metadata, infoErr = itemInfo.Metadata()
			if infoErr != nil {
				i.infoErr = infoErr
				return
			}

			// Set LastModified field
			lmValue, infoErr := itemInfo.LastMod()
			if infoErr != nil {
				i.infoErr = infoErr
				return
			}
			i.properties.LastModified = &lmValue
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

func (i *item) OpenParams(_ map[string]interface{}) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}

func (i *item) ContentRange() (stow.ContentRangeData, error) {
	return stow.ContentRangeData{}, errors.New("not implemented")
}