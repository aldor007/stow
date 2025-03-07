package azure

import (
	"io"
	"net/url"
	"sync"
	"time"

	az "github.com/Azure/azure-sdk-for-go/storage"
	"github.com/aldor007/stow"
	"github.com/pkg/errors"
)

type item struct {
	id         string
	container  *container
	client     *az.BlobStorageClient
	properties az.BlobProperties
	url        url.URL
	metadata   map[string]interface{}
	infoOnce   sync.Once
	infoErr    error
	rangeData  stow.ContentRangeData
}

var (
	_ stow.Item       = (*item)(nil)
	_ stow.ItemRanger = (*item)(nil)
)

func (i *item) ID() string {
	return i.id
}

func (i *item) Name() string {
	return i.id
}

func (i *item) URL() *url.URL {
	u := i.client.GetContainerReference(i.container.id).GetBlobReference(i.id).GetURL()
	url, _ := url.Parse(u)
	url.Scheme = "azure"
	return url
}

func (i *item) Size() (int64, error) {
	return i.properties.ContentLength, nil
}

func (i *item) Open() (io.ReadCloser, error) {
	return i.client.GetContainerReference(i.container.id).GetBlobReference(i.id).Get(nil)
}

func (i *item) OpenParams(_ map[string]interface{}) (io.ReadCloser, error) {
	return i.Open()
}

func (i *item) ContentRange() (stow.ContentRangeData, error) {
	return stow.ContentRangeData{}, errors.New("not implemented")
}

func (i *item) ETag() (string, error) {
	return i.properties.Etag, nil
}

func (i *item) LastMod() (time.Time, error) {
	return time.Time(i.properties.LastModified), nil
}

func (i *item) Metadata() (map[string]interface{}, error) {
	err := i.ensureInfo()
	if err != nil {
		return nil, errors.Wrap(err, "retrieving metadata")
	}

	return i.metadata, nil
}

func (i *item) ensureInfo() error {
	if i.metadata == nil {
		i.infoOnce.Do(func() {
			blob := i.client.GetContainerReference(i.container.Name()).GetBlobReference(i.Name())
			infoErr := blob.GetMetadata(nil)
			if infoErr != nil {
				i.infoErr = infoErr
				return
			}

			mdParsed, infoErr := parseMetadata(blob.Metadata)
			if infoErr != nil {
				i.infoErr = infoErr
				return
			}
			i.metadata = mdParsed
		})
	}

	return i.infoErr
}

func (i *item) getInfo() (stow.Item, error) {
	itemInfo, err := i.container.Item(i.ID())
	if err != nil {
		return nil, err
	}
	return itemInfo, nil
}

// OpenRange opens the item for reading starting at byte start and ending
// at byte end.
func (i *item) OpenRange(start, end uint64) (io.ReadCloser, error) {
	opts := &az.GetBlobRangeOptions{
		Range: &az.BlobRange{
			Start: start,
			End:   end,
		},
	}

	return i.client.GetContainerReference(i.container.id).GetBlobReference(i.id).GetRange(opts)
}
