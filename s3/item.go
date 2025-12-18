package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aldor007/stow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
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
	container *container
	// A client is needed to make requests.
	client *s3.Client
	// properties represent the characteristics of the file. Name, Etag, etc.
	properties properties
	infoOnce   sync.Once
	infoErr    error
	tags       map[string]interface{}
	tagsOnce   sync.Once
	tagsErr    error
	rangeData  stow.ContentRangeData
}

type properties struct {
	ETag         *string    `type:"string"`
	Key          *string    `min:"1" type:"string"`
	LastModified *time.Time `type:"timestamp" timestampFormat:"iso8601"`
	Owner        *types.Owner  `type:"structure"`
	Size         *int64     `type:"integer"`
	StorageClass string    `type:"string" enum:"ObjectStorageClass"`
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
	if i.container.customEndpoint == "" {
		genericURL := fmt.Sprintf("https://s3-%s.amazonaws.com/%s/%s", i.container.Region(), i.container.Name(), i.Name())

		return &url.URL{
			Scheme: "s3",
			Path:   genericURL,
		}
	}

	genericURL := fmt.Sprintf("%s/%s", i.container.Name(), i.Name())
	return &url.URL{
		Scheme: "s3",
		Path:   genericURL,
	}
}

// Open retrieves specic information about an item based on the container name
// and path of the file within the container. This response includes the body of
// resource which is returned along with an error.
func (i *item) Open() (io.ReadCloser, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(i.container.Name()),
		Key:    aws.String(i.ID()),
	}

	response, err := i.client.GetObject(context.TODO(), params)
	if err != nil {
		return nil, errors.Wrap(err, "Open, getting the object")
	}
	return response.Body, nil
}

func (i *item) OpenParams(p map[string]interface{}) (io.ReadCloser, error) {
	var strRange string
	objetRange, ok := p["range"]
	if ok {
		strRange = objetRange.(string)
	}
	params := &s3.GetObjectInput{
		Bucket: aws.String(i.container.Name()),
		Key:    aws.String(i.ID()),
		Range:  aws.String(strRange),
	}

	response, err := i.client.GetObject(context.TODO(), params)
	if err != nil {
		return nil, errors.Wrap(err, "Open, getting the object")
	}
	if cr := response.ContentRange; cr != nil {
		i.rangeData = stow.ContentRangeData{ContentRange: *cr, ContentLength: response.ContentLength}
	}

	return response.Body, nil
}

func (i *item) ContentRange() (stow.ContentRangeData, error) {
	if i.rangeData.ContentRange == "" {
		return stow.ContentRangeData{}, errors.New("response is not a range")
	}
	return i.rangeData, nil
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

// Tags returns a map of tags on an Item
func (i *item) Tags() (map[string]interface{}, error) {
	i.tagsOnce.Do(func() {
		params := &s3.GetObjectTaggingInput{
			Bucket: aws.String(i.container.name),
			Key:    aws.String(i.ID()),
		}

		res, err := i.client.GetObjectTagging(context.TODO(), params)
		if err != nil {
			if strings.Contains(err.Error(), "NoSuchKey") {
				i.tagsErr = stow.ErrNotFound
				return
			}
			i.tagsErr = errors.Wrap(err, "getObjectTagging")
			return
		}

		i.tags = make(map[string]interface{})
		for _, t := range res.TagSet {
			i.tags[*t.Key] = *t.Value
		}
	})

	return i.tags, i.tagsErr
}

// Restore initiates restoration from GLACIER or DEEP_ARCHIVE storage class
// Implements stow.Restorable interface
// days: number of days the restored copy should be available (1-30)
// tier: restoration speed - "Expedited", "Standard", or "Bulk"
func (i *item) Restore(ctx context.Context, days int, tier string) error {
	if err := i.ensureInfo(); err != nil {
		return err
	}

	// Parse tier
	var restoreTier types.Tier
	switch tier {
	case "Expedited":
		restoreTier = types.TierExpedited
	case "Bulk":
		restoreTier = types.TierBulk
	default:
		restoreTier = types.TierStandard
	}

	input := &s3.RestoreObjectInput{
		Bucket: aws.String(i.container.name),
		Key:    aws.String(i.ID()),
		RestoreRequest: &types.RestoreRequest{
			Days: int32(days),
			GlacierJobParameters: &types.GlacierJobParameters{
				Tier: restoreTier,
			},
		},
	}

	_, err := i.client.RestoreObject(ctx, input)
	if err != nil {
		// RestoreAlreadyInProgress is not an error - restore was already requested
		if strings.Contains(err.Error(), "RestoreAlreadyInProgress") {
			return nil
		}
		return errors.Wrap(err, "RestoreObject API call failed")
	}

	return nil
}

// RestoreStatus checks the x-amz-restore header to determine restoration status
// Implements stow.Restorable interface
// Returns:
//   - inProgress: true if restoration is currently ongoing
//   - expiryDate: when the restored temporary copy expires (nil if not available)
//   - err: error if status check failed
func (i *item) RestoreStatus(ctx context.Context) (inProgress bool, expiryDate *time.Time, err error) {
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(i.container.name),
		Key:    aws.String(i.ID()),
	}

	result, err := i.client.HeadObject(ctx, headInput)
	if err != nil {
		return false, nil, errors.Wrap(err, "HeadObject failed")
	}

	// Check x-amz-restore header
	if result.Restore == nil {
		return false, nil, nil // Not being restored
	}

	// Parse x-amz-restore header format:
	// 'ongoing-request="true"' or
	// 'ongoing-request="false", expiry-date="Fri, 21 Dec 2012 00:00:00 GMT"'
	restoreHeader := aws.ToString(result.Restore)
	inProgress = strings.Contains(restoreHeader, `ongoing-request="true"`)

	// Parse expiry date if restore is complete
	if !inProgress && strings.Contains(restoreHeader, "expiry-date") {
		if idx := strings.Index(restoreHeader, `expiry-date="`); idx != -1 {
			dateStart := idx + 13
			if dateEnd := strings.Index(restoreHeader[dateStart:], `"`); dateEnd != -1 {
				dateStr := restoreHeader[dateStart : dateStart+dateEnd]
				if parsedDate, parseErr := time.Parse(time.RFC1123, dateStr); parseErr == nil {
					expiryDate = &parsedDate
				}
			}
		}
	}

	return inProgress, expiryDate, nil
}

// OpenRange opens the item for reading starting at byte start and ending
// at byte end.
func (i *item) OpenRange(start, end uint64) (io.ReadCloser, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(i.container.Name()),
		Key:    aws.String(i.ID()),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	}

	response, err := i.client.GetObject(context.TODO(), params)
	if err != nil {
		return nil, errors.Wrap(err, "Open, getting the object")

	}
	if cr := response.ContentRange; cr != nil {
		i.rangeData = stow.ContentRangeData{ContentRange: *cr, ContentLength: response.ContentLength}
	}

	return response.Body, nil
}
