package b2

import (
	"io"
	"strings"
"fmt"
	"github.com/aldor007/stow"
	"github.com/kurin/blazer/b2"

	"context"
)

// Amazon S3 bucket contains a creationdate and a name.
type container struct {
	name           string // Name is needed to retrieve items.
	client         *b2.Client
	bucket        *b2.Bucket
	ctx         context.Context
}

// ID returns a string value which represents the name of the container.
func (c *container) ID() string {
	return c.name
}

// Name returns a string value which represents the name of the container.
func (c *container) Name() string {
	return c.name
}

// Item returns a stow.Item instance of a container based on the
// name of the container and the key representing
func (c *container) Item(id string) (stow.Item, error) {
	return c.getItem(id)
}

// Items sends a request to retrieve a list of items that are prepended with
// the prefix argument. The 'cursor' variable facilitates pagination.
func (c *container) Items(prefix, cursor string, count int) ([]stow.Item, string, error) {

	iterator := c.bucket.List(c.ctx, b2.ListPrefix(prefix))

	var containerItems []stow.Item

	for iterator.Next() {
		obj := iterator.Object()
		attr,  _ := obj.Attrs(c.ctx)
		containerItems = append(containerItems, &item{
			object: obj,
			container: c,
			attrs: attr,
		})
	}


	return containerItems, "", nil
}

func (c *container) RemoveItem(id string) error {
	return c.bucket.Object(id).Delete(c.ctx)
}

// Put sends a request to upload content to the container. The arguments
// received are the name of the item (S3 Object), a reader representing the
// content, and the size of the file. Many more attributes can be given to the
// file, including metadata. Keeping it simple for now.
func (c *container) Put(name string, r io.Reader, size int64, metadata map[string]interface{}) (stow.Item, error) {
	obj := c.bucket.Object(strings.TrimPrefix(name, "/"))
	fmt.Println("start copy")
	attrs, err :=  prepMetadata(metadata, size)
	if err != nil {
		return  nil, err
	}
	w := obj.NewWriter(c.ctx, b2.WithAttrsOption(attrs))
	//if size > 100 {
	//	w.ConcurrentUploads = int(size/100)
	//}
	defer w.Close()
	if _, err = io.Copy(w, r); err != nil {
		return nil, err
	}

fmt.Println("END copy")
	newItem := &item{
		container: c,
		ctx: c.ctx,
	}

	return newItem, nil
}


// A request to retrieve a single item includes information that is more specific than
// a PUT. Instead of doing a request within the PUT, make this method available so that the
// request can be made by the field retrieval methods when necessary. This is the case for
// fields that are left out, such as the object's last modified date. This also needs to be
// done only once since the requested information is retained.
// May be simpler to just stick it in PUT and and do a request every time, please vouch
// for this if so.
func (c *container) getItem(id string) (*item, error) {
	fmt.Println("AAAAAdsdAA->", c.ctx, strings.TrimPrefix(id, "/"));
	obj := c.bucket.Object(strings.TrimPrefix(id, "/"))
	attrs, err := obj.Attrs(c.ctx)
	if err != nil {
		if b2.IsNotExist(err) {
			return nil, stow.ErrNotFound
		}

		return nil, err
	}


	i := &item{
		container: c,
		ctx: c.ctx,
		object: obj,
		attrs: attrs,
	}

	return i, nil
}

// Remove quotation marks from beginning and end. This includes quotations that
// are escaped. Also removes leading `W/` from prefix for weak Etags.
//
// Based on the Etag spec, the full etag value (<FULL ETAG VALUE>) can include:
// - W/"<ETAG VALUE>"
// - "<ETAG VALUE>"
// - ""
// Source: https://tools.ietf.org/html/rfc7232#section-2.3
//
// Based on HTTP spec, forward slash is a separator and must be enclosed in
// quotes to be used as a valid value. Hence, the returned value may include:
// - "<FULL ETAG VALUE>"
// - \"<FULL ETAG VALUE>\"
// Source: https://www.w3.org/Protocols/rfc2616/rfc2616-sec2.html#sec2.2
//
// This function contains a loop to check for the presence of the three possible
// filler characters and strips them, resulting in only the Etag value.
func cleanEtag(etag string) string {
	for {
		// Check if the filler characters are present
		if strings.HasPrefix(etag, `\"`) {
			etag = strings.Trim(etag, `\"`)

		} else if strings.HasPrefix(etag, `"`) {
			etag = strings.Trim(etag, `"`)

		} else if strings.HasPrefix(etag, `W/`) {
			etag = strings.Replace(etag, `W/`, "", 1)

		} else {
			break
		}
	}
	return etag
}

// prepMetadata parses a raw map into the native type required by S3 to set metadata (map[string]*string).
// TODO: validation for key values. This function also assumes that the value of a key value pair is a string.
func prepMetadata(md map[string]interface{}, size int64) (*b2.Attrs, error) {
	info := make(map[string]string, len(md) - 1)
	for k, v := range md {
		if k != "content-type" {
			info[k] = v.(string)
		}
	}
	fmt.Println("meta = ", md )
	contentType := "application/octet-stream"
	if ct, ok := md["content-type"]; ok {
		contentType = ct.(string)
	}

	return &b2.Attrs{
		Size: size,
		ContentType: contentType,
		Info: info,
	}, nil
}

// The first letter of a dash separated key value is capitalized, so perform a ToLower on it.
// This Key transformation of returning lowercase is consistent with other locations..
func parseMetadata(md map[string]*string) (map[string]interface{}, error) {
	m := make(map[string]interface{}, len(md))
	for key, value := range md {
		k := strings.ToLower(key)
		m[k] = *value
	}
	return m, nil
}
