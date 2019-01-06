package b2

import (
	"net/url"

	"github.com/pkg/errors"
	"github.com/aldor007/stow"
	"github.com/kurin/blazer/b2"
	"context"
)

// A location contains a client + the configurations used to create the client.
type location struct {
	config         stow.Config
	client         *b2.Client
	ctx context.Context
}

// CreateContainer creates a new container, in this case an S3 bucket.
// The bare minimum needed is a container name, but there are many other
// options that can be provided.
func (l *location) CreateContainer(containerName string) (stow.Container, error) {

	bucket, err := l.client.Bucket(l.ctx, containerName)
	if err != nil {
		return nil, err
	}
	newContainer := &container{
		name:           containerName,
		client:         l.client,
		bucket:        bucket,
	}

	return newContainer, nil
}

// Containers returns a slice of the Container interface, a cursor, and an error.
// This doesn't seem to exist yet in the API without doing a ton of manual work.
// Get the list of buckets, query every single one to retrieve region info, and finally
// return the list of containers that have a matching region against the client. It's not
// possible to manipulate a container within a region that doesn't match the clients'.
// This is because AWS user credentials can be tied to regions. One solution would be
// to start a new client for every single container where the region matches, this would
// also check the credentials on every new instance... Tabled for later.
func (l *location) Containers(prefix, cursor string, count int) ([]stow.Container, string, error) {

	var containers []stow.Container

	bucketList, err := l.client.ListBuckets(l.ctx)
	if err != nil {
		return nil, "", errors.Wrap(err, "Containers, listing the buckets")
	}

	// Iterate through the slice of pointers to buckets
	for _, bucket := range bucketList {

		newContainer := &container{
			name:           bucket.Name(),
			client:         l.client,
			bucket: bucket,
			ctx: l.ctx,
		}

		containers = append(containers, newContainer)
	}

	return containers, "", nil
}

// Close simply satisfies the Location interface. There's nothing that
// needs to be done in order to satisfy the interface.
func (l *location) Close() error {
	return nil // nothing to close
}

// Container retrieves a stow.Container based on its name which must be
// exact.
func (l *location) Container(id string) (stow.Container, error) {
	bucket, err := l.client.Bucket(l.ctx, id)
	if err != nil {
		return nil, err
	}

	c:= &container{
		name:           bucket.Name(),
		client:         l.client,
		bucket: bucket,
		ctx: l.ctx,
	}

	return c, nil
}

// RemoveContainer removes a container simply by name.
func (l *location) RemoveContainer(id string) error {
	bucket, err := l.client.Bucket(l.ctx, id)
	if err != nil {
		return err
	}

	return bucket.Delete(l.ctx)
}

// ItemByURL retrieves a stow.Item by parsing the URL, in this
// case an item is an object.
func (l *location) ItemByURL(url *url.URL) (stow.Item, error) {
	return nil, nil
}
