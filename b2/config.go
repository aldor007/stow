package b2

import (
	"net/url"
	"github.com/kurin/blazer/b2"
	"github.com/aldor007/stow"
	"context"
	"fmt"
)

// Kind represents the name of the location/storage type.
const Kind = "b2"

const (

	ConfigAccount = "account"
	ConfigKey= "key"
	ConfigBucket = "bucket"
)

func init() {

	makefn := func(config stow.Config) (stow.Location, error) {
		ctx := context.Background()
		account, _  := config.Config(ConfigAccount)
		key, _ := config.Config(ConfigKey)
		client, err := b2.NewClient(ctx, account, key)
		if err != nil {
			return nil, err
		}
		fmt.Println("init --- ctx ", ctx)
		// Create a location with given config and client (s3 session).
		loc := &location{
			config:         config,
			client:         client,
			ctx: ctx,
		}

		return loc, nil
	}

	kindfn := func(u *url.URL) bool {
		return u.Scheme == Kind
	}

	stow.Register(Kind, makefn, kindfn)
}

