//go:build iam
// +build iam

package s3

import (
	"os"
	"testing"

	"github.com/aldor007/stow"
	"github.com/aldor007/stow/test"
)

func TestStowIAM(t *testing.T) {
	region := os.Getenv("S3REGION")

	if region == "" {
		t.Skip("skipping test because missing S3REGION")
	}

	config := stow.ConfigMap{
		"auth_type": "iam",
		"region":    region,
	}

	test.All(t, "s3", config)
}
