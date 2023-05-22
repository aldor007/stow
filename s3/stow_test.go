// build +disabled
package s3

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aldor007/stow"
	"github.com/aldor007/stow/test"
)

func TestStow(t *testing.T) {
	accessKeyId := os.Getenv("S3_ACCESSKEYID")
	secretKey := os.Getenv("S3_SECRETKEY")
	region := os.Getenv("S3_REGION")

	if accessKeyId == "" || secretKey == "" || region == "" {
		t.Skip("skipping test because missing one or more of S3ACCESSKEYID S3SECRETKEY S3REGION")
	}

	config := stow.ConfigMap{
		"access_key_id": accessKeyId,
		"secret_key":    secretKey,
		"region":        region,
	}

	test.All(t, "s3", config)
}

func TestGetItem(t *testing.T) {
	accessKeyId := os.Getenv("S3_ACCESSKEYID")
	secretKey := os.Getenv("S3_SECRETKEY")
	region := os.Getenv("S3_REGION")
	bucket := os.Getenv("S3_BUCKET")
	r := require.New(t)

	if accessKeyId == "" || secretKey == "" || region == "" || bucket == "" {
		t.Skip("skipping test because missing one or more of S3ACCESSKEYID S3SECRETKEY S3REGION")
	}

	config := stow.ConfigMap{
		"access_key_id": accessKeyId,
		"secret_key":    secretKey,
		"region":        region,
		ConfigEndpoint:  "",
	}

	l, err := stow.Dial("s3", config)
	r.NoError(err)
	c, err := l.Container(bucket)
	r.NoError(err)
	i, err := c.Item("demo/cat.jpg")
	r.NoError(err)
	s, err := i.Size()
	r.NoError(err)
	r.Greater(s, int64(100))

}

func TestPreSignedURL(t *testing.T) {
	r := require.New(t)
	accessKeyId := os.Getenv("S3ACCESSKEYID")
	secretKey := os.Getenv("S3SECRETKEY")
	region := os.Getenv("S3REGION")

	if accessKeyId == "" || secretKey == "" || region == "" {
		t.Skip("skipping test because missing one or more of S3ACCESSKEYID S3SECRETKEY S3REGION")
	}

	config := stow.ConfigMap{
		"access_key_id": accessKeyId,
		"secret_key":    secretKey,
		"region":        region,
	}

	location, err := stow.Dial("s3", config)
	r.NoError(err)

	container, err := location.Container("flyte-demo")
	ctx := context.Background()
	res, err := container.PreSignRequest(ctx, stow.ClientMethodPut, "blah/bloh/fileon", stow.PresignRequestParams{
		ExpiresIn: time.Hour,
	})

	r.NoError(err)
	t.Log(res)
	r.NotEmpty(t, res)
}

func TestEtagCleanup(t *testing.T) {
	etagValue := "9c51403a2255f766891a1382288dece4"
	permutations := []string{
		`"%s"`,       // Enclosing quotations
		`W/\"%s\"`,   // Weak tag identifier with escapted quotes
		`W/"%s"`,     // Weak tag identifier with quotes
		`"\"%s"\"`,   // Double quotes, inner escaped
		`""%s""`,     // Double quotes,
		`"W/"%s""`,   // Double quotes with weak identifier
		`"W/\"%s\""`, // Double quotes with weak identifier, inner escaped
	}
	for index, p := range permutations {
		testStr := fmt.Sprintf(p, etagValue)
		cleanTestStr := cleanEtag(testStr)
		if etagValue != cleanTestStr {
			t.Errorf(`Failure at permutation #%d (%s), result: %s`,
				index, permutations[index], cleanTestStr)
		}
	}
}

func TestPrepMetadataSuccess(t *testing.T) {
	r := require.New(t)

	m := make(map[string]string)
	m["one"] = "two"
	m["3"] = "4"
	m["ninety-nine"] = "100"

	m2 := make(map[string]interface{})
	for key, value := range m {
		str := value
		m2[key] = str
	}

	returnedMap, _, err := prepMetadata(m2)
	r.NoError(err)

	if !reflect.DeepEqual(m, returnedMap) {
		t.Error("Expected and returned maps are not equal.")
	}
}

func TestPrepMetadataFailureWithNonStringValues(t *testing.T) {
	r := require.New(t)

	m := make(map[string]interface{})
	m["float"] = 8.9
	m["number"] = 9

	_, _, err := prepMetadata(m)
	r.Error(err)
}

func TestInvalidAuthtype(t *testing.T) {
	r := require.New(t)

	config := stow.ConfigMap{
		"auth_type": "foo",
	}
	_, err := stow.Dial("s3", config)
	r.Error(err)
	r.True(strings.Contains(err.Error(), "invalid auth_type"))
}
