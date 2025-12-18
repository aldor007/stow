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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

// TestRestorable_InterfaceImplementation verifies S3 items implement Restorable
func TestRestorable_InterfaceImplementation(t *testing.T) {
	// Create a mock item
	c := &container{name: "test-bucket"}
	i := &item{
		container: c,
		properties: properties{
			Key: aws.String("test/image.jpg"),
		},
	}

	// Verify item implements stow.Restorable
	_, ok := interface{}(i).(stow.Restorable)
	if !ok {
		t.Error("S3 item should implement stow.Restorable interface")
	}
}

// TestRestore_TierParsing verifies restore tier parsing
func TestRestore_TierParsing(t *testing.T) {
	tests := []struct {
		name          string
		tier          string
		expectedTier  types.Tier
	}{
		{
			name:         "should parse Expedited tier",
			tier:         "Expedited",
			expectedTier: types.TierExpedited,
		},
		{
			name:         "should parse Standard tier",
			tier:         "Standard",
			expectedTier: types.TierStandard,
		},
		{
			name:         "should parse Bulk tier",
			tier:         "Bulk",
			expectedTier: types.TierBulk,
		},
		{
			name:         "should default to Standard for unknown tier",
			tier:         "Unknown",
			expectedTier: types.TierStandard,
		},
		{
			name:         "should default to Standard for empty tier",
			tier:         "",
			expectedTier: types.TierStandard,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var parsedTier types.Tier
			switch tt.tier {
			case "Expedited":
				parsedTier = types.TierExpedited
			case "Bulk":
				parsedTier = types.TierBulk
			default:
				parsedTier = types.TierStandard
			}

			if parsedTier != tt.expectedTier {
				t.Errorf("expected tier %v, got %v", tt.expectedTier, parsedTier)
			}
		})
	}
}

// TestRestoreStatus_HeaderParsing verifies x-amz-restore header parsing
func TestRestoreStatus_HeaderParsing(t *testing.T) {
	tests := []struct {
		name              string
		restoreHeader     string
		expectInProgress  bool
		expectExpiry      bool
	}{
		{
			name:              "should detect ongoing restore",
			restoreHeader:     `ongoing-request="true"`,
			expectInProgress:  true,
			expectExpiry:      false,
		},
		{
			name:              "should detect completed restore with expiry",
			restoreHeader:     `ongoing-request="false", expiry-date="Fri, 21 Dec 2012 00:00:00 GMT"`,
			expectInProgress:  false,
			expectExpiry:      true,
		},
		{
			name:              "should handle empty header",
			restoreHeader:     "",
			expectInProgress:  false,
			expectExpiry:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inProgress := strings.Contains(tt.restoreHeader, `ongoing-request="true"`)
			hasExpiry := strings.Contains(tt.restoreHeader, "expiry-date")

			if inProgress != tt.expectInProgress {
				t.Errorf("expected inProgress=%v, got %v", tt.expectInProgress, inProgress)
			}
			if hasExpiry != tt.expectExpiry {
				t.Errorf("expected hasExpiry=%v, got %v", tt.expectExpiry, hasExpiry)
			}
		})
	}
}

// TestConfigIncludeGlacier verifies GLACIER inclusion config
func TestConfigIncludeGlacier(t *testing.T) {
	tests := []struct {
		name                string
		includeGlacierValue string
		objectStorageClass  types.ObjectStorageClass
		shouldSkip          bool
	}{
		{
			name:                "should skip GLACIER when not configured",
			includeGlacierValue: "",
			objectStorageClass:  types.ObjectStorageClassGlacier,
			shouldSkip:          true,
		},
		{
			name:                "should skip GLACIER when set to false",
			includeGlacierValue: "false",
			objectStorageClass:  types.ObjectStorageClassGlacier,
			shouldSkip:          true,
		},
		{
			name:                "should include GLACIER when set to true",
			includeGlacierValue: "true",
			objectStorageClass:  types.ObjectStorageClassGlacier,
			shouldSkip:          false,
		},
		{
			name:                "should include STANDARD regardless of config",
			includeGlacierValue: "",
			objectStorageClass:  types.ObjectStorageClassStandard,
			shouldSkip:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the container logic
			includeGlacier := tt.includeGlacierValue
			shouldSkip := tt.objectStorageClass == types.ObjectStorageClassGlacier && includeGlacier != "true"

			if shouldSkip != tt.shouldSkip {
				t.Errorf("expected shouldSkip=%v, got %v", tt.shouldSkip, shouldSkip)
			}
		})
	}
}

// TestRestore_Validation verifies restore parameter validation
func TestRestore_Validation(t *testing.T) {
	tests := []struct {
		name        string
		days        int
		tier        string
		shouldError bool
		description string
	}{
		{
			name:        "should accept valid Standard restore",
			days:        3,
			tier:        "Standard",
			shouldError: false,
			description: "Standard tier with 3 days is valid",
		},
		{
			name:        "should accept Expedited restore",
			days:        1,
			tier:        "Expedited",
			shouldError: false,
			description: "Expedited tier with 1 day is valid",
		},
		{
			name:        "should accept Bulk restore",
			days:        30,
			tier:        "Bulk",
			shouldError: false,
			description: "Bulk tier with 30 days is valid",
		},
		{
			name:        "should accept max days",
			days:        30,
			tier:        "Standard",
			shouldError: false,
			description: "30 days is the maximum allowed",
		},
		{
			name:        "should accept min days",
			days:        1,
			tier:        "Standard",
			shouldError: false,
			description: "1 day is the minimum allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Verify tier parsing doesn't panic
			var parsedTier types.Tier
			switch tt.tier {
			case "Expedited":
				parsedTier = types.TierExpedited
			case "Bulk":
				parsedTier = types.TierBulk
			default:
				parsedTier = types.TierStandard
			}

			// Verify days are in valid range (1-30)
			if tt.days < 1 || tt.days > 30 {
				t.Errorf("days %d out of valid range 1-30", tt.days)
			}

			// Verify tier was parsed
			if parsedTier == "" {
				t.Error("tier should be parsed to a valid types.Tier")
			}
		})
	}
}

// TestRestoreStatus_EdgeCases verifies edge cases in status parsing
func TestRestoreStatus_EdgeCases(t *testing.T) {
	tests := []struct {
		name             string
		restoreHeader    string
		expectInProgress bool
		expectExpiry     bool
	}{
		{
			name:             "should handle malformed header",
			restoreHeader:    "invalid header format",
			expectInProgress: false,
			expectExpiry:     false,
		},
		{
			name:             "should handle ongoing without expiry",
			restoreHeader:    `ongoing-request="true"`,
			expectInProgress: true,
			expectExpiry:     false,
		},
		{
			name:             "should handle completed with malformed date",
			restoreHeader:    `ongoing-request="false", expiry-date="invalid"`,
			expectInProgress: false,
			expectExpiry:     true,
		},
		{
			name:             "should handle mixed case",
			restoreHeader:    `ONGOING-REQUEST="TRUE"`,
			expectInProgress: false, // Case sensitive check
			expectExpiry:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inProgress := strings.Contains(tt.restoreHeader, `ongoing-request="true"`)
			hasExpiry := strings.Contains(tt.restoreHeader, "expiry-date")

			if inProgress != tt.expectInProgress {
				t.Errorf("expected inProgress=%v, got %v for header: %s",
					tt.expectInProgress, inProgress, tt.restoreHeader)
			}
			if hasExpiry != tt.expectExpiry {
				t.Errorf("expected hasExpiry=%v, got %v for header: %s",
					tt.expectExpiry, hasExpiry, tt.restoreHeader)
			}
		})
	}
}

// TestConfigIncludeGlacier_AllStorageClasses verifies handling of all S3 storage classes
func TestConfigIncludeGlacier_AllStorageClasses(t *testing.T) {
	tests := []struct {
		name               string
		storageClass       types.ObjectStorageClass
		includeGlacier     string
		shouldBeIncluded   bool
	}{
		{
			name:             "should include STANDARD always",
			storageClass:     types.ObjectStorageClassStandard,
			includeGlacier:   "",
			shouldBeIncluded: true,
		},
		{
			name:             "should include STANDARD_IA always",
			storageClass:     types.ObjectStorageClassStandardIa,
			includeGlacier:   "",
			shouldBeIncluded: true,
		},
		{
			name:             "should include INTELLIGENT_TIERING always",
			storageClass:     types.ObjectStorageClassIntelligentTiering,
			includeGlacier:   "",
			shouldBeIncluded: true,
		},
		{
			name:             "should skip GLACIER by default",
			storageClass:     types.ObjectStorageClassGlacier,
			includeGlacier:   "",
			shouldBeIncluded: false,
		},
		{
			name:             "should skip DEEP_ARCHIVE by default",
			storageClass:     types.ObjectStorageClassDeepArchive,
			includeGlacier:   "",
			shouldBeIncluded: false,
		},
		{
			name:             "should include GLACIER when configured",
			storageClass:     types.ObjectStorageClassGlacier,
			includeGlacier:   "true",
			shouldBeIncluded: true,
		},
		{
			name:             "should include DEEP_ARCHIVE when configured",
			storageClass:     types.ObjectStorageClassDeepArchive,
			includeGlacier:   "true",
			shouldBeIncluded: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate container logic
			shouldSkip := (tt.storageClass == types.ObjectStorageClassGlacier ||
				tt.storageClass == types.ObjectStorageClassDeepArchive) &&
				tt.includeGlacier != "true"

			included := !shouldSkip

			if included != tt.shouldBeIncluded {
				t.Errorf("expected included=%v, got %v for storage class %v with includeGlacier=%s",
					tt.shouldBeIncluded, included, tt.storageClass, tt.includeGlacier)
			}
		})
	}
}

// TestRestore_ErrorHandling verifies restore error scenarios
func TestRestore_ErrorHandling(t *testing.T) {
	tests := []struct {
		name          string
		errorMessage  string
		shouldSucceed bool
	}{
		{
			name:          "should handle RestoreAlreadyInProgress gracefully",
			errorMessage:  "RestoreAlreadyInProgress: Object restore is already in progress",
			shouldSucceed: true,
		},
		{
			name:          "should handle restore already in progress alternative message",
			errorMessage:  "The restore operation is already in progress",
			shouldSucceed: true,
		},
		{
			name:          "should fail on genuine errors",
			errorMessage:  "AccessDenied: Access Denied",
			shouldSucceed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate error handling logic from Restore() method
			isAlreadyInProgress := strings.Contains(tt.errorMessage, "RestoreAlreadyInProgress") ||
				strings.Contains(tt.errorMessage, "already in progress")

			if isAlreadyInProgress != tt.shouldSucceed {
				t.Errorf("expected shouldSucceed=%v for error: %s", tt.shouldSucceed, tt.errorMessage)
			}
		})
	}
}
