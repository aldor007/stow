package s3

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"log"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"time"
	"errors"

	"github.com/aldor007/stow"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Kind represents the name of the location/storage type.
const Kind = "s3"

var (
	authTypeAccessKey = "accesskey"
	authTypeIAM       = "iam"
)

const (
	// ConfigAuthType is an optional argument that defines whether to use an IAM role or access key based auth
	ConfigAuthType = "auth_type"

	// ConfigAccessKeyID is one key of a pair of AWS credentials.
	ConfigAccessKeyID = "access_key_id"

	// ConfigSecretKey is one key of a pair of AWS credentials.
	ConfigSecretKey = "secret_key"

	// ConfigToken is an optional argument which is required when providing
	// credentials with temporary access.
	// ConfigToken = "token"

	// ConfigRegion represents the region/availability zone of the session.
	ConfigRegion = "region"

	// ConfigEndpoint is optional config value for changing s3 endpoint
	// used for e.g. minio.io
	ConfigEndpoint = "endpoint"

	// ConfigHTTPTracing enable verbose logs for http requests
	ConfigHTTPTracing = "false"
)


// transport is an http.RoundTripper that keeps track of the in-flight
// request and implements hooks to report HTTP tracing events.
type tracingTransport struct {
	transport http.RoundTripper
}

// RoundTrip wraps http.DefaultTransport.RoundTrip to keep track
// of the current request.
func (t *tracingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	trace := &httptrace.ClientTrace{
		PutIdleConn: func(err error) {
			if err != nil {
				log.Printf("REQ_TRACE Method=%s Put Idle Conn: %v\n", req.Method, err)
			}
		},
		GotConn: func(info httptrace.GotConnInfo) {
			if !info.Reused {
				log.Printf("REQ_TRACE Method=%s GotConn: %+v NEW_CONN\n", req.Method, info)
			}
			log.Printf("REQ_TRACE Method=%s GotConn: %+v\n", req.Method, info)
		},
		ConnectStart: func(network, addr string) {
			log.Printf("REQ_TRACE Method=%s ConnectStart\n", req.Method)
		},
		ConnectDone: func(network, addr string, err error) {
			log.Printf("REQ_TRACE Method=%s ConnectDone: %v\n", req.Method, err)
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	return t.transport.RoundTrip(req)
}

func init() {
	validatefn := func(config stow.Config) error {
		authType, ok := config.Config(ConfigAuthType)
		if !ok || authType == "" {
			authType = authTypeAccessKey
		}

		if !(authType == authTypeAccessKey || authType == authTypeIAM) {
			return errors.New("invalid auth_type")
		}

		if authType == authTypeAccessKey {
			_, ok := config.Config(ConfigAccessKeyID)
			if !ok {
				return errors.New("missing Access Key ID")
			}

			_, ok = config.Config(ConfigSecretKey)
			if !ok {
				return errors.New("missing Secret Key")
			}
		}
		return nil
	}
	makefn := func(config stow.Config) (stow.Location, error) {

		authType, ok := config.Config(ConfigAuthType)
		if !ok || authType == "" {
			authType = authTypeAccessKey
		}

		if !(authType == authTypeAccessKey || authType == authTypeIAM) {
			return nil, errors.New("invalid auth_type")
		}

		if authType == authTypeAccessKey {
			_, ok := config.Config(ConfigAccessKeyID)
			if !ok {
				return nil, errors.New("missing Access Key ID")
			}

			_, ok = config.Config(ConfigSecretKey)
			if !ok {
				return nil, errors.New("missing Secret Key")
			}
		}

		// Create a new client (s3 session)
		client, endpoint, err := newS3Client(config, "")
		if err != nil {
			return nil, err
		}

		// Create a location with given config and client (s3 session).
		loc := &location{
			config:         config,
			client:         client,
			customEndpoint: endpoint,
		}

		return loc, nil
	}

	kindfn := func(u *url.URL) bool {
		return u.Scheme == Kind
	}

	stow.Register(Kind, makefn, kindfn, validatefn)
}

// Attempts to create a session based on the information given.
func newS3Client(config stow.Config, region string) (client *s3.Client, endpoint string, err error) {
	authType, _ := config.Config(ConfigAuthType)
	accessKeyID, _ := config.Config(ConfigAccessKeyID)
	secretKey, _ := config.Config(ConfigSecretKey)
	//	token, _ := config.Config(ConfigToken)

	if authType == "" {
		authType = authTypeAccessKey
	}
	transport := http.RoundTripper(&http.Transport{
		Dial: (&net.Dialer{
			Timeout:   4 * time.Second,
			KeepAlive: 60 * time.Second,
		}).Dial,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 3 * time.Second,
		MaxIdleConns:        200,
		// This number must be tuned for highly loaded servers
		// to prevent spawning new connections every time
		// when there is a need for have bigger number of concurrent connections.
		// The default value of 2 is to low for such servers.
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout: 60 * time.Second,
	})
	if _, ok := config.Config(ConfigHTTPTracing); ok  {
		transport = &tracingTransport{
			transport: transport,
		}
	}
	c := &http.Client{
		Transport: transport,
	}

	awsCfgOpts := make([]func(*awsConfig.LoadOptions) error, 0)
	if region == "" {
		region, _ = config.Config(ConfigRegion)
	}
	if region != "" {
		awsCfgOpts = append(awsCfgOpts,	awsConfig.WithRegion(region))
	} else {
		awsCfgOpts = append(awsCfgOpts,	awsConfig.WithRegion("us-east-1"))
	}

	if authType == authTypeAccessKey {
		awsCfgOpts = append(awsCfgOpts,	awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, "")))
	}

	endpoint, ok := config.Config(ConfigEndpoint)
	if ok && endpoint != "" {
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               endpoint,
				HostnameImmutable: true,
		}, nil
		})
		
		awsCfgOpts = append(awsCfgOpts, awsConfig.WithEndpointResolverWithOptions(resolver))
	}

	awsCfgOpts = append(awsCfgOpts, awsConfig.WithHTTPClient(c),		awsConfig.WithRetryMaxAttempts(3))
	awsCfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsCfgOpts...)

	if err != nil {
		return nil,endpoint, err
	}


	s3Client := s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		if endpoint, ok := config.Config(ConfigEndpoint); ok && endpoint != "" {
			options.UsePathStyle = true
		}
	})

	return s3Client, endpoint, nil
}
