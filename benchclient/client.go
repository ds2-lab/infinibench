package benchclient

import (
	"errors"
	"time"

	"github.com/google/uuid"
	infinicache "github.com/mason-leap-lab/infinicache/client"
	"github.com/mason-leap-lab/infinicache/common/logger"
)

const (
	ResultSuccess  = 0
	ResultError    = 1
	ResultNotFound = 2
)

func resultFromError(err error) int {
	switch err {
	case nil:
		return ResultSuccess
	case infinicache.ErrNotFound:
		return ResultNotFound
	default:
		return ResultError
	}
}

var (
	ErrNotSupported = errors.New("not supported")
)

type Client interface {
	EcSet(string, []byte, ...interface{}) (string, error)
	EcGet(string, ...interface{}) (string, infinicache.ReadAllCloser, error)
	Close()
}

type clientSetter func(string, []byte) error
type clientGetter func(string) (infinicache.ReadAllCloser, error)

type defaultClient struct {
	log    logger.ILogger
	setter clientSetter
	getter clientGetter
}

func newDefaultClient(logPrefix string) *defaultClient {
	return newDefaultClientWithAccessor(logPrefix, nil, nil)
}

func newDefaultClientWithAccessor(logPrefix string, setter clientSetter, getter clientGetter) *defaultClient {
	return &defaultClient{
		log: &logger.ColorLogger{
			Verbose: true,
			Level:   logger.LOG_LEVEL_ALL,
			Color:   true,
			Prefix:  logPrefix,
		},
		setter: setter,
		getter: getter,
	}
}

func (c *defaultClient) EcSet(key string, val []byte, args ...interface{}) (string, error) {
	reqId := uuid.New().String()

	// Debuging options
	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if dryrun > 0 {
		return reqId, nil
	}

	if c.setter == nil {
		return reqId, ErrNotSupported
	}

	// Timing
	start := time.Now()
	err := c.setter(key, val)
	duration := time.Since(start)
	nanoLog(logClient, "set", reqId, start.UnixNano(), duration.Nanoseconds(), len(val), resultFromError(err))
	if err != nil {
		c.log.Error("Failed to upload: %v", err)
		return reqId, err
	}
	c.log.Info("Set %s %v", key, duration)
	return reqId, nil
}

func (c *defaultClient) EcGet(key string, args ...interface{}) (string, infinicache.ReadAllCloser, error) {
	reqId := uuid.New().String()

	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if dryrun > 0 {
		return reqId, nil, nil
	}

	if c.getter == nil {
		return reqId, nil, ErrNotSupported
	}

	// Timing
	start := time.Now()
	reader, err := c.getter(key)
	duration := time.Since(start)
	size := 0
	if reader != nil {
		size = reader.Len()
	}
	nanoLog(logClient, "get", reqId, start.UnixNano(), duration.Nanoseconds(), size, resultFromError(err))
	if err != nil {
		c.log.Error("failed to download: %v", err)
		return reqId, nil, err
	}
	c.log.Info("Get %s %v", key, duration)
	return reqId, reader, nil
}

func (c *defaultClient) Close() {
	// Nothing
}
