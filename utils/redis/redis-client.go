package redis

import (
	"regexp"
	"strings"

	rediscli "github.com/go-redis/redis"
	"github.com/pkg/errors"
)

// Client defines the functions neccesary to connect to redis and sentinel to get or set what we nned
type RedisClient interface {
	// Parsing the response of cmd "INFO REPLICATION"，check status by pattern “master_sync_in_progress"，"master_link_status“
	SlaveIsReady(options *rediscli.Options) (bool, error)
	// Parsing the response of cmd "INFO REPLICATION"，get master address by pattern “master_host:([0-9.]+)”
	GetSlaveOf(options *rediscli.Options) (string, error)
	// Parsing the response of cmd "INFO REPLICATION"，check whether is a master by pattern “role:master”
	IsMaster(options *rediscli.Options) (bool, error)
	// Set as master redis by cmd "SLAVEOF NO ONE"
	MakeMaster(options *rediscli.Options) error
	// Set as slave redis by cmd "SLAVEOF <ip:port>"
	MakeSlaveOf(options *rediscli.Options, masterIP string, masterPort string) error

	// Apply custom configs by cmd "CONFIG SET"
	SetCustomRedisConfig(options *rediscli.Options, configs []string) error
}

func NewRedisClient() RedisClient {
	return &redisClient{}
}

type redisClient struct {
	client
}

const (
	redisMasterHostREString = "master_host:([0-9.]+)"
	redisRoleMaster         = "role:master"
	redisSyncing            = "master_sync_in_progress:1"
	redisMasterSillPending  = "master_host:127.0.0.1"
	redisLinkUp             = "master_link_status:up"
)

var (
	redisMasterHostRE = regexp.MustCompile(redisMasterHostREString)
)

// GetSlaveOf returns the master of the given redis, or nil if it's master
func (c *redisClient) GetSlaveOf(options *rediscli.Options) (string, error) {
	cli := rediscli.NewClient(options)
	defer cli.Close()
	info, err := cli.Info("replication").Result()
	if err != nil {
		return "", errors.WithMessage(err, "Failed to get info replication while querying redis")
	}

	match := redisMasterHostRE.FindStringSubmatch(info)
	if len(match) == 0 {
		return "", nil
	}
	return match[1], nil
}

func (c *redisClient) IsMaster(options *rediscli.Options) (bool, error) {
	cli := rediscli.NewClient(options)
	defer cli.Close()

	if info, err := cli.Info("replication").Result(); err != nil {
		return false, err
	} else {
		return strings.Contains(info, redisRoleMaster), nil
	}
}

func (c *redisClient) MakeMaster(options *rediscli.Options) error {
	cli := rediscli.NewClient(options)
	defer cli.Close()

	if res := cli.SlaveOf("NO", "ONE"); res.Err() != nil {
		return res.Err()
	}
	return nil
}

func (c *redisClient) MakeSlaveOf(options *rediscli.Options, masterIP string, masterPort string) error {
	cli := rediscli.NewClient(options)
	defer cli.Close()

	if res := cli.SlaveOf(masterIP, masterPort); res.Err() != nil {
		return res.Err()
	}
	return nil
}

func (c *redisClient) SetCustomRedisConfig(options *rediscli.Options, configs []string) error {
	cli := rediscli.NewClient(options)
	defer cli.Close()

	for _, config := range configs {
		param, value, err := c.getConfigParameters(config)
		if err != nil {
			return err
		}
		// If the configuration is an empty line , it will result in an incorrect configSet, which will not run properly down the line.
		// `config set save ""` should support
		if strings.TrimSpace(param) == "" {
			continue
		}
		if err := c.applyRedisConfig(param, value, cli); err != nil {
			return err
		}
	}
	return nil
}

func (c *redisClient) applyRedisConfig(parameter string, value string, cli *rediscli.Client) error {
	result := cli.ConfigSet(parameter, value)
	if nil != result.Err() {
		return result.Err()
	}
	return result.Err()
}

func (c *redisClient) SlaveIsReady(options *rediscli.Options) (bool, error) {
	cli := rediscli.NewClient(options)
	defer cli.Close()

	if info, err := cli.Info("replication").Result(); err != nil {
		return false, err
	} else {
		ok := !strings.Contains(info, redisSyncing) &&
			!strings.Contains(info, redisMasterSillPending) &&
			strings.Contains(info, redisLinkUp)
		return ok, nil
	}
}
