package redis

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	rediscli "github.com/go-redis/redis"
)

type client struct {
}

func NewRedisClientOptions(ip *string, port int, pass string) *rediscli.Options {
	return &rediscli.Options{
		Addr:     net.JoinHostPort(*ip, strconv.Itoa(port)),
		Password: pass,
		DB:       0,
	}
}

func (c *client) getConfigParameters(config string) (parameter string, value string, err error) {
	s := strings.Split(config, " ")
	if len(s) < 2 {
		return "", "", fmt.Errorf("configuration '%s' malformed", config)
	}
	if len(s) == 2 && s[1] == `""` {
		return s[0], "", nil
	}
	return s[0], strings.Join(s[1:], " "), nil
}
