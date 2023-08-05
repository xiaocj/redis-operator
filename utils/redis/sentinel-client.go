package redis

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	rediscli "github.com/go-redis/redis"
)

type SentinelClient interface {
	// Pasing the response of cmd "INFO SENTINEL <master>"，get the sentinel number by pattern “sentinels=([0-9]+)"
	GetNumberSentinelsInMemory(options *rediscli.Options, masterName string) (int32, error)
	// Pasing the response of cmd "INFO SENTINEL <master>"，get the sentinel slaves by pattern “sslaves=([0-9]+)"
	GetNumberSentinelSlavesInMemory(options *rediscli.Options, masterName string) (int32, error)
	// Reset the sentinel status by cmd "INFO RESET <master>"
	ResetSentinel(options *rediscli.Options, masterName string) error
	// Pasing the response of cmd "SENTINEL master <master>"，get the master address
	GetSentinelMonitor(options *rediscli.Options, masterName string) (string, string, error)
	// Apply custom configs by cmd "SENTINEL SET <master>"
	SetCustomSentinelConfig(options *rediscli.Options, masterName string, configs []string) error
	// Get the quorum number by cmd "SENTINEL ckquorum <master>"
	SentinelCheckQuorum(options *rediscli.Options, masterName string) error

	// Monitor a new master by cmd "SENTINEL REMOVE/MONITOR"
	MonitorRedis(options *rediscli.Options, masterName string, masterIp string, masterPort string, masterPass string, quorum int) error
}

func NewSentinelClient() SentinelClient {
	return &sentinelClient{}
}

type sentinelClient struct {
	client
}

const (
	sentinelsNumberREString = "sentinels=([0-9]+)"
	sentinelStatusREString  = "status=([a-z]+)"
	slaveNumberREString     = "slaves=([0-9]+)"
)

var (
	sentinelNumberRE = regexp.MustCompile(sentinelsNumberREString)
	sentinelStatusRE = regexp.MustCompile(sentinelStatusREString)
	slaveNumberRE    = regexp.MustCompile(slaveNumberREString)
)

// GetNumberSentinelsInMemory return the number of sentinels that the requested sentinel for the masterName
func (c *sentinelClient) GetNumberSentinelsInMemory(options *rediscli.Options, masterName string) (int32, error) {
	cli := rediscli.NewClient(options)
	defer cli.Close()

	body, err := cli.Info("sentinel").Result()
	if err != nil {
		return 0, err
	}

	info := matchFirstLine(body, "")
	if info == nil {
		return 0, errors.New("master not found")
	}
	if err2 := isSentinelReady(*info); err2 != nil {
		return 0, err2
	}

	match := sentinelNumberRE.FindStringSubmatch(*info)
	if len(match) == 0 {
		return 0, errors.New("seninel regex not found")
	}
	nSentinels, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, err
	}
	return int32(nSentinels), nil
}

// GetNumberSentinelSlavesInMemory return the number of sentinels that the requested sentinel has
func (c *sentinelClient) GetNumberSentinelSlavesInMemory(options *rediscli.Options, masterName string) (int32, error) {
	cli := rediscli.NewClient(options)
	defer cli.Close()

	body, err := cli.Info("sentinel").Result()
	if err != nil {
		return 0, err
	}

	info := matchFirstLine(body, "")
	if info == nil {
		return 0, errors.New("master not found")
	}
	if err2 := isSentinelReady(*info); err2 != nil {
		return 0, err2
	}

	match := slaveNumberRE.FindStringSubmatch(*info)
	if len(match) == 0 {
		return 0, errors.New("slaves regex not found")
	}
	nSlaves, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, err
	}
	return int32(nSlaves), nil
}

// ResetSentinel sends a sentinel reset * for the given sentinel
func (c *sentinelClient) ResetSentinel(options *rediscli.Options, masterName string) error {
	cli := rediscli.NewClient(options)
	defer cli.Close()

	cmd := rediscli.NewIntCmd("SENTINEL", "RESET", masterName)
	if err := cli.Process(cmd); err != nil {
		return err
	}

	if _, err := cmd.Result(); err != nil {
		return err
	}

	return nil
}

// GetSentinelMonitor return the master IP and port for the given masterName
func (c *sentinelClient) GetSentinelMonitor(options *rediscli.Options, masterName string) (string, string, error) {
	cli := rediscli.NewClient(options)
	defer cli.Close()

	cmd := rediscli.NewSliceCmd("SENTINEL", "master", masterName)
	if err := cli.Process(cmd); err != nil {
		return "", "", err
	}

	if res, err := cmd.Result(); err != nil {
		return "", "", err
	} else {
		masterIP := res[3].(string)
		masterPort := res[5].(string)
		return masterIP, masterPort, nil
	}
}

func (c *sentinelClient) SetCustomSentinelConfig(options *rediscli.Options, masterName string, configs []string) error {
	cli := rediscli.NewClient(options)
	defer cli.Close()

	for _, config := range configs {
		param, value, err := c.getConfigParameters(config)
		if err != nil {
			return err
		}
		if err := c.applySentinelConfig(param, value, masterName, cli); err != nil {
			return err
		}
	}
	return nil
}

func (c *sentinelClient) SentinelCheckQuorum(options *rediscli.Options, masterName string) error {
	cli := rediscli.NewSentinelClient(options)
	defer cli.Close()

	cmd := rediscli.NewStringCmd("SENTINEL", "CKQUORUM", masterName)
	if err := cli.Process(cmd); err != nil {
		return err
	}
	res, err := cmd.Result()
	if err != nil {
		return err
	}

	s := strings.Split(res, " ")
	status := s[0]
	quorum := s[1]

	if status == "" {
		return fmt.Errorf("quorum command result unexpected output")
	}
	if status == "(error)" && quorum == "NOQUORUM" {
		return fmt.Errorf("quorum Not available")

	} else if status == "OK" {
		return nil
	} else {
		return fmt.Errorf("quorum status unexpected %s", status)
	}
}

func (c *sentinelClient) MonitorRedis(options *rediscli.Options, masterName string, masterIp string, masterPort string, masterPass string, quorum int) error {
	cli := rediscli.NewClient(options)
	defer cli.Close()

	cmd := rediscli.NewBoolCmd("SENTINEL", "REMOVE", masterName)
	_ = cli.Process(cmd)
	// We'll continue even if it fails, the priority is to have the redises monitored
	cmd = rediscli.NewBoolCmd(context.TODO(), "SENTINEL", "MONITOR", masterName, masterIp, masterPort, quorum)
	err := cli.Process(cmd)
	if err != nil {
		return err
	}
	_, err = cmd.Result()
	if err != nil {
		return err
	}

	if masterPass != "" {
		cmd = rediscli.NewBoolCmd(context.TODO(), "SENTINEL", "SET", masterName, "auth-pass", masterPass)
		err := cli.Process(cmd)
		if err != nil {
			return err
		}
		_, err = cmd.Result()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *sentinelClient) applySentinelConfig(parameter string, value string, masterName string, rClient *rediscli.Client) error {
	cmd := rediscli.NewStatusCmd("SENTINEL", "set", masterName, parameter, value)
	err := rClient.Process(cmd)
	if err != nil {
		return err
	}
	return cmd.Err()
}

func isSentinelReady(info string) error {
	matchStatus := sentinelStatusRE.FindStringSubmatch(info)
	if len(matchStatus) == 0 || matchStatus[1] != "ok" {
		return errors.New("sentinels not ready")
	}
	return nil
}

func matchFirstLine(multiLines string, keyword string) *string {
	lines := strings.Split(multiLines, "\r\n")
	for _, line := range lines {
		if strings.Contains(line, keyword) {
			return &line
		}
	}

	return nil
}
