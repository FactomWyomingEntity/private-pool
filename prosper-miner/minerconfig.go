package main

import (
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/kardianos/service"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	ConfigHashTableDirectoryKey = "lxrhash.tabledir"
	ConfigEmailAddressKey = "miner.username"
	ConfigMinerIdKey = "miner.minerid"
	ConfigConcurrentMinersKey = "miner.threads"
	ConfigPoolHostAndPortKey = "pool.host"
)

// This does not validate that the port is less than 65536.
var rxHostAndPort = regexp.MustCompile(":[1-6]?[0-9]{1,4}$")

// Configuration values for the miner
type minerConfig struct {
	emailaddress string
	hashtabledirectory string
	minerid string
	concurrentminers int
	poolhostandport string
}

// Load the configuration values from prosper-miner.toml
func getMinerConfig(path string) (*minerConfig, error) {
	var err error
	path, err = getConfigFilePath(path)
	dir := filepath.Dir(path)
	name := filepath.Base(path)
	extension := filepath.Ext(name)

	if hashtabledir, err := getDefaultHashTableDirectory(); err == nil {
		viper.SetDefault(ConfigHashTableDirectoryKey, hashtabledir)
	} else {
		return nil, err
	}

	viper.AddConfigPath(dir)
	viper.SetConfigName(strings.TrimSuffix(name, extension))

	statinfo, err := os.Stat(path)
	if err != nil || statinfo == nil || os.IsNotExist(err) {
		log.WithFields(log.Fields{"configFilePath": path}).Error("Configuration file could not be read")
		return nil, errors.New("Configuration file could not be read");
	}

	err = viper.ReadInConfig()
	if err != nil {
		log.WithFields(log.Fields{"configFilePath": path}).Error("Failed to read configuration")
		return nil, err
	}

	mc := &minerConfig{}
	mc.hashtabledirectory = viper.GetString(ConfigHashTableDirectoryKey)
	mc.emailaddress = viper.GetString(ConfigEmailAddressKey)
	mc.minerid = viper.GetString(ConfigMinerIdKey)
	mc.concurrentminers = viper.GetInt(ConfigConcurrentMinersKey)
	mc.poolhostandport = viper.GetString(ConfigPoolHostAndPortKey)

	// Validate configuration
	if !strings.Contains(mc.emailaddress, "@") {
		log.WithFields(log.Fields{ConfigEmailAddressKey: mc.emailaddress}).Errorf("%s does not contain '@'", ConfigEmailAddressKey)
		return nil, errors.New(ConfigEmailAddressKey + " should be an e-mail address and must contain '@'")
	}
	if mc.minerid == "" {
		log.WithFields(log.Fields{ConfigMinerIdKey: mc.minerid}).Errorf("%s is an empty string", ConfigMinerIdKey)
		return nil, errors.New(ConfigMinerIdKey + " must not be an empty string")
	}
	if mc.concurrentminers < 1 {
		log.WithFields(log.Fields{ConfigConcurrentMinersKey: mc.concurrentminers}).Errorf("%s must be a positive value", ConfigConcurrentMinersKey)
		return nil, errors.New(ConfigConcurrentMinersKey + " must be a positive value")
	}
	if !rxHostAndPort.MatchString(mc.poolhostandport) {
		log.WithFields(log.Fields{ConfigPoolHostAndPortKey: mc.poolhostandport}).Errorf("%s must contain a port number", ConfigPoolHostAndPortKey)
		return nil, errors.New(ConfigPoolHostAndPortKey + " must contain a port number")
	}

	return mc, nil
}

func getConfigFilePath(path string) (string, error) {
	// First, use the path passed, presuming it is from
	// the user.  If no path is passed, use the path set
	// in the PROSPERPOOL_CONFIG environment variable.  If
	// that value is empty, get the default path for the
	// operating system.
	var err error
	if path == "" {
		path = os.Getenv("PROSPERPOOL_CONFIG")
		if path == "" {
			if service.Interactive() {
				var err error
				path, err = getUserConfigFilePath()
				if err != nil {
					return "", err
				}
				if _, err = os.Stat(path); os.IsNotExist(err) {
					path, err = getSystemConfigFilePath()
					if err != nil {
						return "", err
					}
					log.WithFields(log.Fields{"configFilePath": path}).Debug("Using system config file")
				} else {
					log.WithFields(log.Fields{"configFilePath": path}).Debug("Using user's config file")
				}
			} else {
				path, err = getSystemConfigFilePath()
				if err != nil {
					return "", err
				}
				log.WithFields(log.Fields{"configFilePath": path}).Debug("Using system config file")
			}
		} else {
			log.WithFields(log.Fields{"configFilePath": path}).Debug("Using PROSPERPOOL_CONFIG")
		}
	} else {
		log.WithFields(log.Fields{"configFilePath": path}).Debug("Using user-specified configuration file")
	}
	return path, nil
}

func getUserConfigFilePath() (string, error) {
	currentUser, err := user.Current()
	if err == nil {
		path := filepath.Join(currentUser.HomeDir, ".prosper", "prosper-miner.toml")
		log.WithFields(log.Fields{"configFilePath": path}).Debug("Using user's default configuration file")
		return path, nil
	}
	return "", err
}
