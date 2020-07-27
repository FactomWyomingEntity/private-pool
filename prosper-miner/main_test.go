package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/spf13/pflag"
)

func TestSelectConfigFile01(t *testing.T) {
	var path = "/does/not/exist"
	var flags = &pflag.FlagSet{}
	initFlagsForTesting(flags)
	flags.Parse([]string{"--config", path})
	fs := fsForTesting()

	configFilePath, configFileSpecified, err := selectConfigFile(flags, fs)

	if configFilePath != path || !configFileSpecified || err != nil {
		t.Errorf("Test of specified config file path %s failed", configFilePath)
	}
}

func TestSelectConfigFile02(t *testing.T) {
	var path = "C:\\Users\\user\\config.file"
	var flags = &pflag.FlagSet{}
	initFlagsForTesting(flags)
	flags.Parse([]string{"-c", path})
	fs := fsForTesting()

	configFilePath, configFileSpecified, err := selectConfigFile(flags, fs)

	if configFilePath != path || !configFileSpecified || err != nil {
		t.Errorf("Test of specified config file path %s failed", configFilePath)
	}
}

func TestSelectConfigFile03(t *testing.T) {
	var flags = &pflag.FlagSet{}
	initFlagsForTesting(flags)
	flags.Parse([]string{})
	ensureEnvHomeIsSetForTesting()
	fs := fsForTesting()
	userConfigFilePath := makeConfigFileForTesting(fs, UserConfigFilePath)

	configFilePath, configFileSpecified, err := selectConfigFile(flags, fs)

	if configFilePath != userConfigFilePath {
		t.Errorf("TestSelectConfigFile03 return incorrect config file path %s", configFilePath)
	} else if configFileSpecified {
		t.Errorf("TestSelectConfigFile03 incorrectly reports that the config file path was specified by the user")
	} else if err != nil {
		t.Errorf("TestSelectConfigFile03 failed: %s", err)
	}
}

func TestSelectConfigFile04(t *testing.T) {
	var flags = &pflag.FlagSet{}
	initFlagsForTesting(flags)
	flags.Parse([]string{})
	ensureEnvHomeIsSetForTesting()
	fs := fsForTesting()
	systemPath, err := getSystemConfigFilePath()
	if err != nil {
		t.Errorf("Failed getting system config file path: %s", err)
	}
	makeConfigFileForTesting(fs, systemPath)

	configFilePath, configFileSpecified, err := selectConfigFile(flags, fs)

	if configFilePath != systemPath {
		t.Errorf("TestSelectConfigFile04 return incorrect config file path %s", configFilePath)
	} else if configFileSpecified {
		t.Errorf("TestSelectConfigFile04 incorrectly reports that the config file path was specified by the user")
	} else if err != nil {
		t.Errorf("TestSelectConfigFile04 failed: %s", err)
	}
}

func initFlagsForTesting(flags *pflag.FlagSet) {
	flags.StringP("config", "c", "", "config path location")
}

func fsForTesting() afero.Fs {
	return afero.NewMemMapFs()
}

func ensureEnvHomeIsSetForTesting() {
	homeDir := "C:/Users/user"
	os.Setenv("HOME", homeDir)
}

func makeConfigFileForTesting(fs afero.Fs, configFilePath string) string {
	cfp := os.ExpandEnv(configFilePath)
	configFileDir, _ := filepath.Split(cfp)
	fs.MkdirAll(configFileDir, 0755)
	afero.WriteFile(fs, cfp, []byte("config file"), 0644)
	return cfp
}
