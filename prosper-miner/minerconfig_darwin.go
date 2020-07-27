// +build darwin

package main

import (
	"path/filepath"
)

const bundleIdentifier = "io.prosperpool.ProsperMiner"
const lxrhashBundleIdentifier = "org.pegnet.LXRHash"

func getDefaultHashTableDirectory() (string, error) {

	path := filepath.Join("/Library", "Application Support", lxrhashBundleIdentifier)
	return path, nil
}

func getSystemConfigFilePath() (string, error) {

	configFile := "prosper-miner.toml"

	path := filepath.Join("/Library", "Application Support", bundleIdentifier, configFile)
	return path, nil
}
