// +build windows

package main

import (
	"path/filepath"

	"golang.org/x/sys/windows"
	log "github.com/sirupsen/logrus"
)

func getDefaultHashTableDirectory() (string, error) {
	pdpath, err := windows.KnownFolderPath(windows.FOLDERID_ProgramData, 0)
	if err != nil {
		log.Error("Unable to find the ProgramData folder")
		return "", err
	}
	path := filepath.Join(pdpath, "LXRHash")
	return path, nil
}

func getSystemConfigFilePath() (string, error) {
	pdpath, err := windows.KnownFolderPath(windows.FOLDERID_ProgramData, 0)
	if err != nil {
		log.Error("Unable to find the ProgramData folder")
		return "", err
	}
	path := filepath.Join(pdpath, "Prosper Pool", "prosper-miner.toml")
	return path, nil
}
