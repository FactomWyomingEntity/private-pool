// +build freebsd netbsd openbsd

package main

func getDefaultHashTableDirectory() (string, error) {
	return "/var/db/LXRHash", nil
}

func getSystemConfigFilePath() (string, error) {
	return "/etc/prosper-pool/prosper-pool.toml", nil
}
