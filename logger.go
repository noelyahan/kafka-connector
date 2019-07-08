package kafka_connect

import "github.com/pickme-go/log"

var Logger = log.Constructor.PrefixedLog(
	log.WithColors(true),
	log.FileDepth(2),
	log.WithLevel(log.TRACE),
	log.WithFilePath(true))
