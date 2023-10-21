package message

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/opensourceways/community-robot-lib/kafka"
	"github.com/opensourceways/community-robot-lib/mq"
	"github.com/opensourceways/xihe-script/config"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
)

var topics config.Topics

func Init(cfg mq.MQConfig, log *logrus.Entry, topic config.Topics) error {
	topics = topic

	ca, err := ioutil.ReadFile(cfg.TLSConfig.CAFile)
	if err != nil {
		return err
	}

	if err := os.Remove(cfg.TLSConfig.CAFile); err != nil {
		return err
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return fmt.Errorf("failed to append certs from PEM")
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		RootCAs:            pool,
	}

	err = kafka.Init(
		mq.Addresses(cfg.Addresses...),
		mq.Log(log),
		mq.Secure(true),
		mq.SetTLSConfig(tlsConfig),
	)
	if err != nil {
		return err
	}

	return kafka.Connect()
}

func Exit(log *logrus.Entry) {
	if err := kafka.Disconnect(); err != nil {
		log.Errorf("exit kafka, err:%v", err)
	}
}
