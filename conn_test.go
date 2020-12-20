package db

import (
	"testing"
)

func TestConfig_OpenMysql(t *testing.T) {
	c := &Config{
		Host: "localhost",
		Port: 3306,
		User: "root",
		Password: "sun1990",
		Database: "god",
	}
	_, err := c.OpenMysql()
	if err != nil {
		t.Logf("connect mysql server failed: %s", err.Error())
	}
}