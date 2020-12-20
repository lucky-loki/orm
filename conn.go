package db

import (
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"time"
)

const (
	defaultMaxIdleConns    = 10
	defaultMaxOpenConns    = 100
	defaultConnMaxLifeTime = 10 * time.Hour
)

// db config
type Config struct {
	// db server config
	Host     string
	Port     int
	User     string
	Password string
	Database string

	// connection pool config
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifeTime time.Duration
}

// check conn pool params
func (c *Config) check() {
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = defaultMaxIdleConns
	}
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = defaultMaxOpenConns
	}
	if c.ConnMaxLifeTime == 0 {
		c.ConnMaxLifeTime = defaultConnMaxLifeTime
	}
}

func (c *Config) formMysqlDSN() string {
	format := "%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local"
	return fmt.Sprintf(format, c.User, c.Password, c.Host, c.Port, c.Database)
}

func (c *Config) formPgDSN() string {
	format := "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable"
	return fmt.Sprintf(format, c.Host, c.Port, c.User, c.Password, c.Database)
}

func (c *Config) setConnPoolParams(db *gorm.DB) {
	db.DB().SetMaxIdleConns(c.MaxIdleConns)
	db.DB().SetMaxOpenConns(c.MaxOpenConns)
	db.DB().SetConnMaxLifetime(c.ConnMaxLifeTime)
}

// connect mysql db server
func (c *Config) OpenMysql() (*gorm.DB, error) {
	c.check()
	dsn := c.formMysqlDSN()
	db, err := gorm.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SingularTable(true)
	c.setConnPoolParams(db)
	return db, nil
}

// connect pg db server
func (c *Config) OpenPostgre() (*gorm.DB, error) {
	c.check()
	dsn := c.formPgDSN()
	db, err := gorm.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SingularTable(true)
	c.setConnPoolParams(db)
	return db, nil
}