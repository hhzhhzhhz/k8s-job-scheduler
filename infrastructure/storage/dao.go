package storage

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"net/url"
)

var (
	xdb *sqlx.DB
)

func InitMysql(dsn string) (err error) {
	dsn = fmt.Sprintf(dsn, url.QueryEscape("Asia/Shanghai"))
	xdb, err = sqlx.Open("mysql", dsn)
	return err
}

func db() *sqlx.DB {
	return xdb
}

func Close() error {
	return db().Close()
}
