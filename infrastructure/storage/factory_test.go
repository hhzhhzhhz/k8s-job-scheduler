package storage

import (
	"context"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/hhzhhzhhz/k8s-job-scheduler/entity"
	"github.com/jmoiron/sqlx"
	"net/url"
	"testing"
)

func Test_connetc_sql(t *testing.T) {
	dsn := fmt.Sprintf("root:1234567@tcp(127.0.0.1:3306)/job_scheduling?charset=utf8&parseTime=True&loc=%s&readTimeout=10s&timeout=30s", url.QueryEscape("Asia/Shanghai"))
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		t.Error(err.Error())
	}
	t.Log(db.Mapper)

	res := &entity.OnceJob{}
	err = db.GetContext(context.Background(), res, "select * from once_job where job_id = ?;", "eab49553-e561-4057-8e61-b51e2cf9f280")
	if err != nil {
		t.Error(err.Error())
		return
	}
	t.Log(res)
}

func Test_connetc_sql_fac(t *testing.T) {
	dsn := fmt.Sprintf("root:1234567@tcp(127.0.0.1:3306)/job_scheduling?charset=utf8&parseTime=True&loc=%s&readTimeout=10s&timeout=30s", url.QueryEscape("Asia/Shanghai"))
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		t.Error(err.Error())
	}
	t.Log(db.Mapper)

	res := &entity.OnceJob{}
	err = db.GetContext(context.Background(), res, "select * from once_job where job_id = ?;", "eab49553-e561-4057-8e61-b51e2cf9f280")
	if err != nil {
		t.Error(err.Error())
		return
	}
	t.Log(res)
}
