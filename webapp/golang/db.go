package xsuportal

import (
	"database/sql"
	"log"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/isucon/isucon10-final/webapp/golang/util"
)

func GetDB() (*sqlx.DB, error) {
	mysqlConfig := mysql.NewConfig()
	mysqlConfig.Net = "tcp"
	mysqlConfig.Addr = util.GetEnv("MYSQL_HOSTNAME", "127.0.0.1") + ":" + util.GetEnv("MYSQL_PORT", "3306")
	mysqlConfig.User = util.GetEnv("MYSQL_USER", "isucon")
	mysqlConfig.Passwd = util.GetEnv("MYSQL_PASS", "isucon")
	mysqlConfig.DBName = util.GetEnv("MYSQL_DATABASE", "xsuportal")
	mysqlConfig.Params = map[string]string{
		"time_zone": "'+00:00'",
	}
	mysqlConfig.ParseTime = true

	dbx, err := sqlx.Open(tracedDriver("mysql"), mysqlConfig.FormatDSN())
	if err != nil {
		return dbx, err
	}
	initDB(dbx.DB)
	return dbx, err
}

func initDB(db *sql.DB) {
	waitDB(db)
	go pollDB(db)

	db.SetConnMaxLifetime(10 * time.Second)
	db.SetMaxIdleConns(128)
	db.SetMaxOpenConns(128)
}

func waitDB(db *sql.DB) {
	for {
		err := db.Ping()
		if err == nil {
			return
		}

		log.Printf("Failed to ping DB: %s", err)
		log.Println("Retrying...")
		time.Sleep(time.Second)
	}
}

func pollDB(db *sql.DB) {
	for {
		err := db.Ping()
		if err != nil {
			log.Printf("Failed to ping DB: %s", err)
		}

		time.Sleep(time.Second)
	}
}
