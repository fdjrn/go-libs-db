package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jinzhu/gorm" // Gorm Connections
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/lib/pq" // Native PostgreSQL Connections
)

type pgConf struct {
	Host   string      `json:"host"`
	Port   int         `json:"port"`
	User   string      `json:"user"`
	Pass   string      `json:"pass"`
	DB     string      `json:"db"`
	Logger *log.Logger // Optional
}

// PGConnection :
type PGConnection struct {
	pgConf
	Connection *sql.DB
}

// GORMConnection :
type GORMConnection struct {
	pgConf
	Connection *gorm.DB
}

// Close :
func (p *PGConnection) Close() error {
	err := p.Connection.Close()
	if err != nil {
		return err
	}

	return nil
}

func (a *GORMConnection) Close() error {
	err := a.Connection.Close()

	return err
}

// conditions :
type conditions struct {
	Operator string      `json:"operator"`
	Value1   interface{} `json:"value_1"`
	Value2   interface{} `json:"value_2"`
}

// PGFiltering :
func PGFiltering(db *gorm.DB, conditions map[string]conditions) *gorm.DB {
	db.Error = nil

	for key, value := range conditions {
		switch strings.ToLower(value.Operator) {
		case "lt", "lte", "gt", "gte", "eq", "ne":
			db = normalOperator(db, key, value)
		case "like":
			// key ILIKE val_1
			db = likeOperator(db, key, value)
		case "rng":
			// key BETWEEN val_1 AND val_2
			db = rangeOperator(db, key, value)
		case "rne":
			// key > val_1 AND key < val_2
			db = rangeNotEqualOperator(db, key, value)
		case "nir":
			// key NOT BETWEEN val_1 AND val_2
			db = notInRangeOperator(db, key, value)
		default:
			db.Error = fmt.Errorf("unrecognized operator")
		}
	}

	return db
}

// normalOperator :
func normalOperator(db *gorm.DB, key string, condition conditions) *gorm.DB {
	operator := strings.ToLower(condition.Operator)
	switch operator {
	case "lt":
		operator = "<"
	case "lte":
		operator = "<="
	case "gt":
		operator = ">"
	case "gte":
		operator = ">="
	case "eq":
		operator = "="
	case "ne":
		operator = "!="
	}
	statement := fmt.Sprintf("%s %s ?", key, operator)
	db = db.Where(statement, condition.Value1)

	return db
}

// likeOperator :
func likeOperator(db *gorm.DB, key string, condition conditions) *gorm.DB {
	val, ok := condition.Value1.(string)
	if !ok {
		db.Error = fmt.Errorf("value_1 of %s must be of string type", key)
		return db
	}
	statement := fmt.Sprintf("%s ILIKE ?", key)
	db = db.Where(statement, "%"+val+"%")

	return db
}

// rangeOperator :
func rangeOperator(db *gorm.DB, key string, condition conditions) *gorm.DB {
	_, ok := condition.Value1.(float64)
	if !ok {
		db.Error = fmt.Errorf("value_1 of %s must be of integer or float type", key)
		return db
	}
	_, ok = condition.Value2.(float64)
	if !ok {
		db.Error = fmt.Errorf("value_2 of %s must be of integer or float type", key)
		return db
	}
	statement := fmt.Sprintf("%s BETWEEN ? AND ?", key)
	db = db.Where(statement, condition.Value1, condition.Value2)

	return db
}

// rangeNotEqualOperator :
func rangeNotEqualOperator(db *gorm.DB, key string, condition conditions) *gorm.DB {
	_, ok := condition.Value1.(float64)
	if !ok {
		db.Error = fmt.Errorf("value_1 of %s must be of integer or float type", key)
		return db
	}
	_, ok = condition.Value2.(float64)
	if !ok {
		db.Error = fmt.Errorf("value_2 of %s must be of integer or float type", key)
		return db
	}
	statement := fmt.Sprintf("%s > ? AND %s < ?", key, key)
	db = db.Where(statement, condition.Value1, condition.Value2)

	return db
}

// notInRangeOperator :
func notInRangeOperator(db *gorm.DB, key string, condition conditions) *gorm.DB {
	_, ok := condition.Value1.(float64)
	if !ok {
		db.Error = fmt.Errorf("value_1 of %s must be of integer or float type", key)
		return db
	}
	_, ok = condition.Value2.(float64)
	if !ok {
		db.Error = fmt.Errorf("value_2 of %s must be of integer or float type", key)
		return db
	}
	statement := fmt.Sprintf("%s NOT BETWEEN ? AND ?", key)
	db = db.Where(statement, condition.Value1, condition.Value2)

	return db
}

// NewPGConnection :
func NewPGConnection(host string, port int, user, pass, db string, nLog *log.Logger) (*PGConnection, error) {
	if nLog == nil {
		nLog = log.New(os.Stderr, "", log.LstdFlags)
	}

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, pass, db)

	postgreConn, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	err = postgreConn.Ping()
	if err != nil {
		return nil, err
	}

	var connection = PGConnection{}
	connection.Host = host
	connection.Port = port
	connection.User = user
	connection.Pass = pass
	connection.DB = db
	connection.Logger = nLog
	connection.Connection = postgreConn

	return &connection, nil
}

// NewGORMConnection :
func NewGORMConnection(host string, port int, user, pass, db string, nLog *log.Logger) (*GORMConnection, error) {
	if nLog == nil {
		nLog = log.New(os.Stderr, "", log.LstdFlags)
	}

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, pass, db)
	gormConnPostgre, err := gorm.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	var connection = GORMConnection{}
	connection.Connection = gormConnPostgre
	connection.Host = host
	connection.Port = port
	connection.User = user
	connection.DB = db
	connection.Logger = nLog

	return &connection, nil
}
