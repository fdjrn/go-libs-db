package db

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/go-redis/redis/v8"
	redigo "github.com/gomodule/redigo/redis"
)

type redisConf struct {
	Host      string `json:"host"`
	Auth      string `json:"auth"`
	DB        int    `json:"db"`
	MaxActive int    `json:"max_active"`
}

// RediGoConf :
type RediGoConf struct {
	redisConf
	MaxIdle int         `json:"max_idle"`
	Logger  *log.Logger // Optional
}

type GoRedisConf struct {
	redisConf
	Logger *log.Logger // Optional
}

// NewRedisPoolConnection :
// Create new redis connection (client) using redigo library
func NewRedisPoolConnection(host, auth string, dbIndex, maxIdle, maxActive int, nLog *log.Logger) (*redigo.Pool, error) {
	if nLog == nil {
		nLog = log.New(os.Stderr, "", log.LstdFlags)
	}

	redisConf := RediGoConf{
		MaxIdle: maxIdle,
		Logger:  nLog,
	}

	redisConf.Host = host
	redisConf.Auth = auth
	redisConf.DB = dbIndex
	redisConf.MaxActive = maxActive

	redisPool, err := redisConf.initRedis()
	if err != nil {
		return nil, err
	}

	return redisPool, nil
}

// initRedis :
func (p *RediGoConf) initRedis() (*redigo.Pool, error) {
	conn, err := redigo.Dial("tcp", p.Host)
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	if _, err := conn.Do("AUTH", p.Auth); err != nil {
		conn.Close()
		return nil, err
	}

	if _, err := conn.Do("SELECT", p.DB); err != nil {
		conn.Close()
		return nil, err
	}

	redPool := &redigo.Pool{
		MaxIdle:   p.MaxIdle,
		MaxActive: p.MaxActive, // max number of connections
		Dial: func() (redigo.Conn, error) {
			c, err := redigo.Dial("tcp", p.Host)
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("AUTH", p.Auth); err != nil {
				c.Close()
				return nil, err
			}

			if _, err := c.Do("SELECT", p.DB); err != nil {
				c.Close()
				return nil, err
			}
			return c, nil
		},
	}

	return redPool, nil
}

// NewRedisParsedURLClient :
// Create new connection from parsed URL
func NewRedisParsedURLClient(host, auth string, dbIndex, maxActive int, nLog *log.Logger) (*redis.Client, error) {
	if nLog == nil {
		nLog = log.New(os.Stderr, "", log.LstdFlags)
	}

	conf := GoRedisConf{
		Logger: nLog,
	}

	conf.Host = host
	conf.Auth = auth
	conf.DB = dbIndex
	conf.MaxActive = maxActive // set as PoolSize for connection

	redisPool, err := conf.initRedisParsedURLConnection()
	if err != nil {
		return nil, err
	}

	return redisPool, nil
}

// initRedisParsedURLConnection :
// Initialize new redis client from parsed URL
func (p *GoRedisConf) initRedisParsedURLConnection() (*redis.Client, error) {

	urlConn := fmt.Sprintf("rediss://:%s@%s", p.Auth, p.Host)

	optR, err := redis.ParseURL(urlConn)
	if err != nil {
		return nil, err
	}

	optR.DB = p.DB
	optR.PoolSize = p.MaxActive
	client := redis.NewClient(optR)

	err = client.Ping(context.Background()).Err()
	if err != nil {
		return nil, err
	}

	return client, nil
}
