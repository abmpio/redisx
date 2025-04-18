package redis

import (
	"errors"
)

var (
	ErrKeyNotExist = errors.New("redis: key is missing")
	ErrValueIsNil  = errors.New("redis: value is nil")
)

// 访问redis
type IRedisService interface {
	IRedisKeyService
	IRedisStringService
	IRedisHashService
	IRedisSetService
}

type redisService struct {
	IRedisKeyService
	IRedisStringService
	IRedisHashService
	IRedisSetService
}

var _ IRedisService = (*redisService)(nil)

// new一个IRedisService
func NewRedisService(options *RedisOptions) IRedisService {
	s := &redisService{
		IRedisKeyService:    NewRedisKeyService(options),
		IRedisStringService: NewRedisStringService(options),
		IRedisHashService:   NewRedisHashService(options),
		IRedisSetService:    NewRedisSetService(options),
	}
	return s
}
