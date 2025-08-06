package redis

import (
	"github.com/go-redis/redis/v8"
)

type IRedisHashService interface {
	// get field value in hash key
	HashGet(key string, field string, opts ...RedisValueOption) IRedisValue
	HashGetAll(key string, opts ...RedisValueOption) (RedisValueMap, error)

	HashSetOne(key string, field string, value interface{}, opts ...RedisValueOption) error
	HashSet(key string, values map[string]interface{}, opts ...RedisValueOption) error

	// delete field in hash key
	HashDelete(key string, fields ...string) error
}

type RedisHashService struct {
	*RedisKeyService
}

func NewRedisHashService(options *RedisOptions) IRedisHashService {
	s := &RedisHashService{
		RedisKeyService: NewRedisKeyService(options),
	}
	return s
}

// get field from hash
func (s *RedisHashService) HashGet(key string, field string, opts ...RedisValueOption) IRedisValue {
	options := s.options.createRedisValueOptions()
	options.applyOption(opts...)

	b, err := s.options.client.HGet(options.ctx, options.appendKeyPrefix(key), field).Bytes()
	if err != nil {
		if err == redis.Nil {
			return newNilRedisValue()
		}
		return newErrRedisValue(err)
	}
	return newRedisValue(b, s.options.Unmarshal)
}

// get all value from hash
func (s *RedisHashService) HashGetAll(key string, opts ...RedisValueOption) (RedisValueMap, error) {
	options := s.options.createRedisValueOptions()
	options.applyOption(opts...)

	result := RedisValueMap{}
	b := s.options.client.HGetAll(options.ctx, options.appendKeyPrefix(key))
	if err := b.Err(); err != nil {
		if err == redis.Nil {
			return result, nil
		}
		return nil, err
	}

	valueList := b.Val()
	if len(valueList) <= 0 {
		return result, nil
	}

	for eachKey, eachValue := range valueList {
		result[eachKey] = newRedisValue([]byte(eachValue), s.options.Unmarshal)
	}
	return result, nil
}

func (s *RedisHashService) HashSet(key string, values map[string]interface{}, opts ...RedisValueOption) error {
	options := s.options.createRedisValueOptions()
	options.applyOption(opts...)

	data := make(map[string]interface{})
	for eachKey, eachValue := range values {
		currentSValue, err := s.options.Marshal(eachValue)
		if err != nil {
			return err
		}
		data[eachKey] = string(currentSValue)
	}
	//有效期
	ttl := Redis_NoExpiration_TTL
	if options.ttl != nil {
		ttl = *options.ttl
	}
	handledKey := options.appendKeyPrefix(key)
	if ttl == Redis_NoExpiration_TTL {
		return s.options.client.HSet(options.ctx, handledKey, data).Err()
	} else {
		pipe := s.options.client.TxPipeline()
		pipe.HSet(options.ctx, handledKey, data)
		pipe.Expire(options.ctx, handledKey, ttl)
		_, err := pipe.Exec(options.ctx)
		return err
	}
}

func (s *RedisHashService) HashSetOne(key string, field string, value interface{}, opts ...RedisValueOption) error {
	options := s.options.createRedisValueOptions()
	options.applyOption(opts...)
	currentSValue, err := s.options.Marshal(value)
	if err != nil {
		return err
	}
	return s.options.client.HSet(options.ctx, options.appendKeyPrefix(key), field, currentSValue).Err()
}

// delete field in hash key
func (s *RedisHashService) HashDelete(key string, fields ...string) error {
	options := s.options.createRedisValueOptions()
	options.applyOption()

	return s.options.client.HDel(options.ctx, options.appendKeyPrefix(key), fields...).Err()
}
