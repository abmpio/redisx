package redis

import redis "github.com/go-redis/redis/v8"

type IRedisSetService interface {
	// get member list
	SetGetMembers(key string, opts ...RedisValueOption) ([]IRedisValue, error)
	// add member
	SetAddMember(key string, value interface{}, opts ...RedisValueOption) error
	// remove member
	SetRemoveMember(key string, value interface{}, opts ...RedisValueOption) error
	// member is exist
	SetMemberIsExist(key string, value interface{}, opts ...RedisValueOption) (bool, error)
}

type RedisSetService struct {
	*RedisKeyService
}

var _ IRedisSetService = (*RedisSetService)(nil)

func NewRedisSetService(options *RedisOptions) IRedisSetService {
	s := &RedisSetService{
		RedisKeyService: NewRedisKeyService(options),
	}
	return s
}

func (s *RedisSetService) SetGetMembers(key string, opts ...RedisValueOption) ([]IRedisValue, error) {
	options := s.options.createRedisValueOptions()
	options.applyOption(opts...)

	valueList := make([]IRedisValue, 0)
	members, err := s.options.client.SMembers(options.ctx, options.appendKeyPrefix(key)).Result()
	if err != nil {
		if err == redis.Nil {
			return valueList, nil
		}
		return valueList, err
	}
	for _, eachValue := range members {
		currentValue := newRedisValue([]byte(eachValue), s.options.Unmarshal)
		valueList = append(valueList, currentValue)
	}
	return valueList, nil
}

// add member
func (s *RedisSetService) SetAddMember(key string, value interface{}, opts ...RedisValueOption) error {
	options := s.options.createRedisValueOptions()
	options.applyOption(opts...)

	data, err := s.options.Marshal(value)
	if err != nil {
		return err
	}

	err = s.options.client.SAdd(options.ctx, options.appendKeyPrefix(key), data).Err()
	return err
}

// remove member
func (s *RedisSetService) SetRemoveMember(key string, value interface{}, opts ...RedisValueOption) error {
	options := s.options.createRedisValueOptions()
	options.applyOption(opts...)

	data, err := s.options.Marshal(value)
	if err != nil {
		return err
	}

	err = s.options.client.SRem(options.ctx, options.appendKeyPrefix(key), data).Err()
	return err
}

// member is exist
func (s *RedisSetService) SetMemberIsExist(key string, value interface{}, opts ...RedisValueOption) (bool, error) {
	options := s.options.createRedisValueOptions()
	options.applyOption(opts...)

	data, err := s.options.Marshal(value)
	if err != nil {
		return false, err
	}

	return s.options.client.SIsMember(options.ctx, options.appendKeyPrefix(key), data).Result()
}
