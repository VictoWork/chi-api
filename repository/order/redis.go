package order

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/victowork/chi-api/model"
)

type RedisRepo struct {
	Client *redis.Client
}

func OrderIdKey(id uint64) string {
	return fmt.Sprintf("order:%d", id)
}

func (r *RedisRepo) Insert(ctx context.Context, order model.Order) error {
	data, err := json.Marshal(order)

	if err != nil {
		return fmt.Errorf("failed to decode order: %w", err)
	}

	key := OrderIdKey(order.OrderID)

	txn := r.Client.TxPipeline()

	res := r.Client.SetNX(ctx, key, string(data), 0)

	if err = res.Err(); err != nil {
		txn.Discard()
		return fmt.Errorf("failed to set data in redis: %w", err)
	}

	if err = r.Client.SAdd(ctx, "orders", key).Err(); err != nil {
		txn.Discard()
		return fmt.Errorf("failed to add redis orderid set: %w", err)
	}

	if _, err := txn.Exec(ctx); err != nil {
		return fmt.Errorf("error executing redis transaction in insert order :%w", err)
	}

	return nil
}

var ErrNotExits = errors.New("order does not exists")

func (r *RedisRepo) FindByID(ctx context.Context, id uint64) (model.Order, error) {

	key := OrderIdKey(id)

	value, err := r.Client.Get(ctx, key).Result()

	if errors.Is(err, redis.Nil) {
		return model.Order{}, ErrNotExits
	} else if err != nil {
		return model.Order{}, fmt.Errorf("redis get error: %w", err)
	}

	var order model.Order
	err = json.Unmarshal([]byte(value), &order)

	if err != nil {
		return model.Order{}, fmt.Errorf("failed to decode order json: %w", err)
	}

	return order, nil
}

func (r *RedisRepo) DeleteByID(ctx context.Context, id uint64) error {
	key := OrderIdKey(id)

	txn := r.Client.TxPipeline()
	err := r.Client.Del(ctx, key).Err()

	if errors.Is(err, redis.Nil) {
		txn.Discard()
		return ErrNotExits
	} else if err != nil {
		txn.Discard()
		return fmt.Errorf("error delete order from redis: %w", err)
	}

	if err = r.Client.SRem(ctx, "orders", key).Err(); err != nil {
		return fmt.Errorf("error deleting orderid from set: %w", err)
	}
	if _, err := txn.Exec(ctx); err != nil {
		return fmt.Errorf("error executing redis transaction in delete order :%w", err)
	}
	return nil
}

func (r *RedisRepo) Update(ctx context.Context, order model.Order) error {
	data, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("error to encode order : %w", err)
	}

	key := OrderIdKey(order.OrderID)

	err = r.Client.SetXX(ctx, key, string(data), 0).Err()
	if errors.Is(err, redis.Nil) {
		return ErrNotExits
	} else if err != nil {
		return fmt.Errorf("error updating order in redis: %w", err)
	}
	return nil
}

type FindAllPage struct {
	Size   uint64
	Offset uint64
}

type FindResults struct {
	Orders []model.Order
	Cursor uint64
}

func (r *RedisRepo) FindAll(ctx context.Context, page FindAllPage) (FindResults, error) {
	res := r.Client.SScan(ctx, "orders", page.Offset, "*", int64(page.Size))

	keys, cursor, err := res.Result()

	if err != nil {
		return FindResults{}, fmt.Errorf("faiied to get order ids: %w", err)
	}

	if len(keys) == 0 {
		return FindResults{
			Orders: []model.Order{},
		}, nil
	}

	xs, err := r.Client.MGet(ctx, keys...).Result()

	if err != nil {
		return FindResults{}, fmt.Errorf("error fetching Orders: %w", err)
	}

	orders := make([]model.Order, len(xs))

	for i, x := range xs {
		x := x.(string)

		var order model.Order

		err := json.Unmarshal([]byte(x), &order)
		if err != nil {
			return FindResults{}, fmt.Errorf("error decoding order: %w", err)
		}
		orders[i] = order
	}

	return FindResults{
		Orders: orders,
		Cursor: cursor,
	}, nil
}
