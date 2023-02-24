// Copyright 2020 Michael J. Fromberger. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpcstore

import (
	"context"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/jrpc2"
)

// Store implements the blob.Store interface by calling a JSON-RPC service.
type Store struct {
	cli    *jrpc2.Client
	prefix string
}

// NewStore constructs a Store that delegates through the given client.
func NewStore(cli *jrpc2.Client, opts *StoreOpts) Store {
	s := Store{cli: cli}
	opts.set(&s)
	return s
}

// StoreOpts provide optional settings for a Store client
type StoreOpts struct {
	// Insert this prefix on all method names sent to the service.
	Prefix string
}

func (o *StoreOpts) set(s *Store) {
	if o == nil {
		return
	}
	s.prefix = o.Prefix
}

func (s Store) method(name string) string { return s.prefix + name }

// Get implements a method of blob.Store.
func (s Store) Get(ctx context.Context, key string) ([]byte, error) {
	var data []byte
	err := s.cli.CallResult(ctx, s.method(mGet), KeyRequest{Key: []byte(key)}, &data)
	return data, unfilterErr(err)
}

// Put implements a method of blob.Store.
func (s Store) Put(ctx context.Context, opts blob.PutOptions) error {
	_, err := s.cli.Call(ctx, s.method(mPut), &PutRequest{
		Key:     []byte(opts.Key),
		Data:    opts.Data,
		Replace: opts.Replace,
	})
	return unfilterErr(err)
}

// Delete implements a method of blob.Store.
func (s Store) Delete(ctx context.Context, key string) error {
	_, err := s.cli.Call(ctx, s.method(mDelete), KeyRequest{Key: []byte(key)})
	return unfilterErr(err)
}

// List implements a method of blob.Store.
func (s Store) List(ctx context.Context, start string, f func(string) error) error {
	next := start
	for {
		// Fetch another batch of keys.
		var rsp ListReply
		err := s.cli.CallResult(ctx, s.method(mList), ListRequest{Start: []byte(next)}, &rsp)
		if err != nil {
			return err
		} else if len(rsp.Keys) == 0 {
			break
		}

		// Deliver keys to the callback.
		for _, key := range rsp.Keys {
			if err := f(string(key)); err == blob.ErrStopListing {
				return nil
			} else if err != nil {
				return err
			}
		}
		if len(rsp.Next) == 0 {
			break
		}
		next = string(rsp.Next)
	}
	return nil
}

// Len implements a method of blob.Store.
func (s Store) Len(ctx context.Context) (int64, error) {
	var count int64
	err := s.cli.CallResult(ctx, s.method(mLen), nil, &count)
	return count, err
}

// Close implements a method of blob.Store.
func (s Store) Close(_ context.Context) error { return s.cli.Close() }

// ServerInfo returns the JSON-RPC server status message.
func (s Store) ServerInfo(ctx context.Context) (*jrpc2.ServerInfo, error) {
	return jrpc2.RPCServerInfo(ctx, s.cli)
}

// CAS implements the blob.CAS interface by calling a JSON-RPC service.
type CAS struct {
	Store
}

// NewCAS constructs a CAS that delegates through the given client.
func NewCAS(cli *jrpc2.Client, opts *StoreOpts) CAS {
	return CAS{Store: NewStore(cli, opts)}
}

// CASPut implements part of the blob.CAS type.
func (c CAS) CASPut(ctx context.Context, data []byte) (string, error) {
	var key []byte
	err := c.cli.CallResult(ctx, c.method(mCASPut), &DataRequest{Data: data}, &key)
	return string(key), err
}

// CASKey implements part of the blob.CAS type.
func (c CAS) CASKey(ctx context.Context, data []byte) (string, error) {
	var key []byte
	err := c.cli.CallResult(ctx, c.method(mCASKey), &DataRequest{Data: data}, &key)
	return string(key), err
}
