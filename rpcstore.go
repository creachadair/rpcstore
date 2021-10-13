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

// Package rpcstore implements the blob.Store that delegates to an underlying
// store via a JSON-RPC interface.
package rpcstore

import (
	"context"
	"errors"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
)

// Since the client and the service must agree on method names, define these as
// constants that can be shared between the two.
const (
	mGet    = "get"
	mPut    = "put"
	mCASPut = "cas.put"
	mCASKey = "cas.key"
	mDelete = "delete"
	mSize   = "size"
	mList   = "list"
	mLen    = "len"
)

var errNoCAS = errors.New("store does not implement content addressing")

// Service implements a service that adapts RPC requests to a blob.Store.
type Service struct {
	st  blob.Store
	cas blob.CAS // populated iff st implements blob.CAS
	svc handler.Map
}

// NewService constructs a Service that delegates to the given blob.Store.
func NewService(st blob.Store, opts *ServiceOpts) *Service {
	s := &Service{st: st}
	s.svc = handler.Map{
		mGet:    handler.New(s.Get),
		mPut:    handler.New(s.Put),
		mDelete: handler.New(s.Delete),
		mSize:   handler.New(s.Size),
		mList:   handler.New(s.List),
		mLen:    handler.New(s.Len),
	}
	if cas, ok := st.(blob.CAS); ok {
		s.cas = cas
		s.svc[mCASPut] = handler.New(s.CASPut)
		s.svc[mCASKey] = handler.New(s.CASKey)
	}
	return s
}

// ServiceOpts provides optional settings for constructing a Service.
type ServiceOpts struct{}

// Methods returns a map of the service methods for s.
func (s Service) Methods() jrpc2.Assigner { return s.svc }

// Get handles the corresponding method of blob.Store.
func (s Service) Get(ctx context.Context, req *KeyRequest) ([]byte, error) {
	data, err := s.st.Get(ctx, string(req.Key))
	return data, filterErr(err)
}

// Put handles the corresponding method of blob.Store.
func (s Service) Put(ctx context.Context, req *PutRequest) error {
	return filterErr(s.st.Put(ctx, blob.PutOptions{
		Key:     string(req.Key),
		Data:    req.Data,
		Replace: req.Replace,
	}))
}

// CASPut implements content-addressable storage if the service has a hash
// constructor installed.
func (s Service) CASPut(ctx context.Context, req *DataRequest) ([]byte, error) {
	if s.cas == nil {
		return nil, errNoCAS
	}
	key, err := s.cas.CASPut(ctx, req.Data)
	return []byte(key), err
}

// CASKey computes and returns the hash key for the specified data, if the
// service has a hash constructor installed.
func (s Service) CASKey(ctx context.Context, req *DataRequest) ([]byte, error) {
	if s.cas == nil {
		return nil, errNoCAS
	}
	key, err := s.cas.CASKey(ctx, req.Data)
	return []byte(key), err
}

// Delete handles the corresponding method of blob.Store.
func (s Service) Delete(ctx context.Context, req *KeyRequest) error {
	return filterErr(s.st.Delete(ctx, string(req.Key)))
}

// Size handles the corresponding method of blob.Store.
func (s Service) Size(ctx context.Context, req *KeyRequest) (int64, error) {
	size, err := s.st.Size(ctx, string(req.Key))
	return size, filterErr(err)
}

// List handles the corresponding method of blob.Store.
func (s Service) List(ctx context.Context, req *ListRequest) (*ListReply, error) {
	var rsp ListReply

	limit := req.Count
	if limit <= 0 {
		limit = 64
	}
	if err := s.st.List(ctx, string(req.Start), func(key string) error {
		if len(rsp.Keys) == limit {
			rsp.Next = []byte(key)
			return blob.ErrStopListing
		}
		rsp.Keys = append(rsp.Keys, []byte(key))
		return nil
	}); err != nil {
		return nil, err
	}
	return &rsp, nil
}

// Len handles the corresponding method of blob.Store.
func (s Service) Len(ctx context.Context) (int64, error) { return s.st.Len(ctx) }

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

// Size implements a method of blob.Store.
func (s Store) Size(ctx context.Context, key string) (int64, error) {
	var size int64
	err := s.cli.CallResult(ctx, s.method(mSize), KeyRequest{Key: []byte(key)}, &size)
	return size, unfilterErr(err)
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
