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
//
// All service instances export the following methods:
//
//	Method    JSON Request and response messsages
//	"get"     {"key":"<storage-key"}
//	          "<blob-content>"
//
//	"put"     {"key":"<storage-key>","data":"<blob-content>","replace":bool}
//	          null
//
//	"size"    {"key":"<storage-key>"}
//	          <integer>
//
//	"len":    null
//	          <integer>
//
//	"list":   {"start":"<storage-key>","count":<integer>}
//	          {"keys":["<storage-key>",...],"next":"<storage-key>"}
//
// If the store delegated to the service implements blob.CAS, the service will
// also export these additional methods:
//
//	"cas.put" {"data":"<blob-data>"}
//	          "<storage-key>"
//
//	"cas.key" {"data":"<blob-data>"}
//	          "<storage-key>"
//
// The data formats are:
//
//	<storage-key>   : base-64 encoded string (key), example: "a2V5Zm9v"
//	<blob-content>  : base-64 encoded string (data), example: "ZGF0YWZvbw=="
//	<integer>       : JSON number with integer value, example: 532
type Service struct {
	st  blob.Store
	cas blob.CAS // populated iff st implements blob.CAS
	svc handler.Map
}

func mustWrap(fn interface{}) handler.Func {
	fi, err := handler.Check(fn)
	if err != nil {
		panic(err)
	}
	fi.SetStrict(true)
	return fi.Wrap()
}

// NewService constructs a Service that delegates to the given blob.Store.
func NewService(st blob.Store, opts *ServiceOpts) *Service {
	s := &Service{st: st}
	s.svc = handler.Map{
		mGet:    mustWrap(s.Get),
		mPut:    mustWrap(s.Put),
		mDelete: mustWrap(s.Delete),
		mSize:   mustWrap(s.Size),
		mList:   mustWrap(s.List),
		mLen:    mustWrap(s.Len),
	}
	if cas, ok := st.(blob.CAS); ok {
		s.cas = cas
		s.svc[mCASPut] = mustWrap(s.CASPut)
		s.svc[mCASKey] = mustWrap(s.CASKey)
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
