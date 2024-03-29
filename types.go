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
	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/jrpc2"
)

// KeyRequest is the request to the Get, Delete, and Size methods.
type KeyRequest struct {
	Key []byte `json:"key"`
}

// PutRequest is the request to the Put method.
type PutRequest struct {
	Key     []byte `json:"key"`
	Data    []byte `json:"data"`
	Replace bool   `json:"replace"`
}

// CASPutRequest is the request to the CASPut and CASGet methods.
type CASPutRequest struct {
	Data   []byte `json:"data"`
	Prefix []byte `json:"prefix,omitempty"`
	Suffix []byte `json:"suffix,omitempty"`
}

// ListRequest is the request to the List method.
type ListRequest struct {
	Start []byte `json:"start"`
	Count int    `json:"count"`
}

// ListReply is the reply from the List method.
type ListReply struct {
	Keys [][]byte `json:"keys"`
	Next []byte   `json:"next,omitempty"`
}

var (
	errKeyNotFound = jrpc2.Errorf(-100, "key not found")
	errKeyExists   = jrpc2.Errorf(-101, "key exists")
)

// filterErr assigns stable error codes to important blob.Store errors so they
// will survive transit through JSON-RPC.
func filterErr(err error) error {
	if blob.IsKeyNotFound(err) {
		return errKeyNotFound
	} else if blob.IsKeyExists(err) {
		return errKeyExists
	}
	return err
}

// unfilterErr converts JSON-RPC errors back into blob.Store errors.
func unfilterErr(err error) error {
	switch jrpc2.ErrorCode(err) {
	case -100:
		return blob.ErrKeyNotFound
	case -101:
		return blob.ErrKeyExists
	}
	return err
}
