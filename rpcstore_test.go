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

package rpcstore_test

import (
	"context"
	"crypto/sha1"
	"fmt"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/server"
	"github.com/creachadair/rpcstore"
)

// Interface satisfaction check.
var _ blob.CAS = rpcstore.Store{}

func TestStore(t *testing.T) {
	mem := memstore.New()
	svc := rpcstore.NewService(mem, nil)

	loc := server.NewLocal(svc.Methods(), nil)

	si, err := jrpc2.RPCServerInfo(context.Background(), loc.Client)
	if err != nil {
		t.Fatalf("Server info: %v", err)
	}
	t.Logf("Server methods: %+q", si.Methods)

	rs := rpcstore.NewClient(loc.Client, nil)
	storetest.Run(t, rs)
	if err := loc.Close(); err != nil {
		t.Fatalf("Server close: %v", err)
	}
}

func TestCAS(t *testing.T) {
	mem := blob.NewCAS(memstore.New(), sha1.New)
	svc := rpcstore.NewService(mem, nil)

	loc := server.NewLocal(svc.Methods(), nil)
	defer loc.Close()

	// echo "abcde" | shasum -a 1
	const input = "abcde\n"
	const want = "ec11312386ad561674f724b8cca7cf1796e26d1d"

	rs := rpcstore.NewClient(loc.Client, nil)
	t.Run("CASPut", func(t *testing.T) {
		key, err := rs.PutCAS(context.Background(), []byte(input))
		if err != nil {
			t.Errorf("PutCAS(%q) failed: %v", input, err)
		} else if got := fmt.Sprintf("%x", key); got != want {
			t.Errorf("PutCAS(%q): got key %q, want %q", input, got, want)
		}
	})
	t.Run("CASKey", func(t *testing.T) {
		key, err := rs.CASKey(context.Background(), []byte(input))
		if err != nil {
			t.Errorf("CASKey(%q) failed: %v", input, err)
		} else if got := fmt.Sprintf("%x", key); got != want {
			t.Errorf("CASKey(%q): got key %q, want %q", input, got, want)
		}
	})
}

func TestPrefix(t *testing.T) {
	svc := rpcstore.NewService(memstore.New(), nil)
	loc := server.NewLocal(handler.ServiceMap{
		"blob": svc.Methods(),
	}, nil)
	defer loc.Close()

	rs := rpcstore.NewClient(loc.Client, &rpcstore.StoreOpts{
		Prefix: "blob.",
	})
	storetest.Run(t, rs)
}
