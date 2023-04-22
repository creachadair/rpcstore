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

// Interface satisfaction checks.
var _ blob.Store = rpcstore.Store{}
var _ blob.CAS = rpcstore.CAS{}

func testService(t *testing.T, s blob.Store) *jrpc2.Client {
	svc := rpcstore.NewService(s, nil)
	loc := server.NewLocal(svc.Methods(), nil)
	t.Cleanup(func() {
		if err := loc.Close(); err != nil {
			t.Errorf("Server close: %v", err)
		}
	})
	return loc.Client
}

func TestStore(t *testing.T) {
	t.Run("Status", func(t *testing.T) {
		cli := testService(t, memstore.New())
		var si jrpc2.ServerInfo
		if err := cli.CallResult(context.Background(), "rpc.serverInfo", nil, &si); err != nil {
			t.Fatalf("Server info: %v", err)
		}
		t.Logf("Server methods: %+q", si.Methods)
	})

	t.Run("Store", func(t *testing.T) {
		cli := testService(t, memstore.New())
		rs := rpcstore.NewStore(cli, nil)
		storetest.Run(t, rs)
	})

	t.Run("CAS", func(t *testing.T) {
		cli := testService(t, memstore.New())
		rs := rpcstore.NewCAS(cli, nil)
		storetest.Run(t, rs)
	})
}

func TestCAS(t *testing.T) {
	mem := blob.NewCAS(memstore.New(), sha1.New)
	svc := rpcstore.NewService(mem, nil)

	loc := server.NewLocal(svc.Methods(), nil)
	defer loc.Close()

	// echo "abcde" | shasum -a 1
	const input = "abcde\n"
	const want = "ec11312386ad561674f724b8cca7cf1796e26d1d"

	rs := rpcstore.NewCAS(loc.Client, nil)
	t.Run("CASPut", func(t *testing.T) {
		key, err := rs.CASPut(context.Background(), blob.CASPutOptions{Data: []byte(input)})
		if err != nil {
			t.Errorf("PutCAS(%q) failed: %v", input, err)
		} else if got := fmt.Sprintf("%x", key); got != want {
			t.Errorf("PutCAS(%q): got key %q, want %q", input, got, want)
		}
	})
	t.Run("CASKey", func(t *testing.T) {
		key, err := rs.CASKey(context.Background(), blob.CASPutOptions{Data: []byte(input)})
		if err != nil {
			t.Errorf("CASKey(%q) failed: %v", input, err)
		} else if got := fmt.Sprintf("%x", key); got != want {
			t.Errorf("CASKey(%q): got key %q, want %q", input, got, want)
		}
	})
	t.Run("Len", func(t *testing.T) {
		n, err := rs.Len(context.Background())
		if err != nil {
			t.Errorf("Len failed: %v", err)
		} else if n != 1 {
			t.Errorf("Len: got %d, want %d", n, 1)
		}
	})
}

func TestPrefix(t *testing.T) {
	svc := rpcstore.NewService(memstore.New(), nil)
	loc := server.NewLocal(handler.ServiceMap{
		"blob": svc.Methods(),
	}, nil)
	defer loc.Close()

	rs := rpcstore.NewStore(loc.Client, &rpcstore.StoreOpts{
		Prefix: "blob.",
	})
	storetest.Run(t, rs)
}
