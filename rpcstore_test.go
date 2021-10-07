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
	"strings"
	"testing"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/blob/memstore"
	"github.com/creachadair/ffs/blob/storetest"
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
	"github.com/creachadair/jrpc2/server"
	"github.com/creachadair/rpcstore"
	"github.com/google/go-cmp/cmp"
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

func TestServicePrefix(t *testing.T) {
	svc := rpcstore.NewService(memstore.New(), nil)
	loc := server.NewLocal(handler.ServiceMap{
		"blob": svc.Methods(),
	}, nil)
	defer loc.Close()

	rs := rpcstore.NewClient(loc.Client, &rpcstore.StoreOpts{
		ServicePrefix: "blob.",
	})
	storetest.Run(t, rs)
}

func TestKeyPrefix(t *testing.T) {
	ms := memstore.New()
	svc := rpcstore.NewService(blob.NewCAS(ms, sha1.New), nil)
	loc := server.NewLocal(svc.Methods(), nil)
	defer loc.Close()

	rs := rpcstore.NewClient(loc.Client, &rpcstore.StoreOpts{
		KeyPrefix: "foo/",
	})

	ctx := context.Background()

	// Store a conventional (non-content-addressed) key.
	if err := rs.Put(ctx, blob.PutOptions{
		Key:  "test1",
		Data: []byte("hello"),
	}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Store a content-addressed key.
	if key, err := rs.PutCAS(ctx, []byte("test2")); err != nil {
		t.Fatalf("PutCAS failed: %v", err)
	} else if !strings.HasPrefix(key, "foo/") {
		t.Errorf("PutCAS key: got %q, wanted prefix foo/", key)
	}

	// echo -n "test2" | shasum -a 1
	const test2Hash = "\x10\x9f\x4b\x3c\x50\xd7\xb0\xdf\x72\x9d\x29\x9b\xc6\xf8\xe9\xef\x90\x66\x97\x1f"

	// Verify that both keys were properly prefixed with the target.
	got := ms.Snapshot(make(map[string]string))
	want := map[string]string{
		"foo/test1":        "hello",
		"foo/" + test2Hash: "test2",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Wrong stored keys: (-want, +got)\n%s", diff)
	}
}
