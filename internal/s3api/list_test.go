// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package s3api

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rkislov/kladovka/internal/storage"
)

func listingRequest(h http.Handler, bucket string, q url.Values) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "/"+bucket+"?"+q.Encode(), nil)
	sign(r, nil, time.Now())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func readListing(t *testing.T, w *httptest.ResponseRecorder) listBucketResult {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("list: %d %s", w.Code, w.Body.String())
	}
	var result listBucketResult
	if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}
func listingNames(r listBucketResult) []string {
	names := []string{}
	for _, o := range r.Contents {
		names = append(names, "object:"+o.Key)
	}
	for _, p := range r.CommonPrefixes {
		names = append(names, "prefix:"+p.Prefix)
	}
	sort.Strings(names)
	return names
}
func TestListingGroupingPaginationAndEncoding(t *testing.T) {
	s, h := testServer(t, 1024)
	if err := s.Store.CreatePolka("demo"); err != nil {
		t.Fatal(err)
	}
	keys := []string{"a.txt", "dir/one", "dir/sub/two", "dir2/file", "literal/a%2Fb", "literal/a/b", "z.txt", "отчёт +%/файл.txt"}
	for _, key := range keys {
		if _, err := s.Store.PutObject("demo", key, strings.NewReader("x"), "text/plain"); err != nil {
			t.Fatal(err)
		}
	}
	for _, version := range []string{"", "2"} {
		t.Run("version"+version, func(t *testing.T) {
			q := url.Values{"delimiter": {"/"}, "max-keys": {"1"}}
			if version != "" {
				q.Set("list-type", version)
			}
			got := []string{}
			for pages := 0; ; pages++ {
				if pages > len(keys) {
					t.Fatal("pagination loop")
				}
				result := readListing(t, listingRequest(h, "demo", q))
				names := listingNames(result)
				if len(names) != 1 {
					t.Fatalf("page: %+v", result)
				}
				got = append(got, names...)
				if version == "2" {
					if result.KeyCount == nil || *result.KeyCount != 1 || result.Marker != nil {
						t.Fatalf("v2 fields: %+v", result)
					}
					if result.ContToken != q.Get("continuation-token") {
						t.Fatal("token echo")
					}
				} else if result.KeyCount != nil || result.Marker == nil || *result.Marker != q.Get("marker") || result.NextContToken != "" {
					t.Fatalf("v1 fields: %+v", result)
				}
				if !result.IsTruncated {
					if result.NextContToken != "" || result.NextMarker != "" {
						t.Fatal("terminal cursor")
					}
					break
				}
				if version == "2" {
					if result.NextContToken == "" {
						t.Fatal("missing cursor")
					}
					q.Set("continuation-token", result.NextContToken)
				} else {
					if result.NextMarker == "" {
						t.Fatal("missing marker")
					}
					q.Set("marker", result.NextMarker)
				}
			}
			sort.Strings(got)
			want := []string{"object:a.txt", "object:z.txt", "prefix:dir/", "prefix:dir2/", "prefix:literal/", "prefix:отчёт +%/"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v want %v", got, want)
			}
		})
	}
	q := url.Values{"list-type": {"2"}, "delimiter": {"/"}, "prefix": {"literal/"}, "encoding-type": {"url"}}
	result := readListing(t, listingRequest(h, "demo", q))
	if result.Prefix != "literal%2F" || result.Delimiter != "%2F" || result.EncodingType != "url" || result.Contents[0].Key != "literal%2Fa%252Fb" || result.CommonPrefixes[0].Prefix != "literal%2Fa%2F" {
		t.Fatalf("encoding: %+v", result)
	}
	q.Set("prefix", "отчёт +%/")
	result = readListing(t, listingRequest(h, "demo", q))
	decoded, err := url.QueryUnescape(result.Contents[0].Key)
	if err != nil || decoded != "отчёт +%/файл.txt" || strings.Contains(result.Contents[0].Key, "+") {
		t.Fatalf("unicode encoding: %+v", result)
	}
	q = url.Values{"list-type": {"2"}, "delimiter": {"/"}, "start-after": {"dir/one"}}
	result = readListing(t, listingRequest(h, "demo", q))
	if result.StartAfter != "dir/one" {
		t.Fatal("start-after echo")
	}
	for _, p := range result.CommonPrefixes {
		if p.Prefix == "dir/" {
			t.Fatal("group before StartAfter returned")
		}
	}
	q.Set("max-keys", "0")
	result = readListing(t, listingRequest(h, "demo", q))
	if *result.KeyCount != 0 || result.IsTruncated || result.NextContToken != "" {
		t.Fatalf("zero limit: %+v", result)
	}
}

func TestListingCursorValidationAndShape(t *testing.T) {
	s, h := testServer(t, 1024)
	for _, bucket := range []string{"demo", "other"} {
		if err := s.Store.CreatePolka(bucket); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"a/one", "b/two", "c"} {
		if _, err := s.Store.PutObject("demo", key, strings.NewReader("x"), "text/plain"); err != nil {
			t.Fatal(err)
		}
	}
	q := url.Values{"list-type": {"2"}, "delimiter": {"/"}, "max-keys": {"1"}}
	first := readListing(t, listingRequest(h, "demo", q))
	token := first.NextContToken
	if token == "" || token == "a/" {
		t.Fatal("expected opaque token")
	}
	q.Set("continuation-token", token)
	q.Set("start-after", "z")
	q.Set("max-keys", "2")
	q.Set("encoding-type", "url")
	next := readListing(t, listingRequest(h, "demo", q))
	if *next.KeyCount != 2 || next.IsTruncated {
		t.Fatalf("token takes precedence and page size may change: %+v", next)
	}
	if listingRequest(h, "other", q).Code != 400 {
		t.Fatal("cross bucket token accepted")
	}
	for _, query := range []string{"list-type=2&continuation-token=raw-key", "list-type=2&continuation-token=", "list-type=2&continuation-token=" + token + "&prefix=a", "list-type=2&continuation-token=" + token, "list-type=3", "list-type=", "max-keys=-1", "max-keys=no", "max-keys=", "max-keys=1&max-keys=2", "encoding-type=base64", "list-type=2&marker=a", "start-after=a", "continuation-token=x", "fetch-owner=false", "list-type=2&fetch-owner=bad"} {
		values, err := url.ParseQuery(query)
		if err != nil {
			t.Fatal(err)
		}
		if w := listingRequest(h, "demo", values); w.Code != 400 {
			t.Fatalf("%s: %d %s", query, w.Code, w.Body.String())
		}
	}
	if w := listingRequest(h, "demo", url.Values{"list-type": {"2"}, "fetch-owner": {"true"}}); w.Code != 501 {
		t.Fatal("unsupported owners must fail")
	}
	result := readListing(t, listingRequest(h, "demo", url.Values{"list-type": {"2"}, "max-keys": {"1000000"}}))
	if result.MaxKeys != 1000 {
		t.Fatal("unbounded max-keys")
	}
	if w := listingRequest(h, "missing", url.Values{"list-type": {"2"}, "max-keys": {"0"}}); w.Code != 404 {
		t.Fatal("missing bucket")
	}
	// Changing the catalogue does not repeat a group or depend on its old last key.
	if err := s.Store.DeleteObject("demo", "a/one"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.PutObject("demo", "a/new", strings.NewReader("x"), "text/plain"); err != nil {
		t.Fatal(err)
	}
	result = readListing(t, listingRequest(h, "demo", url.Values{"list-type": {"2"}, "delimiter": {"/"}, "continuation-token": {token}}))
	if !reflect.DeepEqual(listingNames(result), []string{"object:c", "prefix:b/"}) {
		t.Fatalf("resume after group changed: %+v", result)
	}
}

func TestGroupedListingLargeGroupAndCustomDelimiter(t *testing.T) {
	objects := []storage.ObjectMeta{}
	for i := 0; i < 1005; i++ {
		objects = append(objects, storage.ObjectMeta{Key: fmt.Sprintf("group/%04d", i)})
	}
	objects = append(objects, storage.ObjectMeta{Key: "tail"})
	first, last := groupedListing(objects, "", "/", "", 1)
	if !first.IsTruncated || last != "group/" || len(first.CommonPrefixes) != 1 {
		t.Fatalf("large group: %+v %s", first, last)
	}
	next, _ := groupedListing(objects, "", "/", last, 1)
	if next.IsTruncated || len(next.Contents) != 1 || next.Contents[0].Key != "tail" {
		t.Fatalf("next page: %+v", next)
	}
	custom := []storage.ObjectMeta{{Key: "root::a::one"}, {Key: "root::a::two"}, {Key: "root::b"}}
	result, _ := groupedListing(custom, "root::", "::", "", 2)
	if result.IsTruncated || len(result.CommonPrefixes) != 1 || result.CommonPrefixes[0].Prefix != "root::a::" || len(result.Contents) != 1 {
		t.Fatalf("custom delimiter: %+v", result)
	}
}
