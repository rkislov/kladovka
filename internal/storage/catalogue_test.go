// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package storage

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
)

// A deliberately linear oracle independent of prefix bounds and index seeking.
func naivePage(objects []ObjectMeta, prefix, delimiter, after string, limit int) ObjectPage {
	result := ObjectPage{}
	if limit == 0 {
		return result
	}
	seen := map[string]bool{}
	count := 0
	for _, o := range objects {
		if !strings.HasPrefix(o.Key, prefix) {
			continue
		}
		name, group := o.Key, false
		if delimiter != "" {
			if at := strings.Index(name[len(prefix):], delimiter); at >= 0 {
				name = name[:len(prefix)+at+len(delimiter)]
				group = true
			}
		}
		if seen[name] || name <= after {
			continue
		}
		seen[name] = true
		if count == limit {
			result.Truncated = true
			break
		}
		if group {
			result.Prefixes = append(result.Prefixes, name)
		} else {
			result.Objects = append(result.Objects, o)
		}
		result.Next = name
		count++
	}
	return result
}

func TestCataloguePageMatchesLinearOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(1234))
	objects := []ObjectMeta{}
	for i := 0; i < 1100; i++ {
		objects = append(objects, ObjectMeta{Key: fmt.Sprintf("g%02d/sub%d/key-%04d", i%7, i%3, i)})
	}
	for _, key := range []string{"g00", "g00~file", "literal/a%2Fb", "literal/a/b", "кириллица/файл", "root::a::b", "root::a::c", "root::z", "a\xff/one", "a\xff/two"} {
		objects = append(objects, ObjectMeta{Key: key})
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	prefixes := []string{"", "g0", "g00/", "g03/sub", "literal/", "кириллица", "root::", "a\xff"}
	delimiters := []string{"", "/", "::", "sub", "ф"}
	for i := 0; i < 600; i++ {
		prefix, delimiter := prefixes[rng.Intn(len(prefixes))], delimiters[rng.Intn(len(delimiters))]
		after := ""
		if i%2 == 0 {
			after = objects[rng.Intn(len(objects))].Key
		}
		limit := rng.Intn(12)
		got := PaginateObjects(objects, prefix, delimiter, after, limit)
		want := naivePage(objects, prefix, delimiter, after, limit)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("prefix=%q delimiter=%q after=%q limit=%d\ngot=%+v\nwant=%+v", prefix, delimiter, after, limit, got, want)
		}
	}
}

func TestCatalogueMutationsAndRestart(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise incremental updates even on a folding development volume; keys below are ASCII lowercase.
	s.catalogueExactNames = true
	if err = s.CreatePolka("demo"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		key := fmt.Sprintf("folder/file-%02d", i%7)
		if _, err = s.PutObject("demo", key, strings.NewReader(strings.Repeat("v", i+1)), "text/plain"); err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			err = s.DeleteObject("demo", fmt.Sprintf("folder/file-%02d", (i+1)%7))
			if err != nil && !errors.Is(err, ErrObjectNotFound) {
				t.Fatal(err)
			}
		}
		if i%4 == 0 {
			if err = s.SetETag("demo", key, `"multipart-2"`); err != nil {
				t.Fatal(err)
			}
		}
		// New reconstructs from the recovered filesystem, not from the warm index.
		restored, err := New(root)
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range []struct {
			prefix, delimiter, after string
			limit                    int
		}{{"", "", "", 100}, {"", "/", "", 1}, {"folder/", "", "folder/file-01", 2}} {
			warm, err := s.ListObjectPage("demo", query.prefix, query.delimiter, query.after, query.limit)
			if err != nil {
				t.Fatal(err)
			}
			cold, err := restored.ListObjectPage("demo", query.prefix, query.delimiter, query.after, query.limit)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(warm, cold) {
				t.Fatalf("warm/cold differ at %d: %+v %+v", i, warm, cold)
			}
		}
	}
	// Returned metadata is a copy, not a mutable view into the index.
	objects, _, err := s.ListObjects("demo", "", 100, "")
	if err != nil {
		t.Fatal(err)
	}
	objects[0].ETag = "changed"
	again, _, _ := s.ListObjects("demo", "", 100, "")
	if again[0].ETag == "changed" {
		t.Fatal("caller mutated index")
	}
	for _, o := range again {
		if err = s.DeleteObject("demo", o.Key); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.DeletePolka("demo"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ListObjectPage("demo", "", "", "", 0); !errors.Is(err, ErrPolkaNotFound) {
		t.Fatal(err)
	}
	if err = s.CreatePolka("demo"); err != nil {
		t.Fatal(err)
	}
	result, err := s.ListObjectPage("demo", "", "", "", 100)
	if err != nil || len(result.Objects) != 0 {
		t.Fatal("old catalogue survived bucket recreation", result, err)
	}
}

func TestCatalogueUnfinishedCommitAndLiveRecovery(t *testing.T) {
	for _, mode := range []string{"put", "delete"} {
		for _, point := range []string{"prepared", "data-published", "metadata-published"} {
			t.Run(mode+point, func(t *testing.T) {
				s, err := New(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				s.CreatePolka("demo")
				if _, err = s.PutObject("demo", "key", strings.NewReader("old"), ""); err != nil {
					t.Fatal(err)
				}
				if _, _, err = s.ListObjects("demo", "", 100, ""); err != nil {
					t.Fatal(err)
				}
				s.afterStep = func(step string) error {
					if step == point {
						return errors.New("injected commit failure")
					}
					return nil
				}
				if mode == "put" {
					_, err = s.PutObject("demo", "key", strings.NewReader("new content"), "")
				} else {
					err = s.DeleteObject("demo", "key")
				}
				if err == nil {
					t.Fatal("expected commit failure")
				}
				if _, err = s.ListObjectPage("demo", "", "", "", 1); !errors.Is(err, ErrPendingWrite) {
					t.Fatal("unfinished commit exposed by LIST", err)
				}
				s.afterStep = nil
				// The next writer recovers the journal within this process.
				if _, err = s.PutObject("demo", "after", strings.NewReader("after"), ""); err != nil {
					t.Fatal(err)
				}
				page, err := s.ListObjectPage("demo", "", "", "", 100)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "delete" {
					if len(page.Objects) != 1 || page.Objects[0].Key != "after" {
						t.Fatal(page)
					}
				} else {
					if len(page.Objects) != 2 || page.Objects[1].Key != "key" || page.Objects[1].Size != 11 {
						t.Fatal(page)
					}
				}
			})
		}
	}
}

func TestCatalogueConcurrentReadersAndWriter(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.catalogueExactNames = true
	s.CreatePolka("demo")
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 50; n++ {
				page, e := s.ListObjectPage("demo", "", "", "", 100)
				if e != nil {
					t.Error(e)
					return
				}
				for j := 1; j < len(page.Objects); j++ {
					if page.Objects[j-1].Key >= page.Objects[j].Key {
						t.Error("unordered page")
						return
					}
				}
			}
		}()
	}
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("key-%d", i)
		if _, err = s.PutObject("demo", key, strings.NewReader("v"), ""); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	page, err := s.ListObjectPage("demo", "", "", "", 100)
	if err != nil || len(page.Objects) != 8 {
		t.Fatal(page, err)
	}
}

func BenchmarkCataloguePage(b *testing.B) {
	for _, count := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			c := &catalogue{objects: map[string]ObjectMeta{}}
			for i := 0; i < count; i++ {
				c.put(ObjectMeta{Key: fmt.Sprintf("group-%06d/key-%06d", i/100, i)})
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c.page("", "/", "", 100)
			}
		})
	}
}

func TestCatalogueFilesystemAliases(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if s.catalogueExactNames {
		t.Skip("filesystem preserves distinct case/normalization")
	}
	s.CreatePolka("demo")
	for _, key := range []string{"Folder/Aa-é", "folder/aa-é", "FOLDER/Aa-e\u0301", "folder/other"} {
		if _, err = s.PutObject("demo", key, strings.NewReader(key), ""); err != nil {
			t.Fatal(err)
		}
		got, _, err := s.ListObjects("demo", "", 100, "")
		if err != nil {
			t.Fatal(err)
		}
		restored, err := New(s.root)
		if err != nil {
			t.Fatal(err)
		}
		want, _, err := restored.ListObjects("demo", "", 100, "")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("alias created phantom: %v %v", got, want)
		}
	}
	if err = s.DeleteObject("demo", "folder/aa-é"); err != nil {
		t.Fatal(err)
	}
	page, err := s.ListObjectPage("demo", "", "", "", 100)
	if err != nil || len(page.Objects) != 1 || !strings.HasSuffix(page.Objects[0].Key, "other") {
		t.Fatal(page, err)
	}
}

// Exercise folding-FS invalidation even on Linux CI; native aliases are tested
// separately on APFS. Only a proven existing spelling may retain its index.
func TestCatalogueExactOverwriteOnFoldingFilesystem(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s.catalogueExactNames = false
	for _, bucket := range []string{"demo", "other"} {
		if err = s.CreatePolka(bucket); err != nil {
			t.Fatal(err)
		}
		if _, err = s.PutObject(bucket, "Dir/key", strings.NewReader("old"), ""); err != nil {
			t.Fatal(err)
		}
	}
	for _, bucket := range []string{"demo", "other"} {
		if _, err = s.ListObjectPage(bucket, "", "", "", 100); err != nil {
			t.Fatal(err)
		}
	}
	pp, _ := s.polkaPath("demo")
	op, _ := s.polkaPath("other")
	index := s.catalogues[pp]
	meta, err := s.PutObject("demo", "Dir/key", strings.NewReader("new body"), "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	if s.catalogues[pp] != index || s.catalogues[op] != nil {
		t.Fatal("exact cache not retained or aliases not invalidated")
	}
	page, err := s.ListObjectPage("demo", "", "", "", 100)
	if err != nil || len(page.Objects) != 1 || page.Objects[0].ETag != meta.ETag || page.Objects[0].Size != 8 {
		t.Fatal(page, err)
	}
	if err = s.SetETag("demo", "Dir/key", "multipart-etag"); err != nil {
		t.Fatal(err)
	}
	if s.catalogues[pp] != index || index.objects["Dir/key"].ETag != "multipart-etag" {
		t.Fatal("ETag refresh lost")
	}
	if err = s.DeleteObject("demo", "Dir/key"); err != nil {
		t.Fatal(err)
	}
	if s.catalogues[pp] != index || len(index.objects) != 0 {
		t.Fatal("exact delete not reflected")
	}
	if _, err = s.PutObject("demo", "Dir/new", strings.NewReader("new"), ""); err != nil {
		t.Fatal(err)
	}
	if len(s.catalogues) != 0 {
		t.Fatal("unproven spelling retained an index")
	}
}

func TestCatalogueWarmBucketAndDirectoryAliases(t *testing.T) {
	s, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if s.catalogueExactNames {
		t.Skip("requires native filesystem aliases")
	}
	if err = s.CreatePolka("demo"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"Folder/Aa-é", "Folder/second"} {
		if _, err = s.PutObject("demo", key, strings.NewReader("old"), ""); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"Folder/Aa-é", "folder/AA-e\u0301", "FOLDER/second"} {
		// Two independently warmed spellings of the very same directory.
		for _, bucket := range []string{"demo", "DEMO"} {
			if _, err = s.ListObjectPage(bucket, "", "", "", 100); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = s.PutObject("demo", key, strings.NewReader(key), "text/plain"); err != nil {
			t.Fatal(err)
		}
		restored, err := New(s.root)
		if err != nil {
			t.Fatal(err)
		}
		for _, bucket := range []string{"demo", "DEMO"} {
			got, e := s.ListObjectPage(bucket, "", "", "", 100)
			if e != nil {
				t.Fatal(e)
			}
			want, e := restored.ListObjectPage(bucket, "", "", "", 100)
			if e != nil || !reflect.DeepEqual(got, want) {
				t.Fatal("stale alias", got, want, e)
			}
		}
	}
}
