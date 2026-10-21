// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0.
package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ObjectPage counts each common prefix as one entry, just like an object.
type ObjectPage struct {
	Objects   []ObjectMeta
	Prefixes  []string
	Truncated bool
	Next      string
}

type catalogue struct {
	keys    []string
	objects map[string]ObjectMeta
}

// prefixEnd is a lexical upper bound, not a key or a UTF-8 string for output.
func prefixEnd(prefix string) (string, bool) {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 255 {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}

func pageKeys(keys []string, lookup func(int) ObjectMeta, prefix, delimiter, after string, limit int) ObjectPage {
	result := ObjectPage{}
	if limit <= 0 {
		return result
	}
	pos := sort.SearchStrings(keys, prefix)
	if after != "" {
		pos = max(pos, sort.Search(len(keys), func(i int) bool { return keys[i] > after }))
	}
	count := 0
	for pos < len(keys) && strings.HasPrefix(keys[pos], prefix) {
		key := keys[pos]
		group := ""
		if delimiter != "" {
			if at := strings.Index(key[len(prefix):], delimiter); at >= 0 {
				group = key[:len(prefix)+at+len(delimiter)]
			}
		}
		next := pos + 1
		name := key
		if group != "" {
			name = group
			if end, ok := prefixEnd(group); ok {
				next = sort.SearchStrings(keys, end)
			} else {
				next = len(keys)
			}
		}
		if name > after {
			if count == limit {
				result.Truncated = true
				break
			}
			if group != "" {
				result.Prefixes = append(result.Prefixes, group)
			} else {
				result.Objects = append(result.Objects, lookup(pos))
			}
			result.Next = name
			count++
		}
		pos = next
	}
	return result
}

// PaginateObjects is the common grouping contract for an already sorted snapshot.
// Consensus supplies its authoritative catalogue rather than its materialized cache.
func PaginateObjects(objects []ObjectMeta, prefix, delimiter, after string, limit int) ObjectPage {
	keys := make([]string, len(objects))
	for i := range objects {
		keys[i] = objects[i].Key
	}
	return pageKeys(keys, func(i int) ObjectMeta { return objects[i] }, prefix, delimiter, after, limit)
}

func (c *catalogue) page(prefix, delimiter, after string, limit int) ObjectPage {
	return pageKeys(c.keys, func(i int) ObjectMeta { return c.objects[c.keys[i]] }, prefix, delimiter, after, limit)
}
func (c *catalogue) put(meta ObjectMeta) {
	if _, exists := c.objects[meta.Key]; !exists {
		at := sort.SearchStrings(c.keys, meta.Key)
		c.keys = append(c.keys, "")
		copy(c.keys[at+1:], c.keys[at:])
		c.keys[at] = meta.Key
	}
	c.objects[meta.Key] = meta
}
func (c *catalogue) remove(key string) {
	if _, exists := c.objects[key]; !exists {
		return
	}
	at := sort.SearchStrings(c.keys, key)
	copy(c.keys[at:], c.keys[at+1:])
	c.keys[len(c.keys)-1] = ""
	c.keys = c.keys[:len(c.keys)-1]
	delete(c.objects, key)
}

// prepareCatalogueMutation retains only an index that already contains the
// exact physical key spelling. Unknown keys may alias existing case/Unicode
// names, so they still invalidate every index on folding filesystems. Other
// bucket spellings can be aliases too: discard them even on the fast path.
// Called under mu, before any durable mutation; failed commits discard the
// retained index as well. No external DATA_ROOT edits are supported.
func (s *Store) prepareCatalogueMutation(polka, key string) {
	if s.catalogueExactNames {
		return
	}
	pp, err := s.polkaPath(polka)
	if err != nil {
		clear(s.catalogues)
		return
	}
	path, err := s.objectPath(polka, key)
	if err != nil {
		clear(s.catalogues)
		return
	}
	rel, err := filepath.Rel(pp, path)
	c := s.catalogues[pp]
	if err != nil || c == nil {
		clear(s.catalogues)
		return
	}
	if _, known := c.objects[filepath.ToSlash(rel)]; !known {
		clear(s.catalogues)
		return
	}
	for other := range s.catalogues {
		if other != pp {
			delete(s.catalogues, other)
		}
	}
}

// Called with mu held after durable commit. A failed metadata refresh drops the
// disposable index; the next LIST rebuilds it. It cannot undo an acknowledged write.
func (s *Store) refreshCatalogue(polka, path string, meta *ObjectMeta) {
	pp, err := s.polkaPath(polka)
	if err != nil {
		return
	}
	c := s.catalogues[pp]
	if c == nil {
		return
	}
	key, err := filepath.Rel(pp, path)
	if err != nil {
		delete(s.catalogues, pp)
		return
	}
	key = filepath.ToSlash(key)
	if meta == nil {
		c.remove(key)
		return
	}
	st, err := os.Stat(path)
	if err != nil {
		delete(s.catalogues, pp)
		return
	}
	value := *meta
	value.Key, value.Size, value.LastModified = key, st.Size(), st.ModTime().UTC()
	c.put(value)
}

func (s *Store) buildCatalogue(pp string) (*catalogue, error) {
	c := &catalogue{objects: map[string]ObjectMeta{}}
	err := filepath.WalkDir(pp, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.HasSuffix(d.Name(), ".kladovka-meta.json") || strings.HasSuffix(d.Name(), ".tmp") {
			return nil
		}
		rel, err := filepath.Rel(pp, path)
		if err != nil {
			return err
		}
		st, err := d.Info()
		if err != nil {
			return err
		}
		meta, err := s.readSideMeta(path)
		if err != nil {
			meta = &ObjectMeta{ETag: `""`, ContentType: "application/octet-stream"}
		}
		meta.Key, meta.Size, meta.LastModified = filepath.ToSlash(rel), st.Size(), st.ModTime().UTC()
		c.keys = append(c.keys, meta.Key)
		c.objects[meta.Key] = *meta
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(c.keys)
	return c, nil
}

// ListObjectPage uses an in-memory ordered index under the store lock. Cold
// indexes are rebuilt only after journal recovery; no index files enter backups.
func (s *Store) ListObjectPage(polka, prefix, delimiter, after string, limit int) (ObjectPage, error) {
	pp, err := s.polkaPath(polka)
	if err != nil {
		return ObjectPage{}, err
	}
	s.mu.RLock()
	c := s.catalogues[pp]
	if len(s.pending) > 0 {
		s.mu.RUnlock()
		return ObjectPage{}, ErrPendingWrite
	}
	if c != nil {
		exists, err := s.PolkaExists(polka)
		if err != nil || !exists {
			s.mu.RUnlock()
			if err != nil {
				return ObjectPage{}, err
			}
			return ObjectPage{}, ErrPolkaNotFound
		}
		result := c.page(prefix, delimiter, after, limit)
		s.mu.RUnlock()
		return result, nil
	}
	s.mu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pending) > 0 {
		return ObjectPage{}, ErrPendingWrite
	}
	exists, err := s.PolkaExists(polka)
	if err != nil {
		return ObjectPage{}, err
	}
	if !exists {
		return ObjectPage{}, ErrPolkaNotFound
	}
	c = s.catalogues[pp]
	if c == nil {
		c, err = s.buildCatalogue(pp)
		if err != nil {
			return ObjectPage{}, err
		}
		s.catalogues[pp] = c
	}
	return c.page(prefix, delimiter, after, limit), nil
}

// Filesystems with case folding or Unicode normalization can alias different S3
// names. Unknown lexical mutations are unsafe there; exact cached keys can
// be refreshed after commit, with all other alias indexes invalidated.
// Probe in the journal directory, where crash leftovers are cleaned at startup.
func (s *Store) exactCatalogueNames() (bool, error) {
	dir, err := os.MkdirTemp(s.transactionRoot(), "index-probe-")
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(dir)
	if err = os.WriteFile(filepath.Join(dir, "Aa-é"), nil, 0600); err != nil {
		return false, err
	}
	for _, alias := range []string{"aa-é", "Aa-e\u0301"} {
		if _, err = os.Stat(filepath.Join(dir, alias)); err == nil {
			return false, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return true, nil
}
