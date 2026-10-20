// Copyright 2026 Роман Сергеевич Кислов
// Licensed under the Apache License, Version 2.0 (the "License").

package auth

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
)

// Principal grants object access to exact shelf names. Only admins manage shelves.
type Principal struct {
	ID    string   `json:"id"`
	Role  string   `json:"role"`
	Polki []string `json:"polki"`
	Keys  []Key    `json:"keys"`
}

type Key struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

// Policy is an immutable per-request snapshot; removing a key revokes new requests.
type Policy struct {
	Version int         `json:"version"`
	Users   []Principal `json:"users"`
}

func LoadPolicy(path string) (*Policy, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot open access policy")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("access policy must be a regular file of at most 1 MiB with mode 0600")
	}
	var p Policy
	d := json.NewDecoder(io.LimitReader(f, (1<<20)+1))
	d.DisallowUnknownFields()
	// Never include decoder errors: they may quote a secret from the input.
	if d.Decode(&p) != nil || d.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid access policy JSON")
	}
	if p.Version != 1 {
		return nil, errors.New("unsupported access policy version")
	}
	users, keys := map[string]bool{}, map[string]bool{}
	for _, u := range p.Users {
		if strings.TrimSpace(u.ID) == "" || users[u.ID] || (u.Role != "admin" && u.Role != "reader" && u.Role != "writer") {
			return nil, errors.New("invalid or duplicate user or role in access policy")
		}
		users[u.ID] = true
		seen := map[string]bool{}
		for _, shelf := range u.Polki {
			if shelf == "" || shelf != strings.TrimSpace(shelf) || strings.HasPrefix(shelf, ".") || strings.Contains(shelf, "..") || strings.ContainsAny(shelf, `/\`) || seen[shelf] {
				return nil, errors.New("invalid or duplicate shelf in access policy")
			}
			seen[shelf] = true
		}
		for _, k := range u.Keys {
			if k.AccessKey == "" || strings.ContainsAny(k.AccessKey, " /\t\r\n,=") || len(k.SecretKey) < 16 || keys[k.AccessKey] {
				return nil, errors.New("invalid or duplicate key in access policy; secrets need at least 16 bytes")
			}
			keys[k.AccessKey] = true
		}
	}
	return &p, nil
}

func (p *Policy) Credentials() Credentials {
	creds := Credentials{}
	for _, u := range p.Users {
		for _, k := range u.Keys {
			creds[k.AccessKey] = k.SecretKey
		}
	}
	return creds
}

func (p *Policy) Principal(key Identity) (Principal, error) {
	for _, u := range p.Users {
		for _, k := range u.Keys {
			if k.AccessKey == string(key) {
				u.Keys = nil // Do not retain credentials in request contexts.
				return u, nil
			}
		}
	}
	return Principal{}, errors.New("unknown principal")
}

func (p Principal) CanRead(shelf string) bool {
	if p.Role == "admin" {
		return true
	}
	for _, s := range p.Polki {
		if s == shelf {
			return true
		}
	}
	return false
}

func (p Principal) CanWrite(shelf string) bool {
	return p.Role == "admin" || (p.Role == "writer" && p.CanRead(shelf))
}
