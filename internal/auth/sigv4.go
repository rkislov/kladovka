// Copyright 2026 Роман Сергеевич Кислов
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Package auth implements a practical subset of AWS Signature Version 4 for S3.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Credentials maps access key -> secret.
type Credentials map[string]string

// Authenticator validates SigV4 Authorization headers (header-based signing).
type Authenticator struct {
	Region string
	Creds  Credentials
}

// Identity is the authenticated principal (access key id).
type Identity string

// AuthenticateRequest verifies AWS4-HMAC-SHA256 Authorization when present.
// If Authorization is missing, returns ("", ErrMissingAuth) so callers can allow anonymous policies later.
func (a *Authenticator) AuthenticateRequest(r *http.Request, payloadHash string) (Identity, error) {
	authz := r.Header.Get("Authorization")
	if authz == "" {
		// Presigned query support (minimal).
		if alg := r.URL.Query().Get("X-Amz-Algorithm"); alg == "AWS4-HMAC-SHA256" {
			return a.verifyPresigned(r)
		}
		return "", ErrMissingAuth
	}
	if !strings.HasPrefix(authz, "AWS4-HMAC-SHA256 ") {
		return "", ErrInvalidAuth
	}
	parts := parseAuthParts(authz[len("AWS4-HMAC-SHA256 "):])
	cred := parts["Credential"]
	signedHeaders := parts["SignedHeaders"]
	signature := parts["Signature"]
	if cred == "" || signedHeaders == "" || signature == "" {
		return "", ErrInvalidAuth
	}
	credParts := strings.Split(cred, "/")
	if len(credParts) != 5 {
		return "", ErrInvalidAuth
	}
	accessKey, dateStamp, region, service := credParts[0], credParts[1], credParts[2], credParts[3]
	secret, ok := a.Creds[accessKey]
	if !ok {
		return "", ErrUnknownKey
	}
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		amzDate = r.Header.Get("Date")
	}
	if amzDate == "" {
		return "", ErrInvalidAuth
	}
	if payloadHash == "" {
		payloadHash = r.Header.Get("X-Amz-Content-Sha256")
	}
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	headers := map[string]string{}
	for _, h := range strings.Split(signedHeaders, ";") {
		h = strings.ToLower(strings.TrimSpace(h))
		if h == "" {
			continue
		}
		if h == "host" {
			headers[h] = r.Host
			continue
		}
		headers[h] = strings.TrimSpace(r.Header.Get(h))
	}
	canonical := buildCanonicalRequest(r.Method, r.URL.Path, r.URL.RawQuery, headers, signedHeaders, payloadHash)
	scope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	sts := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonical)),
	}, "\n")
	signingKey := deriveSigningKey(secret, dateStamp, region, service)
	expected := hex.EncodeToString(hmacSHA256(signingKey, []byte(sts)))
	if !hmac.Equal([]byte(strings.ToLower(expected)), []byte(strings.ToLower(signature))) {
		return "", ErrBadSignature
	}
	_ = a.Region
	return Identity(accessKey), nil
}

func (a *Authenticator) verifyPresigned(r *http.Request) (Identity, error) {
	q := r.URL.Query()
	cred := q.Get("X-Amz-Credential")
	amzDate := q.Get("X-Amz-Date")
	signedHeaders := q.Get("X-Amz-SignedHeaders")
	signature := q.Get("X-Amz-Signature")
	expires := q.Get("X-Amz-Expires")
	credParts := strings.Split(cred, "/")
	if len(credParts) != 5 {
		return "", ErrInvalidAuth
	}
	accessKey, dateStamp, region, service := credParts[0], credParts[1], credParts[2], credParts[3]
	secret, ok := a.Creds[accessKey]
	if !ok {
		return "", ErrUnknownKey
	}
	if expires != "" {
		t, err := time.Parse("20060102T150405Z", amzDate)
		if err == nil {
			var sec int
			fmt.Sscanf(expires, "%d", &sec)
			if sec > 0 && time.Now().UTC().After(t.Add(time.Duration(sec)*time.Second)) {
				return "", ErrExpired
			}
		}
	}
	// Rebuild query without signature for canonical request.
	vals := url.Values{}
	for k, vv := range q {
		if strings.EqualFold(k, "X-Amz-Signature") {
			continue
		}
		for _, v := range vv {
			vals.Add(k, v)
		}
	}
	canonicalQuery := encodeQuery(vals)
	headers := map[string]string{}
	for _, h := range strings.Split(signedHeaders, ";") {
		h = strings.ToLower(strings.TrimSpace(h))
		if h == "host" {
			headers[h] = r.Host
		} else {
			headers[h] = strings.TrimSpace(r.Header.Get(h))
		}
	}
	canonical := buildCanonicalRequest(r.Method, r.URL.Path, canonicalQuery, headers, signedHeaders, "UNSIGNED-PAYLOAD")
	scope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	sts := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, scope, sha256Hex([]byte(canonical))}, "\n")
	signingKey := deriveSigningKey(secret, dateStamp, region, service)
	expected := hex.EncodeToString(hmacSHA256(signingKey, []byte(sts)))
	if !hmac.Equal([]byte(strings.ToLower(expected)), []byte(strings.ToLower(signature))) {
		return "", ErrBadSignature
	}
	return Identity(accessKey), nil
}

var (
	ErrMissingAuth  = fmt.Errorf("missing AWS Signature V4")
	ErrInvalidAuth  = fmt.Errorf("invalid AWS Signature V4")
	ErrUnknownKey   = fmt.Errorf("unknown access key")
	ErrBadSignature = fmt.Errorf("signature mismatch")
	ErrExpired      = fmt.Errorf("request has expired")
)

func parseAuthParts(rest string) map[string]string {
	out := map[string]string{}
	rest = strings.ReplaceAll(rest, "\n", " ")
	for _, item := range strings.Split(rest, ",") {
		item = strings.TrimSpace(item)
		if i := strings.IndexByte(item, '='); i > 0 {
			out[item[:i]] = strings.TrimSpace(item[i+1:])
		}
	}
	return out
}

func buildCanonicalRequest(method, path, rawQuery string, headers map[string]string, signedHeaders, payloadHash string) string {
	if path == "" {
		path = "/"
	}
	// Encode path segments.
	segs := strings.Split(path, "/")
	for i, s := range segs {
		if s == "" {
			continue
		}
		segs[i] = uriEncode(s, false)
	}
	canPath := strings.Join(segs, "/")
	if !strings.HasPrefix(canPath, "/") {
		canPath = "/" + canPath
	}

	var q string
	if rawQuery == "" {
		q = ""
	} else if strings.Contains(rawQuery, "=") || strings.Contains(rawQuery, "&") {
		// Already encoded or raw — normalize via url.ParseQuery when possible.
		vals, err := url.ParseQuery(rawQuery)
		if err == nil {
			q = encodeQuery(vals)
		} else {
			q = rawQuery
		}
	} else {
		q = rawQuery
	}

	names := strings.Split(signedHeaders, ";")
	sort.Strings(names)
	var hdrLines strings.Builder
	for _, n := range names {
		n = strings.ToLower(strings.TrimSpace(n))
		hdrLines.WriteString(n)
		hdrLines.WriteByte(':')
		hdrLines.WriteString(strings.TrimSpace(headers[n]))
		hdrLines.WriteByte('\n')
	}
	return strings.Join([]string{
		method,
		canPath,
		q,
		hdrLines.String(),
		strings.Join(names, ";"),
		payloadHash,
	}, "\n")
}

func encodeQuery(v url.Values) string {
	if len(v) == 0 {
		return ""
	}
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	first := true
	for _, k := range keys {
		vals := v[k]
		sort.Strings(vals)
		for _, val := range vals {
			if !first {
				b.WriteByte('&')
			}
			first = false
			b.WriteString(uriEncode(k, true))
			b.WriteByte('=')
			b.WriteString(uriEncode(val, true))
		}
	}
	return b.String()
}

func uriEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
		} else if c == '/' && !encodeSlash {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func deriveSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
