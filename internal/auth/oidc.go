package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// oidcVerifier validates JWTs minted by an external OIDC issuer.
// JWKS is fetched lazily and auto-refreshed by keyfunc on kid miss.
type oidcVerifier struct {
	issuer   string
	audience string
	jwks     keyfunc.Keyfunc
}

func newOIDCVerifier(ctx context.Context, issuer, audience string) (*oidcVerifier, error) {
	jwksURI, err := discoverJWKS(ctx, issuer)
	if err != nil {
		return nil, fmt.Errorf("oidc discovery: %w", err)
	}
	k, err := keyfunc.NewDefaultCtx(ctx, []string{jwksURI})
	if err != nil {
		return nil, fmt.Errorf("fetch jwks %s: %w", jwksURI, err)
	}
	return &oidcVerifier{issuer: issuer, audience: audience, jwks: k}, nil
}

func (v *oidcVerifier) verify(token string) (string, error) {
	opts := []jwt.ParserOption{
		jwt.WithIssuer(v.issuer),
		jwt.WithExpirationRequired(),
	}
	if v.audience != "" {
		opts = append(opts, jwt.WithAudience(v.audience))
	}
	var claims jwt.RegisteredClaims
	_, err := jwt.ParseWithClaims(token, &claims, v.jwks.Keyfunc, opts...)
	if err != nil {
		return "", err
	}
	if claims.Subject == "" {
		return "", errors.New("missing subject claim")
	}
	return claims.Subject, nil
}

// tokenIssuer extracts the `iss` claim without verifying the signature, so
// the validator can route tokens to the right verifier.
func tokenIssuer(token string) (string, error) {
	parser := jwt.NewParser()
	var claims jwt.RegisteredClaims
	_, _, err := parser.ParseUnverified(token, &claims)
	if err != nil {
		return "", err
	}
	return claims.Issuer, nil
}

func discoverJWKS(ctx context.Context, issuer string) (string, error) {
	url := strings.TrimRight(issuer, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	var doc struct {
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return "", err
	}
	if doc.JWKSURI == "" {
		return "", errors.New("discovery doc missing jwks_uri")
	}
	return doc.JWKSURI, nil
}
