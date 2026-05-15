package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const localJWTIssuer = "duckflight"

// localJWT mints and verifies HS256 tokens for Handshake-issued sessions.
type localJWT struct {
	secret []byte
	ttl    time.Duration
}

func newLocalJWT(secret []byte, ttl time.Duration) *localJWT {
	return &localJWT{secret: secret, ttl: ttl}
}

func (l *localJWT) issue(username string) (string, error) {
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Issuer:    localJWTIssuer,
		Subject:   username,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(l.ttl)),
	})
	return tok.SignedString(l.secret)
}

func (l *localJWT) verify(token string) (string, error) {
	var claims jwt.RegisteredClaims
	_, err := jwt.ParseWithClaims(token, &claims, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Method)
		}
		return l.secret, nil
	}, jwt.WithIssuer(localJWTIssuer), jwt.WithExpirationRequired())
	if err != nil {
		return "", err
	}
	if claims.Subject == "" {
		return "", errors.New("missing subject claim")
	}
	return claims.Subject, nil
}
