package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const localJWTIssuer = "duckflight"

// sessionClaims extends the standard claims with a session id that pins the
// client to a dedicated DuckDB connection on the server. See
// [internal/session.Manager].
type sessionClaims struct {
	SessionID string `json:"sid,omitempty"`
	jwt.RegisteredClaims
}

// localJWT mints and verifies HS256 tokens for Handshake-issued sessions.
type localJWT struct {
	secret []byte
	ttl    time.Duration
}

func newLocalJWT(secret []byte, ttl time.Duration) *localJWT {
	return &localJWT{secret: secret, ttl: ttl}
}

// issue mints a fresh JWT for the given username, stamping a unique session id
// so subsequent requests from the same handshake land on the same DuckDB
// connection.
func (l *localJWT) issue(username string) (string, error) {
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, sessionClaims{
		SessionID: uuid.NewString(),
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    localJWTIssuer,
			Subject:   username,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(l.ttl)),
		},
	})
	return tok.SignedString(l.secret)
}

// verify validates the token and returns (subject, sid).
func (l *localJWT) verify(token string) (string, string, error) {
	var claims sessionClaims
	_, err := jwt.ParseWithClaims(token, &claims, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Method)
		}
		return l.secret, nil
	}, jwt.WithIssuer(localJWTIssuer), jwt.WithExpirationRequired())
	if err != nil {
		return "", "", err
	}
	if claims.Subject == "" {
		return "", "", errors.New("missing subject claim")
	}
	return claims.Subject, claims.SessionID, nil
}
