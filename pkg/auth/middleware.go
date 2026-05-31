package auth

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"golang.org/x/time/rate"
)

type contextKey string

const UserIDKey contextKey = "user_id"

// RequireAuth is HTTP middleware that validates a JWT and checks RBAC.
func RequireAuth(allowedRoles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
				http.Error(w, "missing or malformed authorization header", http.StatusUnauthorized)
				return
			}

			tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
			claims, err := ValidateToken(tokenStr)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			// RBAC check
			if len(allowedRoles) > 0 {
				hasRole := false
				for _, role := range allowedRoles {
					if claims.Role == role {
						hasRole = true
						break
					}
				}
				if !hasRole {
					http.Error(w, "forbidden: insufficient permissions", http.StatusForbidden)
					return
				}
			}

			ctx := context.WithValue(r.Context(), UserIDKey, claims.UserID.String())
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RateLimit implements an in-memory token bucket per IP address.
func RateLimit(r rate.Limit, b int) func(http.Handler) http.Handler {
	type client struct {
		limiter *rate.Limiter
	}
	var (
		mu      sync.Mutex
		clients = make(map[string]*client)
	)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ip := req.RemoteAddr
			// Handle cases where RemoteAddr contains port
			if idx := strings.LastIndex(ip, ":"); idx != -1 {
				ip = ip[:idx]
			}

			mu.Lock()
			if _, found := clients[ip]; !found {
				clients[ip] = &client{limiter: rate.NewLimiter(r, b)}
			}
			c := clients[ip]
			mu.Unlock()

			if !c.limiter.Allow() {
				http.Error(w, "too many requests", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, req)
		})
	}
}
