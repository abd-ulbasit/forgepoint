package handlers

import (
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/httpx"
)

// AuthHandler proxies the browser login to the auth service. This is the BFF's
// cookie<->JWT bridge seam (the ADR's browser-session concern).
type AuthHandler struct {
	auth   AuthClient
	logger *slog.Logger
}

// NewAuthHandler wires the (segmented) auth client.
func NewAuthHandler(auth AuthClient, logger *slog.Logger) *AuthHandler {
	return &AuthHandler{auth: auth, logger: logger}
}

// loginRequest is the SPA-facing JSON body. We accept JSON (browser-idiomatic)
// and translate to the auth proto. NEVER logged — it carries the password.
type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// loginResponse is the SPA-facing JSON. We deliberately do NOT just relay the
// proto: we shape it for the browser and, crucially, control what comes back.
type loginResponse struct {
	// AccessToken is the JWT the SPA stores and sends back as
	// "Authorization: Bearer <token>" (dev flow). The BFF then forwards it
	// verbatim to every downstream call.
	AccessToken string `json:"accessToken"`
	// ExpiresAt is RFC3339 so the SPA can pre-emptively re-login before expiry.
	ExpiresAt string `json:"expiresAt"`
	// User is the authenticated profile, echoed so the SPA avoids a follow-up
	// GetUser round trip (the proto already returns it on login).
	User *userView `json:"user"`
}

type userView struct {
	ID    string `json:"id"`
	Email string `json:"email"`
	Name  string `json:"name"`
	Team  string `json:"team"`
	Role  string `json:"role"`
}

// Login handles POST /api/v1/login. PUBLIC route (no token required — this is
// where the token is OBTAINED).
//
// AUTH-SHAPE DECISION (documented per the task):
//
//	The token is returned in the JSON BODY for the SPA dev flow. The MORE SECURE
//	option is an httpOnly+Secure+SameSite cookie: a body token must live in JS
//	(localStorage/memory) and is therefore reachable by XSS, whereas an httpOnly
//	cookie is invisible to JS. The BFF's RequireAuth middleware already ACCEPTS
//	the cookie transport (httpx.cookieName), so flipping to cookies later is a
//	one-line change here (Set-Cookie instead of body) with no SPA-auth-forwarding
//	rewrite. For local SPA development the body is simpler (no CSRF token dance),
//	so we ship the body now and document the cookie as the production upgrade.
//
// SECURITY: we never log the request (password) or the response (token). On
// success we log only the non-secret email + user id.
func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	var body loginRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.Email == "" || body.Password == "" {
		httpx.WriteError(w, http.StatusBadRequest, "email and password are required")
		return
	}

	// Call auth Login. No token to forward (this IS the token-minting call).
	resp, err := h.auth.Login(r.Context(), &authv1.LoginRequest{
		Email:    body.Email,
		Password: body.Password,
	})
	if err != nil {
		// Invalid credentials surface as Unauthenticated -> 401 via the mapping.
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}

	out := loginResponse{
		AccessToken: resp.GetAccessToken(),
	}

	// PROFILE + EXPIRY POPULATION — two sources, in priority order:
	//
	//  1. The auth LoginResponse's OWN user/expires_at fields, IF the auth service
	//     populates them. They are the authoritative, structured source.
	//  2. FALLBACK: decode the access token's JWT claims. In M1 the auth service's
	//     Login returns ONLY access_token (expires_at/user are left unset — see
	//     auth_handler_rpcs.go), so without this fallback the SPA receives
	//     user:null / expiresAt:"" and the header renders "User"/"?". The JWT
	//     itself carries everything the header needs (sub/email/name/team/role/exp),
	//     so we read it back out here for display convenience.
	//
	// WHY WE DO NOT VERIFY THE SIGNATURE HERE: the BFF is not
	// an identity authority (see httpx/auth.go) and this token did not arrive from
	// an untrusted client — we MINTED it one line above via a trusted in-cluster
	// gRPC call to the auth service. Re-verifying would force the BFF to hold
	// FP_JWT_SECRET, which is exactly the coupling the ADR forbids. The decoded
	// claims are used ONLY to render the user's own profile back to that same user;
	// no authorization decision is made on them. Downstream services still verify
	// the signature on every forwarded call, so a tampered token gains nothing.
	if ts := resp.GetExpiresAt(); ts != nil {
		out.ExpiresAt = ts.AsTime().UTC().Format(time.RFC3339)
	}
	if u := resp.GetUser(); u != nil {
		out.User = &userView{
			ID:    u.GetId(),
			Email: u.GetEmail(),
			Name:  u.GetName(),
			Team:  u.GetTeam(),
			Role:  u.GetRole(),
		}
	}
	// Fallback: fill any field the auth response left blank from the JWT claims.
	if out.User == nil || out.ExpiresAt == "" {
		if claims, ok := decodeJWTClaims(resp.GetAccessToken()); ok {
			if out.User == nil {
				out.User = &userView{
					ID:    claims.Sub,
					Email: claims.Email,
					Name:  claims.Name,
					Team:  claims.Team,
					Role:  claims.Role,
				}
			}
			if out.ExpiresAt == "" && claims.Exp > 0 {
				out.ExpiresAt = time.Unix(claims.Exp, 0).UTC().Format(time.RFC3339)
			}
		}
	}

	// Log success WITHOUT the token. Email + id are non-secret audit fields.
	h.logger.Info("login succeeded",
		slog.String("email", body.Email),
		slog.String("user_id", out.User.GetID()),
	)

	httpx.WriteJSON(w, http.StatusOK, out)
}

// GetID is a nil-safe accessor used only by the log line above so a (defensive)
// nil user can't panic the success log.
func (u *userView) GetID() string {
	if u == nil {
		return ""
	}
	return u.ID
}

// jwtClaims mirrors the subset of the auth service's JWT payload the SPA header
// needs. The field tags match exactly how the auth service signs them
// (services/auth/internal/domain/jwt.go): "sub" is the registered subject claim
// (the user id); email/name/team/role are private claims; "exp" is the registered
// expiry as a NumericDate (Unix SECONDS, per RFC 7519 §2). Any field the token
// omits stays at its zero value.
type jwtClaims struct {
	Sub   string `json:"sub"`
	Email string `json:"email"`
	Name  string `json:"name"`
	Team  string `json:"team"`
	Role  string `json:"role"`
	Exp   int64  `json:"exp"`
}

// decodeJWTClaims base64url-decodes the PAYLOAD segment of a JWT and unmarshals
// it into jwtClaims. It returns ok=false on any malformed input so the caller can
// silently skip the convenience fields rather than fail the login.
//
// IT DELIBERATELY DOES NOT VERIFY THE SIGNATURE. A JWT is three base64url
// segments joined by dots: header.payload.signature. We split on '.', take
// segment [1] (the payload), and decode it. This is safe ONLY because the token
// originated from the trusted auth service over an in-cluster call and is used
// solely to echo the caller's own profile (no authZ decision). See the long
// comment at the call site for the full rationale.
//
// RawURLEncoding is the correct alphabet: JWT uses base64url WITHOUT padding
// (RFC 7515 §2 / RFC 4648 §5). StdEncoding would reject the '-'/'_' characters
// and the missing '=' padding.
func decodeJWTClaims(token string) (jwtClaims, bool) {
	var claims jwtClaims
	if token == "" {
		return claims, false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return claims, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return claims, false
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return claims, false
	}
	return claims, true
}
