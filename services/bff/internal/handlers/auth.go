package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"

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
	if ts := resp.GetExpiresAt(); ts != nil {
		out.ExpiresAt = ts.AsTime().UTC().Format("2006-01-02T15:04:05Z07:00")
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
