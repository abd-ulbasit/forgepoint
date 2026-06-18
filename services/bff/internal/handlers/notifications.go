package handlers

import (
	"log/slog"
	"net/http"

	notificationv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/notification/v1"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/httpx"
)

// NotificationsHandler maps /api/v1/notifications onto the notification service.
type NotificationsHandler struct {
	notification NotificationClient
	logger       *slog.Logger
}

func NewNotificationsHandler(notification NotificationClient, logger *slog.Logger) *NotificationsHandler {
	return &NotificationsHandler{notification: notification, logger: logger}
}

// List handles GET /api/v1/notifications. Optional unread_only flag from query.
func (h *NotificationsHandler) List(w http.ResponseWriter, r *http.Request) {
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.notification.ListNotifications(ctx, &notificationv1.ListNotificationsRequest{
		UnreadOnly: r.URL.Query().Get("unread_only") == "true",
		Pagination: httpx.Pagination(r),
	})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}
