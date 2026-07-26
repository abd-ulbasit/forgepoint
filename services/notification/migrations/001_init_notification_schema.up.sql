-- 001_init_notification_schema.up.sql — the Notification service's durable schema.
--
-- ============================================================================
-- WHAT THIS SCHEMA IS FOR: the inbox read model + per-user routing rules + the
-- delivery audit log of a pure CHOREOGRAPHY event reactor.
-- ============================================================================
--
-- The Notification service consumes platform events off NATS (`fp.>`), looks up
-- the recipient's preferences, DECIDES which channels to fire (IN_APP + webhook/
-- Slack/email), and records what it did. Four tables, one per domain concern:
--
--   notifications              the INBOX read model (one row per delivered event;
--                              what List/Get return and MarkRead mutates).
--   notification_preferences   the per-user ROUTING AGGREGATE (muted patterns +
--                              the updated_at stamp) — one row per user.
--   notification_channel_prefs the per-(user,channel) settings (enabled, floor,
--                              target). A child of notification_preferences so the
--                              aggregate is one document with N channel rows.
--   delivery_log               the per-channel per-attempt AUDIT TRAIL (DELIVERED/
--                              FAILED/SUPPRESSED + response detail). Append-only.
--
-- ----------------------------------------------------------------------------
-- DESIGN CHOICES
-- ----------------------------------------------------------------------------
--
-- IDs are TEXT (UUIDv4 strings), not native uuid. The domain carries ids as
-- strings everywhere (domain.UUIDGenerator → uuid.NewString()); TEXT keeps the
-- adapter a pass-through with no type translation at the boundary — the same call
-- every other Forgepoint service makes. (Native uuid would save 28 bytes/row and
-- add validation; we trade that for a dead-simple adapter and the domain owning
-- the id format.)
--
-- ENUMS (severity, channel, delivery status) are stored as the domain's SMALLINT
-- integer values, NOT a Postgres ENUM type and NOT free TEXT. WHY ints here (the
-- experiment-tracker chose TEXT enums — a deliberate per-service difference):
--   * The domain's enums are ALREADY int32 mirrored 1:1 from the proto wire values
--     (models.go: ChannelInApp=1, SeverityCritical=4, StatusDelivered=3, ...). So
--     the column is a trivial lossless cast (int16(n.Severity)) at the boundary —
--     no string lookup table, no drift between three representations.
--   * Severity ORDERING is load-bearing: the MinSeverity floor comparison is a
--     numeric >=. Storing the int lets the DB do `severity >= $n` directly for the
--     min-severity list filter, no CASE mapping.
--   * A CHECK constraint pins the legal set so a forged/out-of-range value can't
--     land even though the column is a plain SMALLINT — same "bad value can't be
--     written" guarantee a Postgres ENUM gives, with zero CREATE TYPE migration
--     friction when the proto adds a rung.
--
-- channels (the fan-out set on a notification) is SMALLINT[] — a Postgres array of
-- the channel ints. WHY an array column and not a child table: Channels is a small,
-- read-mostly SET that is always read WITH the notification and never queried
-- across rows ("which notifications used Slack?" is a delivery_log question, which
-- has its own indexed channel column). An array keeps the inbox read a single-row
-- fetch (no join) — exactly what a GitHub-style notification center wants.

-- ----------------------------------------------------------------------------
-- notifications — the INBOX READ MODEL (the central row List/Get/MarkRead touch).
-- ----------------------------------------------------------------------------
--
-- SECURITY / AUTHORITY: every column is SERVER-AUTHORITATIVE. recipient_user_id is
-- set from the matched preference's owner (the routing engine), NEVER from a
-- request body — there is no client write path to this table at all (the only
-- client mutation is MarkRead flipping read/read_at). This is the anti
-- mass-assignment rule realized in the schema: the table simply has no column a
-- client request populates.
CREATE TABLE notifications (
    -- Server-generated UUIDv4 (domain.IDGenerator). The stable inbox-entry id —
    -- NOT the originating event id (that is event_id below).
    id               TEXT        PRIMARY KEY,

    -- The user this was delivered to. THE anti-IDOR axis: every read/mutate on this
    -- table is scoped by recipient_user_id (the NotificationRepository ports take it
    -- as a parameter, never fetch by id alone). NOT NULL — a notification always
    -- belongs to exactly one recipient.
    recipient_user_id TEXT       NOT NULL,

    title            TEXT        NOT NULL,
    body             TEXT        NOT NULL DEFAULT '',

    -- Server-derived priority (drives sort + the min-severity list filter). SMALLINT
    -- holding the domain Severity int; CHECK pins 0..4 (UNSPECIFIED..CRITICAL).
    severity         SMALLINT    NOT NULL
                                 CHECK (severity BETWEEN 0 AND 4),

    -- The fan-out channels this notification was delivered over (IN_APP + 0..N
    -- external). SMALLINT[] of channel ints. The element CHECK pins every entry to
    -- the legal channel set so a bad value can't sneak in via the array.
    channels         SMALLINT[]  NOT NULL DEFAULT '{}'::smallint[]
                                 CHECK (channels <@ ARRAY[0,1,2,3,4]::smallint[]),

    -- read flips false→true via MarkRead/MarkAllRead. The partial index below makes
    -- the unread COUNT/filter cheap.
    read             BOOLEAN     NOT NULL DEFAULT FALSE,

    -- --- PROVENANCE (back-pointer to the originating EventEnvelope) ---

    -- The EventEnvelope.id that triggered this row. ALSO the idempotency anchor:
    -- re-processing the same (event_id, recipient) must not create a duplicate inbox
    -- row — enforced by the UNIQUE index below (the DB half of the idempotent
    -- consumer guarantee; the Redis IdempotencyStore is the fast pre-check).
    event_id         TEXT        NOT NULL,
    -- The matched EventEnvelope.type (e.g. "fp.pipelines.failed") and producing
    -- service ("pipeline"). For the optional event-type list filter + support triage.
    event_type       TEXT        NOT NULL DEFAULT '',
    source_service   TEXT        NOT NULL DEFAULT '',

    -- When the event was processed (server clock, immutable). The keyset sort axis.
    created_at       TIMESTAMPTZ NOT NULL,
    -- When the user marked it read. NULL while unread (the domain's zero time.Time
    -- maps to/from NULL). Server clock.
    read_at          TIMESTAMPTZ
);

-- IDEMPOTENT-CONSUMER BACKSTOP: at most one inbox row per (event, recipient).
--
-- WHY a UNIQUE index and not just the Redis pre-check: NATS is at-least-once, so a
-- redelivered event can reach the reactor again — and a TTL'd Redis key could have
-- expired, or two replicas could race the same redelivery between the Seen() check
-- and the Create(). This index is the DURABLE guarantee that a duplicate Create
-- fails with 23505 (which the adapter maps to ErrRepoAlreadyExists) rather than
-- silently double-inserting. Belt (Redis dedup) AND suspenders (this constraint) —
-- the standard "idempotency key in the database" pattern. (event_id, recipient)
-- because the SAME event can legitimately notify DIFFERENT users (one row each).
CREATE UNIQUE INDEX notifications_event_recipient_uniq
    ON notifications (event_id, recipient_user_id);

-- Keyset-list support: ListForUser pages a user's inbox newest-first by
-- (created_at, id). This composite index makes ORDER BY ... LIMIT a clean backwards
-- index scan (no sort), and (created_at, id) IS the cursor tuple.
CREATE INDEX notifications_recipient_created_idx
    ON notifications (recipient_user_id, created_at DESC, id DESC);

-- Unread badge + UnreadOnly filter: a PARTIAL index over only the unread rows.
-- WHY partial (WHERE read = FALSE): the badge COUNT and the unread list only ever
-- touch unread rows, and unread is the small, hot minority (most rows age into
-- read). A partial index is tiny and stays hot in cache, so CountUnread is a cheap
-- indexed COUNT rather than a full per-user scan — the classic "index the predicate
-- you actually filter on" optimization.
CREATE INDEX notifications_recipient_unread_idx
    ON notifications (recipient_user_id, created_at DESC, id DESC)
    WHERE read = FALSE;

-- ----------------------------------------------------------------------------
-- notification_preferences — the per-user ROUTING AGGREGATE root.
-- ----------------------------------------------------------------------------
--
-- One row per user. Holds the user-level fields (muted patterns + the write stamp);
-- the per-channel settings hang off it in notification_channel_prefs. WHY split the
-- aggregate across two tables instead of stuffing channels into a JSONB column:
-- the per-channel rows have a small fixed shape we want a CHECK on (valid channel,
-- valid floor) and that we read/replace as a set — a child table gives referential
-- integrity (the FK + ON DELETE CASCADE) and per-row CHECKs that JSONB can't. The
-- adapter still presents it to the domain as ONE NotificationPreferences document
-- (PUT semantics on Upsert), so the split is invisible above the repository.
--
-- SECURITY: user_id is server-set from the authenticated caller on read/write,
-- NEVER from a request body (prevents one user editing another's prefs via
-- mass-assignment). It is the PRIMARY KEY — exactly one preference document per user.
CREATE TABLE notification_preferences (
    user_id              TEXT        PRIMARY KEY,

    -- Event-type wildcard patterns muted across ALL channels (e.g. "fp.inference.*").
    -- TEXT[] (small, read-with-the-aggregate). Defaults to empty so a read never has
    -- to special-case NULL — the mute list is always a (possibly empty) array. The
    -- SERVICE caps its length (maxMutePatterns) before write; we don't re-cap here.
    muted_event_patterns TEXT[]      NOT NULL DEFAULT '{}'::text[],

    -- When the prefs were last written (server clock). Returned to the settings UI.
    updated_at           TIMESTAMPTZ NOT NULL
);

-- ----------------------------------------------------------------------------
-- notification_channel_prefs — the per-(user,channel) routing settings.
-- ----------------------------------------------------------------------------
--
-- A child of notification_preferences: one row per channel the user configured.
-- (user_id, channel) is the PRIMARY KEY, so a user has at most one setting per
-- channel — the domain's ChannelFor lookup is a unique row.
CREATE TABLE notification_channel_prefs (
    -- FK to the aggregate root. ON DELETE CASCADE so replacing/clearing a user's
    -- prefs (the Upsert "delete children then re-insert" path) and any future user
    -- deletion clean up the channel rows automatically — no orphans.
    user_id      TEXT     NOT NULL
                          REFERENCES notification_preferences(user_id) ON DELETE CASCADE,

    -- Which transport this row configures. SMALLINT channel int; CHECK pins 0..4.
    -- (We allow IN_APP rows even though IN_APP is implicitly always-on — storing an
    -- explicit IN_APP preference is harmless and the domain's ChannelFor handles
    -- both the present and absent cases.)
    channel      SMALLINT NOT NULL
                          CHECK (channel BETWEEN 0 AND 4),

    -- Master on/off for the channel. A disabled channel is SUPPRESSED (a decision),
    -- never FAILED.
    enabled      BOOLEAN  NOT NULL DEFAULT TRUE,

    -- The min-severity floor for THIS channel. SMALLINT Severity; CHECK 0..4.
    -- 0 (UNSPECIFIED) = no floor (deliver everything) per Severity.MeetsFloor.
    min_severity SMALLINT NOT NULL DEFAULT 0
                          CHECK (min_severity BETWEEN 0 AND 4),

    -- Channel-specific routing target: WEBHOOK/SLACK → the HTTPS URL (SSRF-guarded
    -- by the SERVICE before it ever reaches here — the persistence layer trusts that
    -- the service already validated it), EMAIL → the address, IN_APP → '' (ignored).
    -- Stored as '' (not NULL) since the domain carries it as a string.
    target       TEXT     NOT NULL DEFAULT '',

    PRIMARY KEY (user_id, channel)
);

-- ----------------------------------------------------------------------------
-- delivery_log — the per-channel, per-attempt AUDIT TRAIL (append-only).
-- ----------------------------------------------------------------------------
--
-- One Notification fans out to N channels, each retried K times, so attempts are a
-- SEPARATE table from notifications (flattening would lose the per-channel/per-try
-- structure). The consumer adapter appends one row per attempt AND one row for each
-- SUPPRESSED channel decision (a held decision is recorded, not silently dropped).
--
-- SECURITY: response_code/error_message describe OUR delivery outcome only — we
-- deliberately do NOT store the target URL or any auth header here, just enough to
-- debug delivery health without echoing a secret or PII (matches the domain's
-- DeliveryAttempt comment).
CREATE TABLE delivery_log (
    -- Surrogate PK. WHY a synthetic id rather than (notification_id, channel,
    -- attempt): a SUPPRESSED record has attempt=0 and there can be several attempts
    -- per channel; a plain serial id keeps every append unconditional (no ON CONFLICT
    -- bookkeeping on the hot audit path) and gives the keyset cursor a guaranteed-
    -- unique tiebreaker. BIGINT identity (Postgres 10+ GENERATED ALWAYS) — never
    -- client-set.
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- The notification this attempt belongs to. FK with ON DELETE CASCADE so a
    -- notification's audit rows go with it. recipient_user_id is DENORMALIZED onto
    -- this table (below) rather than reached via a join, because the fleet
    -- ListDeliveryAttemptsForUser view filters by user directly and we want it
    -- index-served without joining notifications on every page.
    notification_id TEXT        NOT NULL
                                REFERENCES notifications(id) ON DELETE CASCADE,

    -- DENORMALIZED owner for the anti-IDOR user scope on the fleet view + the
    -- per-notification GetAttempts path. Set by the adapter from the notification's
    -- recipient — server-authoritative, never client input.
    recipient_user_id TEXT      NOT NULL,

    -- The transport this attempt used. SMALLINT channel int; CHECK 0..4. (IN_APP
    -- attempts are valid too — the inbox "delivery" is a recorded success.)
    channel         SMALLINT    NOT NULL
                                CHECK (channel BETWEEN 0 AND 4),

    -- The outcome of THIS attempt. SMALLINT DeliveryStatus; CHECK 0..5
    -- (UNSPECIFIED..SUPPRESSED).
    status          SMALLINT    NOT NULL
                                CHECK (status BETWEEN 0 AND 5),

    -- 1-based try number within the retry budget. 0 = not-yet-attempted, e.g. a
    -- SUPPRESSED or PENDING record (those have no network try).
    attempt         INTEGER     NOT NULL DEFAULT 0
                                CHECK (attempt >= 0),

    -- Final HTTP status for WEBHOOK/SLACK (e.g. 200, 503); 0 for non-HTTP channels
    -- or when no response was received (timeout).
    response_code   INTEGER     NOT NULL DEFAULT 0,

    -- Human-readable failure detail for logs (empty on success). NOT for end-user
    -- display; never the target URL or an auth header.
    error_message   TEXT        NOT NULL DEFAULT '',

    -- When this attempt was made (server clock). The keyset sort axis for the log.
    attempted_at    TIMESTAMPTZ NOT NULL
);

-- Per-notification attempts view (GetAttemptsForNotification): oldest-first by
-- (attempted_at, id) within one notification, scoped by user. This index serves
-- the WHERE notification_id = $1 AND recipient_user_id = $2 ORDER BY attempted_at
-- scan directly.
CREATE INDEX delivery_log_notification_idx
    ON delivery_log (notification_id, recipient_user_id, attempted_at ASC, id ASC);

-- Fleet view (ListDeliveryAttemptsForUser): newest-first by (attempted_at, id)
-- across all of a user's notifications, with optional channel/status filters.
-- Leading with recipient_user_id keeps the user scope index-served; the trailing
-- (attempted_at DESC, id DESC) matches the keyset ORDER BY for a backwards scan.
CREATE INDEX delivery_log_recipient_attempted_idx
    ON delivery_log (recipient_user_id, attempted_at DESC, id DESC);
