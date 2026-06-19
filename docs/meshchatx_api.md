# Reticulum MeshChatX — Web API Reference

The MeshChatX backend (`meshchatx/meshchat.py`) exposes its entire feature set over
HTTP + WebSocket on a single `aiohttp` app. The same surface is consumed by the
bundled Vue frontend, the Electron shell, and the Android Chaquopy build.

- **Base URL (default)**: `http://127.0.0.1:8000`
- **CLI**: `meshchatx --headless --host 127.0.0.1 --port 8000`
- **Environment overrides**: `MESHCHAT_HOST`, `MESHCHAT_PORT`
- **WebSocket URL**: `ws://127.0.0.1:8000/ws`
- **Auxiliary WebSocket (call audio bridge)**: `ws://127.0.0.1:8000/ws/telephone/audio`
- **All REST endpoints respond JSON** unless explicitly streaming audio, file
  bytes, or static assets.

> The implementation is dispatched from `meshchatx/meshchat.py` (~19k LoC).
> File:line references throughout this document point at the canonical handler.

---

## 1. Authentication

Authentication is **optional**. When enabled it is local-password only, backed
by an `aiohttp-session` cookie and bcrypt password hashes stored in config.

| Method | Path | Purpose |
| ------ | ---- | ------- |
| GET    | `/api/v1/auth/status` | Reports `auth_enabled`, `password_set`, `authenticated` |
| POST   | `/api/v1/auth/setup`  | First-time password set; rejected once a password exists |
| POST   | `/api/v1/auth/login`  | Body `{"password": "..."}`; sets session cookie on success |
| POST   | `/api/v1/auth/logout` | Invalidates the session cookie |

When `auth_enabled` is true, all non-auth endpoints require the session cookie.
Failed attempts are recorded in `/api/v1/debug/access-attempts`. Trusted IP/UA
combinations bypass repeated login challenges.

> Source: `meshchatx/meshchat.py` lines 4192–4432.

---

## 2. WebSocket Protocol

Two long-lived WebSockets carry asynchronous data:

### 2.1 `GET /ws` — main event bus

Frames are JSON text. Each message has a top-level `"type"` field. The server
broadcasts state changes to all connected clients and supports a request /
response pattern keyed by `type`.

#### Inbound (client → server) message types

| `type` | Required fields | Behavior |
| --- | --- | --- |
| `ping` | — | Server replies `{"type":"pong"}` |
| `config.set` | `config` (object) | Update config, then broadcast `config` |
| `nomadnet.download.cancel` | `download_id` | Cancel in-flight nomadnet download |
| `nomadnet.page.archives.get` | `destination_hash`, `page_path` | List archived versions for a page |
| `nomadnet.page.archive.load` | `archive_id` | Send an archived page to the client (status=success, archived) |
| `nomadnet.page.archive.flush` | — | Delete all archived pages, rebroadcast `config` |
| `nomadnet.page.archive.add` | `destination_hash`, `page_path`, `content` | Manually archive a page |
| `nomadnet.file.download` | `nomadnet_file_download` ({`destination_hash`, `file_path`, `data?`}) | Stream phases: `started` → `phase` → `progress` → `success`/`failure` |
| `nomadnet.page.download` | `nomadnet_page_download` ({`destination_hash`, `page_path`, `field_data?`}) | Same streaming model |
| `lxmf.forwarding.rules.get` | — | Returns `{"type":"lxmf.forwarding.rules", "rules":[…]}` |
| `lxmf.forwarding.rule.add` | `rule` ({`forward_to_hash`, `identity_hash?`, `source_filter_hash?`, `is_active?`, `name?`}) | Insert + rebroadcast list |
| `lxmf.forwarding.rule.delete` | `id` | Remove + rebroadcast list |
| `lxmf.forwarding.rule.toggle` | `id` | Toggle + rebroadcast list |
| `lxm.ingest_uri` | `uri` | Decodes `lxmf://`, `lxm://`, `lxma://`, `meshchatx://map`, `meshchatx://docs`; replies `lxm.ingest_uri.result` |
| `lxm.generate_paper_uri` | `destination_hash`, `content`, `title?` | Returns LXMF paper-message URI |
| `keyboard_shortcuts.get` | — | Returns `{"type":"keyboard_shortcuts", "shortcuts":[…]}` |
| `keyboard_shortcuts.set` | `action`, `keys` (array) | Upsert + rebroadcast list |
| `keyboard_shortcuts.delete` | `action` | Remove + rebroadcast list |
| `rns.link.open` | `destination_hash` (hex), `aspect` (dot-separated), `auto_identify?` (bool, default `false`), `request_id` | Open (or reuse) an RNS Link to `(aspect, destination_hash)`. Streams `phase` events (`finding_path`, `establishing_link`, `identifying`), then a final `success` (with `identified: bool`) or `failure` (with `failure_reason`). |
| `rns.link.identify` | `destination_hash`, `aspect`, `request_id` | `link.identify(self.identity)` on the cached link. Replies `success` or `failure` (`no_active_link`, `no_local_identity`, …). |
| `rns.link.request` | `destination_hash`, `aspect`, `path`, `data_b64?`, `timeout?`, `request_id` | Generic request/response over an RNS Link — drop-in replacement for `nomadnet.page.download` with configurable `aspect`. **`data_b64` is the msgpack encoding of the request payload**; the server msgpack-decodes it and passes the resulting native value to `RNS.Link.request(data=…)`, so the structure embeds natively in the wire envelope. **`body_b64` in the reply is the msgpack encoding of whatever the remote handler returned**; the caller msgpack-decodes to recover the native value. To send opaque bytes, msgpack-wrap them as a `bin` value before encoding; on receipt they'll come back as `bin` too. Streams `phase`/`progress` then final `success` (`body_b64`) or `failure` (`failure_reason`). |
| `rns.link.send` | `destination_hash`, `aspect`, `payload_b64`, `request_id` | `RNS.Packet(link, payload).send()` on the cached link. Replies `success` or `failure`. |
| `rns.link.close` | `destination_hash`, `aspect`, `request_id` | `link.teardown()` + un-cache. Replies `success` or `failure` (`no_active_link`). |

> Source: `meshchatx/meshchat.py::on_websocket_data_received` (line 15109).

#### Outbound (server → client) broadcast/response types

| `type` | Trigger / payload summary |
| --- | --- |
| `pong` | Reply to inbound `ping` |
| `config` | Full app config on connect and after each `config.set` |
| `announced` | The local identity has just announced; payload `{identity_hash}` |
| `announce` | New/updated announce observed (broadcast to all clients) |
| `lxmf_message` | A new inbound LXMF message has been stored |
| `lxmf_message_created` | A new outbound LXMF message has been queued |
| `lxmf_message_state_updated` | Delivery / failure / progress for an outbound message |
| `lxmf_message_deleted` | Server-side deletion of a message |
| `lxmf.delivery` | Live delivery notification with attachments/extension fields |
| `lxmf.telemetry` | Telemetry datagram from a peer |
| `lxmf.forwarding.rules` | Snapshot list (response to `lxmf.forwarding.rules.get`) |
| `nomadnet.page.download` | Status frames: `started`, `phase`, `progress`, `success`, `failure` |
| `nomadnet.file.download` | Same status model as page download |
| `nomadnet.page.archives` | Archive index for a destination/page |
| `nomadnet.page.archive.added` | Confirmation of manual archive |
| `nomadnet.download.cancelled` | Confirmation of cancel |
| `rrc.change` | Reticulum Rooms/Chat hub or room state change |
| `rrc.message` | New RRC chat message |
| `rrc.server.change` | Local RRC server state changed |
| `rnsh.session.change` | RNSH (remote shell) session lifecycle change |
| `rnsh.output` | Streamed terminal stdout/stderr |
| `rncp.transfer.progress` | RNCP file transfer progress |
| `rncp.send.completed` | RNCP outbound completed |
| `rncp.fetch.completed` | RNCP inbound completed |
| `telephone_ringing` | Incoming LXST call |
| `telephone_call_established` | Call answered (local or remote) |
| `telephone_call_ended` | Call hangup / failure |
| `telephone_initiation_status` | Outgoing call setup state |
| `telephone_missed_call` | Notification of a missed call |
| `new_voicemail` | A voicemail has just been stored |
| `keyboard_shortcuts` | Snapshot of saved shortcut bindings |
| `lxm.ingest_uri.result` | Result of an `lxm.ingest_uri` call (status: success/info/warning/error) |
| `lxm.generate_paper_uri.result` | URI for the LXMF paper message |
| `reticulum_reload_status` | Status of a `reticulum/reload` operation |
| `identity_switched` | Active identity changed; UI should refetch |
| `blocked_destinations` | Snapshot of block list |
| `rns.link.open` / `rns.link.identify` / `rns.link.request` / `rns.link.send` / `rns.link.close` | Per-`request_id` reply frames for the generic RNS Link API. Status field is `phase`, `progress`, `success`, or `failure` (mirrors the `nomadnet.page.download` shape). |
| `rns.link.event` | Asynchronous Link event broadcast to all clients. `event` is `packet_received` (with `payload_b64`) or `link_closed`. Carries `destination_hash` and `aspect`. |

Connections are reflected in `self.websocket_clients`. Max inbound message size
on `/ws` is **50 MiB**.

### 2.2 `GET /ws/telephone/audio` — call audio bridge

Used by the browser/Electron client to deliver microphone audio frames when
LXST cannot use a host audio device (e.g. headless Linux, Chaquopy on Android).

- Binary frames pushed by the client are forwarded to the active call
  (`web_audio_bridge.push_client_frame`).
- Text frames support `{"type":"attach"}` (attach to current call) and
  `{"type":"ping"}` (→ `{"type":"pong"}`).
- The server emits status frames via `web_audio_bridge.send_status` and may
  emit `{"type":"error", "message":"..."}` if audio bridge is disabled or no
  call is active.
- Max inbound message size: **5 MiB**.

> Source: `meshchatx/meshchat.py` lines 5809–5891.

---

## 3. REST Endpoints

All endpoints below live under the root host. Path params are shown in
`{braces}`. Unless noted, requests/responses are JSON and the version prefix is
`/api/v1`.

### 3.1 App lifecycle & metadata

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/` | Serves the SPA `index.html` |
| GET | `/manifest.json` | PWA manifest |
| GET | `/service-worker.js` | PWA service worker |
| GET | `/call.html` | Standalone call window |
| GET | `/robots.txt` | Static |
| GET | `/api/v1/status` | Liveness: `{"status":"ok"}` |
| GET | `/api/v1/app/info` | Process / memory / config snapshot used by Diagnostics UI |
| GET | `/api/v1/app/changelog` | Markdown changelog content |
| POST | `/api/v1/app/changelog/seen` | Mark current changelog as seen |
| GET | `/api/v1/licenses` | Bundled third-party notices |
| POST | `/api/v1/app/tutorial/seen` | Hide first-run tutorial |
| POST | `/api/v1/app/integrity/acknowledge` | Acknowledge backend-integrity warning |
| POST | `/api/v1/app/shutdown` | Request orderly shutdown |
| POST | `/api/v1/setup/storage-migration` | Migrate storage paths |

### 3.2 Configuration

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET   | `/api/v1/config` | Full config (also pushed via WS `config`) |
| PATCH | `/api/v1/config` | Partial config update; rebroadcast over WS |

### 3.3 Debug / diagnostics / database

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/debug/logs` | Persistent log buffer |
| GET | `/api/v1/debug/access-attempts` | Login / access-attempt audit |
| GET | `/api/v1/diagnostics/memory` | Memory summary |
| POST | `/api/v1/diagnostics/memory/snapshot` | Take heap snapshot |
| GET | `/api/v1/diagnostics/memory/heap` | Heap details |
| GET | `/api/v1/diagnostics/memory/gc` | GC stats |
| POST | `/api/v1/diagnostics/memory/gc/collect` | Trigger gc.collect() |
| GET | `/api/v1/diagnostics/memory/referrers` | Object referrers report |
| POST | `/api/v1/diagnostics/memory/reset` | Reset diagnostics state |
| GET | `/api/v1/database/health` | Vacuum / WAL / busy-timeout state |
| POST | `/api/v1/database/vacuum` | VACUUM |
| POST | `/api/v1/database/recover` | Run recovery routine |
| POST | `/api/v1/database/snapshot` | Create snapshot |
| GET | `/api/v1/database/snapshots` | List snapshots |
| DELETE | `/api/v1/database/snapshots/{filename}` | Delete snapshot |
| GET | `/api/v1/database/snapshots/{filename}/download` | Download snapshot file |
| POST | `/api/v1/database/restore` | Restore from snapshot (multipart) |
| POST | `/api/v1/database/backup` | Trigger backup |
| GET | `/api/v1/database/backups` | List backups |
| DELETE | `/api/v1/database/backups/{filename}` | Delete backup |
| GET | `/api/v1/database/backups/{filename}/download` | Download backup |
| GET | `/api/v1/database/backup/download` | Latest backup download |

### 3.4 Identity management

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/identities` | List local identities |
| GET | `/api/v1/identities/export-all` | Export every identity (encrypted bundle) |
| POST | `/api/v1/identities/create` | Create a new identity |
| DELETE | `/api/v1/identities/{identity_hash}` | Remove an identity |
| POST | `/api/v1/identities/switch` | Switch the active identity (broadcasts `identity_switched`) |
| GET | `/api/v1/identity/backup/download` | Download active identity backup (binary) |
| GET | `/api/v1/identity/backup/base32` | Base32 encoded backup |
| POST | `/api/v1/identity/restore` | Restore from backup |

### 3.5 Reticulum / interfaces

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/reticulum/interfaces` | List configured interfaces |
| POST | `/api/v1/reticulum/interfaces/add` | Add an interface (body: `name`, `type`, `allow_overwriting_interface?`, type-specific fields) |
| POST | `/api/v1/reticulum/interfaces/enable` | Enable by name |
| POST | `/api/v1/reticulum/interfaces/disable` | Disable by name |
| POST | `/api/v1/reticulum/interfaces/delete` | Delete by name |
| POST | `/api/v1/reticulum/interfaces/export` | Export interface config |
| POST | `/api/v1/reticulum/interfaces/import-preview` | Preview an import |
| POST | `/api/v1/reticulum/interfaces/import` | Commit imported interfaces |
| GET | `/api/v1/reticulum/discovery` | Auto-discovery state |
| PATCH | `/api/v1/reticulum/discovery` | Toggle auto-discovery |
| GET | `/api/v1/reticulum/discovered-interfaces` | Discovered LAN/Internet candidates |
| POST | `/api/v1/reticulum/enable-transport` | Become a transport node |
| POST | `/api/v1/reticulum/disable-transport` | Stop being a transport node |
| POST | `/api/v1/reticulum/reload` | Reload Reticulum (broadcasts `reticulum_reload_status`) |
| GET | `/api/v1/reticulum/config/raw` | Raw `config` text |
| PUT | `/api/v1/reticulum/config/raw` | Replace raw config text |
| POST | `/api/v1/reticulum/config/reset` | Reset to defaults |
| GET | `/api/v1/reticulum/blackhole` | Blackhole filter state |
| GET | `/api/v1/community-interfaces` | Bundled community interfaces directory |
| POST | `/api/v1/community-interfaces/refresh` | Fetch latest community list |
| GET | `/api/v1/comports` | Serial port enumeration |
| GET | `/api/v1/system/network-interfaces` | OS network interface list |
| GET | `/api/v1/tools/rnode/latest_release` | RNode firmware metadata |
| GET | `/api/v1/tools/rnode/download_firmware` | Download a firmware binary (`?file=...`) |
| GET | `/api/v1/tools/micron-parser-go-release` | Micron parser release metadata |
| GET | `/api/v1/interface-stats` | RNS internal interface counters |
| GET | `/api/v1/rnstatus` | Equivalent of `rnstatus` CLI |
| GET | `/api/v1/path-table` | Full path table |
| POST | `/api/v1/path-table` | Filtered path table (`destination_hashes`) |
| GET | `/api/v1/rnpath/table` | RNS path table (alternate view) |
| GET | `/api/v1/rnpath/rates` | Per-interface rate stats |
| POST | `/api/v1/rnpath/drop` | Drop a specific path |
| POST | `/api/v1/rnpath/drop-via` | Drop all paths via a hop |
| POST | `/api/v1/rnpath/drop-queues` | Drop transport queues |
| POST | `/api/v1/rnpath/request` | Request a new path |
| GET | `/api/v1/rnpath/trace/{destination_hash}` | Trace path |
| POST | `/api/v1/rnprobe` | Send a probe |

### 3.6 Destination utilities

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/destination/{destination_hash}/path` | Path info |
| POST | `/api/v1/destination/{destination_hash}/drop-path` | Drop path |
| POST | `/api/v1/destination/{destination_hash}/request-path` | Request path |
| GET | `/api/v1/destination/{destination_hash}/signal-metrics` | RSSI/SNR over time |
| GET | `/api/v1/destination/{destination_hash}/custom-display-name` | Get user-defined display name |
| POST | `/api/v1/destination/{destination_hash}/custom-display-name` | Set/clear it (line 11390 in `meshchat.py`) |
| GET | `/api/v1/destination/{destination_hash}/lxmf-stamp-info` | LXMF stamp cost / proof state |
| GET | `/api/v1/ping/{destination_hash}/lxmf.delivery` | Send a single ping (streams via WS) |

### 3.7 Announces & favourites

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/announce` | Force-announce the local identity |
| GET | `/api/v1/announces` | Paginated announces. Query: `aspect`, `identity_hash`, `destination_hash`, `search`, `limit`, `offset`, `include_blocked` |
| GET | `/api/v1/favourites` | List favourites |
| POST | `/api/v1/favourites/add` | Add favourite (body: destination_hash, name, …) |
| POST | `/api/v1/favourites/{destination_hash}/rename` | Rename favourite |
| DELETE | `/api/v1/favourites/{destination_hash}` | Remove favourite |
| POST | `/api/v1/favourites/import` | Import favourites JSON |

### 3.8 LXMF messaging

| Method | Path | Notes |
| ------ | ---- | ----- |
| POST | `/api/v1/lxmf-messages/send` | Send a message. Body: `{"lxmf_message":{"destination_hash":..., "content":..., "fields":{"image":{…},"audio":{…},"file_attachments":[…],"telemetry":…,"commands":[…],"app_extensions":{…}},"reply_to_hash":?,"reply_quoted_content":?}, "delivery_method":?}` |
| POST | `/api/v1/lxmf-messages/reactions` | Toggle reaction |
| POST | `/api/v1/lxmf-messages/{hash}/cancel` | Cancel outbound message |
| POST | `/api/v1/lxmf-messages/{hash}/spam` | Mark as spam |
| DELETE | `/api/v1/lxmf-messages/{hash}` | Delete one message |
| GET | `/api/v1/lxmf-messages/conversation/{destination_hash}` | Full conversation thread. Query: `limit`, `offset`, `order`, … |
| DELETE | `/api/v1/lxmf-messages/conversation/{destination_hash}` | Delete an entire conversation |
| GET | `/api/v1/lxmf-messages/attachment/{message_hash}/{attachment_type}` | Stream raw attachment bytes |
| GET | `/api/v1/lxmf-messages/{message_hash}/uri` | Generate paper/share URI |

### 3.9 Conversations / folders / sieve

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/lxmf/conversations` | Conversation list. Query: `search` (alias `q`), `unread`, `failed`, `has_attachments`, `folder_id`, `limit`, `offset` |
| POST | `/api/v1/lxmf/conversations/{destination_hash}/mark-as-read` | Mark single conversation read |
| POST | `/api/v1/lxmf/conversations/bulk-mark-as-read` | Body: `{"destination_hashes":[…]}` |
| POST | `/api/v1/lxmf/conversations/bulk-delete` | Bulk delete |
| POST | `/api/v1/lxmf/conversations/move-to-folder` | Move list to folder |
| GET | `/api/v1/lxmf/conversation-pins` | Pinned conversations |
| POST | `/api/v1/lxmf/conversation-pins/toggle` | Toggle pin |
| GET | `/api/v1/lxmf/folders` | List folders |
| POST | `/api/v1/lxmf/folders` | Create folder |
| PATCH | `/api/v1/lxmf/folders/{id}` | Rename/edit folder |
| DELETE | `/api/v1/lxmf/folders/{id}` | Delete folder |
| GET | `/api/v1/lxmf/folders/export` | Export folder tree |
| POST | `/api/v1/lxmf/folders/import` | Import folder tree |
| GET | `/api/v1/lxmf/sieve-filters` | Sieve filter rules |
| PUT | `/api/v1/lxmf/sieve-filters` | Replace sieve filter rules |

### 3.10 Notifications

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/notifications` | Notifications list |
| POST | `/api/v1/notifications/mark-as-viewed` | Body: `{"notifications":[…]}` |

### 3.11 LXMF propagation node

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/lxmf/propagation-node/status` | Status |
| GET | `/api/v1/lxmf/propagation-node/sync` | Trigger sync |
| GET | `/api/v1/lxmf/propagation-node/stop-sync` | Stop sync |
| POST | `/api/v1/lxmf/propagation-node/stop` | Stop node |
| POST | `/api/v1/lxmf/propagation-node/restart` | Restart node |
| GET | `/api/v1/lxmf/propagation-nodes` | Known nodes |

### 3.12 Telephone (LXST voice)

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/telephone/status` | Status snapshot |
| GET | `/api/v1/telephone/call/{identity_hash}` | Place a call |
| GET | `/api/v1/telephone/answer` | Answer current ringing |
| GET | `/api/v1/telephone/hangup` | Hang up active call |
| GET | `/api/v1/telephone/send-to-voicemail` | Send caller to voicemail |
| GET | `/api/v1/telephone/mute-transmit` / `/unmute-transmit` | Microphone mute |
| GET | `/api/v1/telephone/mute-receive` / `/unmute-receive` | Speaker mute |
| GET | `/api/v1/telephone/switch-audio-profile/{profile_id}` | Switch profile |
| GET | `/api/v1/telephone/audio-profiles` | List profiles |
| GET | `/api/v1/telephone/history` | Call history |
| DELETE | `/api/v1/telephone/history` | Clear history |
| GET | `/api/v1/telephone/voicemail/status` | Voicemail state |
| POST | `/api/v1/telephone/voicemail/greeting/record/start` | Record greeting |
| POST | `/api/v1/telephone/voicemail/greeting/record/stop` | Stop recording |
| POST | `/api/v1/telephone/voicemail/greeting/upload` | Upload greeting (multipart) |
| DELETE | `/api/v1/telephone/voicemail/greeting` | Delete greeting |
| POST | `/api/v1/telephone/voicemail/generate-greeting` | TTS greeting |
| GET | `/api/v1/telephone/voicemail/greeting/audio` | Stream greeting bytes |
| GET | `/api/v1/telephone/voicemails` | List voicemails |
| POST | `/api/v1/telephone/voicemails/{id}/read` | Mark as read |
| DELETE | `/api/v1/telephone/voicemails/{id}` | Delete voicemail |
| GET | `/api/v1/telephone/voicemails/{id}/audio` | Stream voicemail audio |
| GET | `/api/v1/telephone/recordings` | Call recordings list |
| GET | `/api/v1/telephone/recordings/{id}/audio/{side}` | Stream `caller` or `callee` audio |
| DELETE | `/api/v1/telephone/recordings/{id}` | Delete recording |
| GET | `/api/v1/telephone/ringtones` | List ringtones |
| GET | `/api/v1/telephone/ringtones/status` | Ringtone playback state |
| GET | `/api/v1/telephone/ringtones/{id}/audio` | Stream ringtone |
| POST | `/api/v1/telephone/ringtones/upload` | Upload ringtone (multipart) |
| PATCH | `/api/v1/telephone/ringtones/{id}` | Rename / set default |
| DELETE | `/api/v1/telephone/ringtones/{id}` | Delete ringtone |
| GET | `/api/v1/telephone/contacts` | Address book |
| POST | `/api/v1/telephone/contacts` | Create contact |
| PATCH | `/api/v1/telephone/contacts/{id}` | Update contact |
| DELETE | `/api/v1/telephone/contacts/{id}` | Delete contact |
| GET | `/api/v1/telephone/contacts/check/{identity_hash}` | Check if identity already a contact |
| GET | `/api/v1/telephone/contacts/export` | Export contacts |
| POST | `/api/v1/telephone/contacts/import` | Import contacts |

### 3.13 RRC (Reticulum Rooms/Chat)

Hubs (client side):

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/rrc/hubs` | List subscribed hubs |
| POST | `/api/v1/rrc/hubs` | Subscribe |
| DELETE | `/api/v1/rrc/hubs/{hub_hash}` | Unsubscribe |
| PATCH | `/api/v1/rrc/hubs/{hub_hash}` | Rename / settings |
| PUT | `/api/v1/rrc/hubs/order` | Reorder hubs |
| PUT | `/api/v1/rrc/hubs/{hub_hash}/rooms/order` | Reorder rooms |
| POST | `/api/v1/rrc/hubs/{hub_hash}/connect` | Open link |
| POST | `/api/v1/rrc/hubs/{hub_hash}/disconnect` | Close link |
| POST | `/api/v1/rrc/hubs/{hub_hash}/rooms` | Join room |
| DELETE | `/api/v1/rrc/hubs/{hub_hash}/rooms/{room}` | Leave room |
| GET | `/api/v1/rrc/hubs/{hub_hash}/rooms/{room}/messages` | Fetch messages |
| POST | `/api/v1/rrc/hubs/{hub_hash}/rooms/{room}/messages` | Post message |
| DELETE | `/api/v1/rrc/hubs/{hub_hash}/rooms/{room}/messages` | Purge local message cache |
| POST | `/api/v1/rrc/hubs/{hub_hash}/rooms/{room}/read` | Mark room read |
| POST | `/api/v1/rrc/hubs/{hub_hash}/command` | Run hub command |

Servers (hosting a hub):

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/rrc/servers` | List local hub servers |
| POST | `/api/v1/rrc/servers` | Create server |
| DELETE | `/api/v1/rrc/servers/{hub_id}` | Delete server |
| PATCH | `/api/v1/rrc/servers/{hub_id}` | Edit server |
| POST | `/api/v1/rrc/servers/{hub_id}/start` | Start |
| POST | `/api/v1/rrc/servers/{hub_id}/stop` | Stop |
| POST | `/api/v1/rrc/servers/{hub_id}/announce` | Announce |
| POST | `/api/v1/rrc/servers/{hub_id}/rooms` | Create room |
| DELETE | `/api/v1/rrc/servers/{hub_id}/rooms/{room}` | Delete room |
| GET | `/api/v1/rrc/servers/{hub_id}/members` | List members |
| GET | `/api/v1/rrc/servers/{hub_id}/activity` | Activity log |
| GET | `/api/v1/rrc/servers/{hub_id}/messages` | Server-side message log |
| POST | `/api/v1/rrc/servers/{hub_id}/moderate` | Mute/ban/etc. |

### 3.14 RNSH (remote shell)

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/rnsh/sessions` | List sessions |
| POST | `/api/v1/rnsh/sessions` | Create session |
| DELETE | `/api/v1/rnsh/sessions/{session_id}` | Remove session |
| POST | `/api/v1/rnsh/sessions/{session_id}/start` | Connect |
| POST | `/api/v1/rnsh/sessions/{session_id}/stop` | Disconnect |
| POST | `/api/v1/rnsh/sessions/{session_id}/input` | Send input bytes |
| POST | `/api/v1/rnsh/sessions/{session_id}/resize` | Resize PTY (`cols`, `rows`) |
| GET | `/api/v1/rnsh/sessions/{session_id}/output` | Fetch buffered output |
| POST | `/api/v1/rnsh/sessions/{session_id}/clear` | Clear scrollback |

### 3.15 RNCP (file transfer)

| Method | Path | Notes |
| ------ | ---- | ----- |
| POST | `/api/v1/rncp/send` | Initiate outbound |
| POST | `/api/v1/rncp/fetch` | Initiate inbound |
| GET | `/api/v1/rncp/transfer/{transfer_id}` | Transfer state |
| POST | `/api/v1/rncp/listen` | Start listening |
| GET | `/api/v1/rncp/status` | Overall status |
| POST | `/api/v1/rncp/stop` | Stop listening |

### 3.16 Page nodes (Nomad-net style content)

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/page-nodes` | List local page nodes |
| POST | `/api/v1/page-nodes` | Create node |
| GET | `/api/v1/page-nodes/{node_id}` | Get node |
| DELETE | `/api/v1/page-nodes/{node_id}` | Delete node |
| POST | `/api/v1/page-nodes/{node_id}/start` | Start node |
| POST | `/api/v1/page-nodes/{node_id}/stop` | Stop node |
| POST | `/api/v1/page-nodes/{node_id}/announce` | Announce node |
| PUT | `/api/v1/page-nodes/{node_id}/rename` | Rename |
| GET | `/api/v1/page-nodes/{node_id}/pages` | List pages |
| POST | `/api/v1/page-nodes/{node_id}/pages` | Create / overwrite page |
| GET | `/api/v1/page-nodes/{node_id}/pages/{page_name}` | Read page |
| DELETE | `/api/v1/page-nodes/{node_id}/pages/{page_name}` | Delete page |
| GET | `/api/v1/page-nodes/{node_id}/files` | List files |
| POST | `/api/v1/page-nodes/{node_id}/files` | Upload file (multipart) |
| DELETE | `/api/v1/page-nodes/{node_id}/files/{file_name}` | Delete file |

### 3.17 Nomadnet identification & archives

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/nomadnet/archives` | List archived pages |
| DELETE | `/api/v1/nomadnet/archives` | Delete archives (filters via body) |
| POST | `/api/v1/nomadnetwork/{destination_hash}/identify` | Establish link + identify |

### 3.18 Documentation

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/docs/status` | Docs DB status |
| POST | `/api/v1/docs/upload` | Upload a docs version (multipart) |
| POST | `/api/v1/docs/switch` | Switch active version |
| DELETE | `/api/v1/docs/version/{version}` | Delete version |
| GET | `/api/v1/docs/search` | Full-text search (`q=`) |
| GET | `/api/v1/docs/export` | Export bundled docs |
| GET | `/api/v1/docs/export/reticulum` | Export Reticulum-specific docs |
| GET | `/api/v1/meshchatx-docs/list` | List local MeshChatX docs |
| GET | `/api/v1/meshchatx-docs/content` | Fetch doc content (`?path=`) |
| DELETE | `/api/v1/maintenance/docs/reticulum` | Clear Reticulum docs |
| GET | `/reticulum-docs/{filename:.*}` | Static Reticulum docs file |

### 3.19 Repository server

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/repository-server/status` | Status |
| GET | `/api/v1/repository-server/list` | Hosted artifacts |
| POST | `/api/v1/repository-server/upload` | Upload artifact (multipart) |
| DELETE | `/api/v1/repository-server/upload/{name}` | Remove artifact |
| POST | `/api/v1/repository-server/refresh-bundled` | Refresh bundled wheels |
| POST | `/api/v1/repository-server/http/start` | Start HTTP server |
| POST | `/api/v1/repository-server/http/stop` | Stop HTTP server |
| POST | `/api/v1/repository-server/http/restart` | Restart HTTP server |

### 3.20 Maintenance (bulk deletes / exports)

| Method | Path | Notes |
| ------ | ---- | ----- |
| DELETE | `/api/v1/maintenance/messages` | Purge all LXMF messages |
| DELETE | `/api/v1/maintenance/announces` | Purge all announces |
| DELETE | `/api/v1/maintenance/favourites` | Purge favourites |
| DELETE | `/api/v1/maintenance/archives` | Purge nomadnet archives |
| DELETE | `/api/v1/maintenance/lxmf-icons` | Purge cached user icons |
| DELETE | `/api/v1/maintenance/stickers` | Purge stickers |
| DELETE | `/api/v1/maintenance/gifs` | Purge GIFs |
| GET | `/api/v1/maintenance/messages/export` | Export messages |
| POST | `/api/v1/maintenance/messages/import` | Import JSON |
| POST | `/api/v1/maintenance/messages/import-file` | Import from file (multipart) |

### 3.21 Translator

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/translator/languages` | List installed/available |
| POST | `/api/v1/translator/translate` | Translate text |
| POST | `/api/v1/translator/install-languages` | Install models |

### 3.22 Bots

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/bots/status` | Status / installed bots |
| POST | `/api/v1/bots/start` | Start bot |
| POST | `/api/v1/bots/stop` | Stop bot |
| POST | `/api/v1/bots/restart` | Restart bot |
| POST | `/api/v1/bots/delete` | Remove bot |
| GET | `/api/v1/bots/subprocess-log` | Tail subprocess log |
| PATCH | `/api/v1/bots/update` | Update bot config |
| POST | `/api/v1/bots/announce` | Force announce bot identity |
| GET | `/api/v1/bots/export` | Export bots |

### 3.23 Blocking / spam keywords

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/blocked-destinations` | List blocked destinations |
| POST | `/api/v1/blocked-destinations` | Add block |
| DELETE | `/api/v1/blocked-destinations/{destination_hash}` | Unblock |
| GET | `/api/v1/spam-keywords` | List keywords |
| POST | `/api/v1/spam-keywords` | Add keyword |
| DELETE | `/api/v1/spam-keywords/{keyword_id}` | Remove |

### 3.24 Stickers / sticker packs / GIFs

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/stickers` | List |
| POST | `/api/v1/stickers` | Create |
| PATCH | `/api/v1/stickers/{sticker_id}` | Update |
| DELETE | `/api/v1/stickers/{sticker_id}` | Delete |
| GET | `/api/v1/stickers/{sticker_id}/image` | Image bytes |
| GET | `/api/v1/stickers/export` | Export all |
| POST | `/api/v1/stickers/import` | Import (multipart) |
| GET | `/api/v1/sticker-packs` | List |
| POST | `/api/v1/sticker-packs` | Create |
| GET | `/api/v1/sticker-packs/{pack_id}` | Detail |
| PATCH | `/api/v1/sticker-packs/{pack_id}` | Edit |
| DELETE | `/api/v1/sticker-packs/{pack_id}` | Delete |
| POST | `/api/v1/sticker-packs/reorder` | Reorder |
| GET | `/api/v1/sticker-packs/{pack_id}/export` | Export pack |
| POST | `/api/v1/sticker-packs/install` | Install pack |
| GET | `/api/v1/gifs` | List |
| POST | `/api/v1/gifs` | Create |
| PATCH | `/api/v1/gifs/{gif_id}` | Edit |
| DELETE | `/api/v1/gifs/{gif_id}` | Delete |
| GET | `/api/v1/gifs/{gif_id}/image` | Image bytes |
| POST | `/api/v1/gifs/{gif_id}/use` | Bump use counter |
| GET | `/api/v1/gifs/export` | Export |
| POST | `/api/v1/gifs/import` | Import |

### 3.25 Map / tiles / drawings / exports

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/map/offline` | Offline status |
| POST | `/api/v1/map/offline` | Toggle / configure |
| GET | `/api/v1/map/tiles/{z}/{x}/{y}` | Stream a single tile |
| GET | `/api/v1/map/mbtiles` | List installed MBTiles |
| DELETE | `/api/v1/map/mbtiles/{filename}` | Delete |
| POST | `/api/v1/map/mbtiles/active` | Activate one |
| GET | `/api/v1/map/drawings` | Drawings list |
| POST | `/api/v1/map/drawings` | Create drawing |
| PATCH | `/api/v1/map/drawings/{drawing_id}` | Update drawing |
| DELETE | `/api/v1/map/drawings/{drawing_id}` | Delete drawing |
| POST | `/api/v1/map/export` | Start tile export job |
| GET | `/api/v1/map/export/{export_id}` | Export status |
| GET | `/api/v1/map/export/{export_id}/download` | Download MBTiles |
| DELETE | `/api/v1/map/export/{export_id}` | Cancel/cleanup |

### 3.26 Telemetry / tracking

| Method | Path | Notes |
| ------ | ---- | ----- |
| GET | `/api/v1/telemetry/peers` | All telemetry-enabled peers |
| GET | `/api/v1/telemetry/trusted-peers` | Trusted subset |
| POST | `/api/v1/telemetry/tracking/{destination_hash}/toggle` | Toggle tracking |
| GET | `/api/v1/telemetry/tracking` | All tracked destinations |
| GET | `/api/v1/telemetry/history/{destination_hash}` | Historical fixes |
| GET | `/api/v1/telemetry/latest/{destination_hash}` | Latest fix |

### 3.27 Static assets

The frontend bundle (`meshchatx/public/`) is served from `/` (catch-all
`add_static`). Vendor sub-paths (`/vendor/...`, `/icons/...`, etc.) are
delivered through the same static route.

---

## 4. Common conventions

- **Pagination**: most list endpoints accept `limit` and `offset` query params.
- **Search**: list endpoints generally accept `search` (alias `q`) and feature-
  specific filter flags (`unread`, `failed`, etc.) parsed with
  `parse_bool_query_param`.
- **Destination hashes** are 32-character hex strings (16 bytes).
- **Identity hashes** are 32-character hex strings (16 bytes).
- **Binary payloads** (images, audio, attachments, identity backups) are
  exchanged base64-encoded inside JSON or as `multipart/form-data`. Endpoints
  whose path ends in `/audio`, `/image`, `/download`, `/tiles/...` stream raw
  bytes.
- **WS broadcast on mutation**: many REST endpoints emit a WebSocket event
  (`config`, `announce`, `lxmf_message_*`, `rrc.change`, etc.) so connected
  clients refresh without polling.
- **Error responses**: `{"message": "..."}` for handler-emitted errors;
  `{"error": "..."}` for auth flows. Status codes follow standard HTTP.

---

## 5. Quick examples

### 5.1 Login

```bash
curl -c cookies.txt -X POST http://127.0.0.1:8000/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"password":"hunter2"}'
```

### 5.2 Send an LXMF message

```bash
curl -b cookies.txt -X POST http://127.0.0.1:8000/api/v1/lxmf-messages/send \
  -H 'Content-Type: application/json' \
  -d '{
    "lxmf_message": {
      "destination_hash": "0123456789abcdef0123456789abcdef",
      "content": "Hello mesh!"
    }
  }'
```

### 5.3 Listen for announces over WebSocket

```bash
websocat ws://127.0.0.1:8000/ws
# server immediately sends {"type":"config",...}
# any announce produces {"type":"announce", ...}
```

### 5.4 Resize an RNSH PTY

```bash
curl -b cookies.txt -X POST \
  http://127.0.0.1:8000/api/v1/rnsh/sessions/abc123/resize \
  -H 'Content-Type: application/json' \
  -d '{"cols":120,"rows":40}'
```
