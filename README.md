# mqtt-pizza

A three-app pizza ordering demo built on MQTT. A customer places an order, a kitchen
cooks it, and a dashboard watches everything live. Each app is a tiny HTTP server
with a browser UI, and all state moves between them over MQTT topics.

One binary, four roles:

| Role | Port | What it does |
|------|------|--------------|
| `broker` | 1883 | An MQTT broker running in-process, so nothing has to be installed |
| `customer` | 3000 | Places orders, shows their live status |
| `kitchen` | 3001 | Receives orders and advances them through the pipeline |
| `dashboard` | 3002 | Shows every order plus which clients are online |

## Requirements

Rust 1.88+ (the crate uses edition 2024 and let chains). Nothing else. Install Rust
from [rustup.rs](https://rustup.rs); on Windows, rustup's default MSVC toolchain also
needs the Visual Studio Build Tools with the "Desktop development with C++" workload,
and prompts for it on first run.

## Running

Four terminals, same on Linux, macOS and Windows:

```bash
cargo run -- broker       # mqtt://0.0.0.0:1883
cargo run -- kitchen      # http://localhost:3001
cargo run -- customer     # http://localhost:3000
cargo run -- dashboard    # http://localhost:3002
```

Open the customer page, order a pizza, and watch the status move from `ordered` to
`baking` to `ready` on all three pages. Kill the kitchen process and the dashboard
flips it to `offline` a few seconds later.

The `broker` role embeds [rumqttd](https://crates.io/crates/rumqttd), so it is a
regular Cargo dependency rather than a separate program to install. It keeps
everything in memory and starts with no config file.

## Using an external broker instead

Any MQTT 3.1.1 broker works in place of the `broker` role. With Docker:

```bash
docker run -d --name pizza-broker -p 1883:1883 eclipse-mosquitto:2 \
  sh -c 'printf "listener 1883\nallow_anonymous true\n" > /mosquitto/config/mosquitto.conf && mosquitto -c /mosquitto/config/mosquitto.conf'
```

Or install Mosquitto natively. On Arch:

```bash
sudo pacman -S mosquitto
sudo systemctl enable --now mosquitto
```

On Ubuntu:

```bash
sudo apt install mosquitto mosquitto-clients
sudo systemctl enable --now mosquitto
```

On Windows, install Mosquitto with winget (or Chocolatey, or the installer from
mosquitto.org):

```powershell
winget install -e --id EclipseFoundation.Mosquitto
# choco install mosquitto
```

The installer registers a Windows service that starts automatically. Manage it from an
elevated PowerShell:

```powershell
Restart-Service mosquitto
Get-Service mosquitto
```

To run it in the foreground instead, with verbose logging:

```powershell
& "C:\Program Files\mosquitto\mosquitto.exe" -c "C:\Program Files\mosquitto\mosquitto.conf" -v
```

A default Mosquitto install listens on localhost only and rejects anonymous clients.
For this demo, add these two lines to the config and restart the service:

```
listener 1883
allow_anonymous true
```

The config file is `/etc/mosquitto/mosquitto.conf` on Linux (a file in `conf.d/` works
too) and `C:\Program Files\mosquitto\mosquitto.conf` on Windows. Editing the Windows
one needs an elevated editor.

If you want other machines to reach the broker or the web pages on Windows, allow them
through the firewall once, from an elevated PowerShell:

```powershell
New-NetFirewallRule -DisplayName "MQTT 1883" -Direction Inbound -Protocol TCP -LocalPort 1883 -Action Allow
New-NetFirewallRule -DisplayName "pizza apps" -Direction Inbound -Protocol TCP -LocalPort 3000-3002 -Action Allow
```

Then start the three apps as above, leaving out the `broker` role.

## Configuration

| Variable | Default | Meaning |
|----------|---------|---------|
| `MQTT_BROKER` | `localhost` | Broker hostname |
| `MQTT_PORT` | `1883` | Broker port |

Set them per shell before running. Bash:

```bash
MQTT_BROKER=192.168.1.50 cargo run -- dashboard
```

PowerShell:

```powershell
$env:MQTT_BROKER = "192.168.1.50"
cargo run -- dashboard
```

`MQTT_PORT` also decides which port the `broker` role listens on. The HTTP ports are
fixed per role and bind to `0.0.0.0`, so the pages are reachable from other machines
on the network.

## How it works

### Topics

| Topic | Published by | QoS | Retained | Payload |
|-------|--------------|-----|----------|---------|
| `pizza/order/new` | customer | 1 | no | `Order` |
| `pizza/order/status/{order_id}` | kitchen | 1 | yes | `Order` |
| `pizza/system/client-status/{name}` | every app | 1 | yes | `ClientStatus` |

Status lives on a per-order topic so that retained messages do not overwrite each
other. A client that subscribes to `pizza/order/status/+` gets the current state of
every order it missed, not just the most recent one.

Presence uses a retained message per client plus a matching MQTT last will. An app
publishes `online` after connecting, and the broker publishes `offline` on its behalf
if the connection drops without a clean disconnect, within the keep-alive window (5s).

### Payloads

```jsonc
// Order
{
  "timestamp": "2026-09-16T06:37:18.052930907+00:00",  // RFC 3339, last state change
  "order_id": 137196,
  "pizza": "Margherita" | "Salami" | "Funghi",
  "size": "Small" | "Medium" | "Large",
  "status": "ordered" | "baking" | "ready"
}

// ClientStatus
{ "client": "kitchen", "status": "online" | "offline" }
```

### Order lifecycle

1. The customer publishes an `Order` with status `ordered` to `pizza/order/new`.
2. The kitchen picks it up, echoes it to the order's status topic, and starts a timer
   thread. Orders it has already seen are ignored, since QoS 1 permits redelivery.
3. After 4 seconds the status becomes `baking`, after another 7 seconds `ready`. Both
   transitions are published retained.
4. Customer and dashboard update from their subscriptions. Timings live in the
   `PIPELINE` constant in `src/main.rs`.

### Web UI

Each page renders a shell once and then polls `/api` every 1.5 seconds for JSON, so
the tables update without a page reload. The customer form posts to `/`, which
validates the fields against a whitelist and answers with a 303 redirect.

## Project layout

Everything lives in `src/main.rs`, grouped into sections: MQTT plumbing, the HTTP
layer, shared markup, and one function per role (`customer`, `kitchen`, `dashboard`,
`embedded_broker`).

## Development

```bash
cargo clippy --all-targets
cargo fmt
```

## Caveats

This is a demo, not a production service. There is no authentication, no TLS, no
persistence (orders live in memory and vanish on restart), and the kitchen accepts
every order that arrives. The embedded broker is unauthenticated and listens on all
interfaces, which is fine on a laptop and not fine anywhere else.
