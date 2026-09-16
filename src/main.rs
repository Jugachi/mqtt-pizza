use anyhow::Result;
use chrono::Utc;
use rumqttc::{Client, Event, LastWill, MqttOptions, Packet, QoS};
use serde::{Deserialize, Serialize};
use std::{
    env,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use tiny_http::{Header, Method, Response, Server};

const BROKER: &str = "localhost";
const NEW: &str = "pizza/order/new";
const STATUS: &str = "pizza/order/status";
const CLIENTS: &str = "pizza/system/client-status";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Order {
    timestamp: String,
    order_id: u32,
    pizza: String,
    size: String,
    status: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ClientStatus {
    client: String,
    status: String,
}

type Orders = Arc<Mutex<Vec<Order>>>;

type ClientHandle = Arc<Mutex<Client>>;

fn now() -> String {
    Utc::now().to_rfc3339()
}

fn connect(name: &str) -> (Client, rumqttc::Connection) {
    let mut options = MqttOptions::new(name, BROKER, 1883);
    options.set_keep_alive(Duration::from_secs(5));
    options.set_last_will(LastWill::new(
        CLIENTS,
        format!(r#"{{"client":"{}","status":"offline"}}"#, name),
        QoS::AtLeastOnce,
        true,
    ));
    Client::new(options, 20)
}

fn publish_json<T: Serialize>(
    client: &mut Client,
    topic: &str,
    value: &T,
    qos: QoS,
    retain: bool,
) -> Result<()> {
    client.publish(topic, qos, retain, serde_json::to_vec(value)?)?;
    Ok(())
}

fn online(client: &mut Client, name: &str) -> Result<()> {
    publish_json(
        client,
        CLIENTS,
        &ClientStatus {
            client: name.into(),
            status: "online".into(),
        },
        QoS::AtLeastOnce,
        true,
    )
}

fn html(title: &str, body: &str, script: &str) -> String {
    format!(
        r#"<!doctype html><html><head><meta name="viewport" content="width=device-width"><title>{}</title><style>body{{font:16px system-ui;background:#111827;color:#f9fafb;max-width:850px;margin:30px auto;padding:0 18px}}.card{{background:#1f2937;border-radius:14px;padding:20px;margin:14px 0}}button,select,input{{padding:10px;margin:5px;border:0;border-radius:8px}}button{{background:#f97316;color:white;cursor:pointer}}table{{width:100%;text-align:left}}.pill{{background:#374151;padding:5px 9px;border-radius:12px}}</style></head><body><h1>🍕 {}</h1>{}<script>{}</script></body></html>"#,
        title, title, body, script
    )
}

fn serve<F, G>(server: Server, render: F, post: G)
where
    F: Fn(String) -> String + Send + 'static,
    G: Fn(String) -> String + Send + 'static,
{
    for mut request in server.incoming_requests() {
        let path = request.url().to_string();
        let body = if request.method() == &Method::Post {
            let mut input = String::new();
            let _ = request.as_reader().read_to_string(&mut input);
            post(input)
        } else {
            render(path)
        };
        let header = Header::from_bytes("Content-Type", "text/html; charset=utf-8").unwrap();
        let _ = request.respond(Response::from_string(body).with_header(header));
    }
}

fn customer() -> Result<()> {
    let (client, mut connection) = connect("customer");
    let client: ClientHandle = Arc::new(Mutex::new(client));
    online(&mut client.lock().unwrap(), "customer")?;
    let orders: Orders = Arc::new(Mutex::new(Vec::new()));
    let received = orders.clone();
    thread::spawn(move || {
        for event in connection.iter() {
            if let Ok(Event::Incoming(Packet::Publish(packet))) = event {
                if packet.topic == STATUS {
                    if let Ok(order) = serde_json::from_slice::<Order>(&packet.payload) {
                        let mut list = received.lock().unwrap();
                        list.retain(|x| x.order_id != order.order_id);
                        list.push(order);
                    }
                }
            }
        }
    });
    let server = Server::http("0.0.0.0:3000").unwrap();
    let view_orders = orders.clone();
    let render = move |_| {
        let json = serde_json::to_string(&*view_orders.lock().unwrap()).unwrap();
        html(
            "Kunden-App",
            r#"<div class="card"><h2>Pizza bestellen</h2><form method="post"><select name="pizza"><option>Margherita</option><option>Salami</option><option>Funghi</option></select><select name="size"><option>Small</option><option>Medium</option><option>Large</option></select><button>Bestellen</button></form></div><div class="card"><h2>Bestellungen</h2><div id="orders"></div></div>"#,
            &format!(
                "let data={};document.getElementById('orders').innerHTML=data.map(x=>'<p>#'+x.order_id+' '+x.pizza+' – <b>'+x.status+'</b></p>').join('')||'Noch keine Bestellung';setTimeout(()=>location.reload(),3000);",
                json
            ),
        )
    };
    let sender = client.clone();
    serve(server, render, move |form| {
        let pizza = if form.contains("Salami") {
            "Salami"
        } else if form.contains("Funghi") {
            "Funghi"
        } else {
            "Margherita"
        };
        let size = if form.contains("Large") {
            "Large"
        } else if form.contains("Medium") {
            "Medium"
        } else {
            "Small"
        };
        let order = Order {
            timestamp: now(),
            order_id: (Utc::now().timestamp_millis() as u32) % 100000,
            pizza: pizza.into(),
            size: size.into(),
            status: "ordered".into(),
        };
        publish_json(
            &mut sender.lock().unwrap(),
            NEW,
            &order,
            QoS::AtLeastOnce,
            false,
        )
        .unwrap();
        "Bestellung gesendet! <a href='/'>Zurück</a>".into()
    });
    Ok(())
}

fn kitchen() -> Result<()> {
    let (client, mut connection) = connect("kitchen");
    let client: ClientHandle = Arc::new(Mutex::new(client));
    online(&mut client.lock().unwrap(), "kitchen")?;
    client.lock().unwrap().subscribe(NEW, QoS::AtLeastOnce)?;
    let orders: Orders = Arc::new(Mutex::new(Vec::new()));
    let received = orders.clone();
    let sender = client.clone();
    thread::spawn(move || {
        for event in connection.iter() {
            if let Ok(Event::Incoming(Packet::Publish(packet))) = event {
                if packet.topic == NEW {
                    if let Ok(order) = serde_json::from_slice::<Order>(&packet.payload) {
                        received.lock().unwrap().push(order.clone());
                        let _ = publish_json(
                            &mut sender.lock().unwrap(),
                            STATUS,
                            &order,
                            QoS::AtLeastOnce,
                            true,
                        );
                    }
                }
            }
        }
    });
    let server = Server::http("0.0.0.0:3001").unwrap();
    let view = orders.clone();
    let render = move |_| {
        let rows = view.lock().unwrap().iter().map(|o| format!("<tr><td>#{}</td><td>{}</td><td>{}</td><td><span class='pill'>{}</span></td></tr>", o.order_id,o.pizza,o.size,o.status)).collect::<String>();
        html(
            "Küchen-App",
            &format!(
                "<div class='card'><h2>Eingegangene Bestellungen</h2><table><tr><th>Nr.</th><th>Pizza</th><th>Größe</th><th>Status</th></tr>{}</table></div>",
                rows
            ),
            "setTimeout(()=>location.reload(),3000);",
        )
    };
    serve(server, render, |_| "<a href='/'>Zurück</a>".into());
    Ok(())
}

fn dashboard() -> Result<()> {
    let (mut client, mut connection) = connect("dashboard");
    online(&mut client, "dashboard")?;
    client.subscribe(STATUS, QoS::AtLeastOnce)?;
    client.subscribe(CLIENTS, QoS::AtLeastOnce)?;
    let orders: Orders = Arc::new(Mutex::new(Vec::new()));
    let received = orders.clone();
    thread::spawn(move || {
        for event in connection.iter() {
            if let Ok(Event::Incoming(Packet::Publish(packet))) = event {
                if packet.topic == STATUS {
                    if let Ok(order) = serde_json::from_slice::<Order>(&packet.payload) {
                        let mut list = received.lock().unwrap();
                        list.retain(|x| x.order_id != order.order_id);
                        list.push(order);
                    }
                }
            }
        }
    });
    let server = Server::http("0.0.0.0:3002").unwrap();
    let view = orders.clone();
    serve(
        server,
        move |_| {
            let rows = view
                .lock()
                .unwrap()
                .iter()
                .map(|o| {
                    format!(
                        "<tr><td>#{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                        o.order_id, o.pizza, o.size, o.status
                    )
                })
                .collect::<String>();
            html(
                "Live-Dashboard",
                &format!(
                    "<div class='card'><table><tr><th>Nr.</th><th>Pizza</th><th>Größe</th><th>Status</th></tr>{}</table></div>",
                    rows
                ),
                "setTimeout(()=>location.reload(),2000);",
            )
        },
        |_| "".into(),
    );
    Ok(())
}

fn main() -> Result<()> {
    match env::args().nth(1).as_deref() {
        Some("customer") => customer(),
        Some("kitchen") => kitchen(),
        Some("dashboard") => dashboard(),
        _ => {
            eprintln!("cargo run -- customer|kitchen|dashboard");
            Ok(())
        }
    }
}
