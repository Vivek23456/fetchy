use anyhow::Result;
use axum::{
    extract::{Path,State},
    http::StatusCode,
    routing::get,
    Json,Router,

};

use futures_util::{SinkExt,StreamExt};
use serde::{Serialize, Deserialize};
use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

use tokio::{
    net::TcpListener,
    sync::RwLock,
    time::sleep,

};

use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,

};

//shared application state
#[derive(Clone)]

struct Appstate {
    prices: Arc<RwLock<HashMap<String,PriceSnapshot>>>,

}
//normalized price structure 

#[derive(Debug,Clone,Serialize)]

struct PriceSnapshot {
    exchange: String,
    symbol:String,
    price:String,
    quantity:String, 
    event_time_ms:u64,

}
//Binance trade
#[derive(Debug,Deserialize)]

struct BinanceTrade {
    s:String,
    p:String,
    q:String,
    #[serde(rename = "T")]
    event_time:u64,
}

//Binance Websocket Connector

async fn run_binance(state:Appstate){
    let url = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade";   
    loop{
        println!("Connecting to binance....");

        match connect_async(url).await {
            Ok((mut ws, _response)) => {
                println!("connected to binance!");
                while let Some(message)= ws.next().await{
                    match message{
                        Ok(Message::Text(text)) => {
                            let trade:BinanceTrade = 
                            match serde_json::from_str(text.as_str()){
                                Ok(data)=>data,
                                Err(error)=>{
                                    eprintln!(" JSON error:{error}");
                                    continue;

                                }
                            };

                            let snapshot = PriceSnapshot {
                                exchange: "binance".to_string(),
                                symbol: trade.s.clone(),
                                price: trade.p,
                                quantity: trade.q,
                                event_time_ms: trade.event_time,
                            };

                            let mut prices = state.prices.write().await;
                            prices.insert(trade.s, snapshot);
                        }

                        Ok(Message::Ping(payload)) => {
                            if let Err(error) =
                                ws.send(Message::Pong(payload)).await
                            {
                                eprintln!("Pong error: {error}");
                                break;
                            }
                        }

                        Ok(Message::Close(_)) => {
                            println!("Binance closed connection");
                            break;
                        }

                        Err(error) => {
                            eprintln!("WebSocket error: {error}");
                            break;
                        }

                        _ => {}
                    }
                }
            }

            Err(error) => {
                eprintln!("Connection failed: {error}");
            }
        }

        println!("Reconnecting in 5 seconds...");
        sleep(Duration::from_secs(5)).await;
    }
}

                            
// HTTP endpoint :health check 

async fn health() -> &'static str {
    "Market data api is running"
}

//http endpoint for latest price

async fn get_price(
    State(state): State<Appstate>,
    Path(symbol): Path<String>,

) -> Result<Json<PriceSnapshot>,StatusCode>{
    let prices = state.prices.read().await;

    prices
    
    .get(&symbol.to_uppercase())
    .cloned()
    .map(Json)
    .ok_or(StatusCode::NOT_FOUND)
}

#[tokio::main]
async fn main() -> Result<()> {

     
    let state = Appstate {
        prices: Arc::new(RwLock::new(HashMap::new())),
    };

    // Start Binance ingestion in the background
    let binance_state = state.clone();

    tokio::spawn(async move {
        run_binance(binance_state).await;
    });

    // Build HTTP API
    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/prices/{symbol}", get(get_price))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:3000").await?;

    println!("API running at http://127.0.0.1:3000");

    axum::serve(listener, app).await?;

    Ok(())
}