use super::*;
use axum_server::Handle;
use http::status;
use crate::subcommand::server;
use crate::index::fetcher;
use crate::subcommand::wallet::inscriptions;

use mysql::Pool;
use mysql::prelude::Queryable;
use mysql::params;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::Row;
use tokio::sync::Semaphore;
use tokio::sync::Mutex;
use serde::Serialize;
use sha256::digest;

use axum::{
  routing::get,
  http::StatusCode,
  response::IntoResponse,
  Json, 
  Router,
  extract::{Extension, Path, Query, State},
};
use std::net::SocketAddr;

#[derive(Debug, Parser, Clone)]
pub(crate) struct Vermilion {
  #[clap(
    long,
    default_value = "0.0.0.0",
    help = "Listen on <ADDRESS> for incoming requests."
  )]
  address: String,
  #[clap(
    long,
    help = "Request ACME TLS certificate for <ACME_DOMAIN>. This ord instance must be reachable at <ACME_DOMAIN>:443 to respond to Let's Encrypt ACME challenges."
  )]
  acme_domain: Vec<String>,
  #[clap(
    long,
    help = "Listen on <HTTP_PORT> for incoming HTTP requests. [default: 80]."
  )]
  http_port: Option<u16>,
  #[clap(
    long,
    group = "port",
    help = "Listen on <HTTPS_PORT> for incoming HTTPS requests. [default: 443]."
  )]
  https_port: Option<u16>,
  #[clap(long, help = "Store ACME TLS certificates in <ACME_CACHE>.")]
  acme_cache: Option<PathBuf>,
  #[clap(long, help = "Provide ACME contact <ACME_CONTACT>.")]
  acme_contact: Vec<String>,
  #[clap(long, help = "Serve HTTP traffic on <HTTP_PORT>.")]
  http: bool,
  #[clap(long, help = "Serve HTTPS traffic on <HTTPS_PORT>.")]
  https: bool,
  #[clap(long, help = "Redirect HTTP traffic to HTTPS.")]
  redirect_http_to_https: bool,
  #[clap(
    long,
    help = "Listen on <HTTP_PORT> for incoming REST requests. [default: 81]."
  )]
  api_http_port: Option<u16>,
  #[clap(
    long,
    help = "Number of threads to use when uploading content and metadata. [default: 10]."
  )]
  n_threads: Option<u16>,
}

#[derive(Clone, Serialize)]
pub struct Metadata {
  id: String,
  address: Option<String>,
  content_length: Option<i64>,
  content_type: Option<String>,
  genesis_fee: i64,
  genesis_height: i64,
  genesis_transaction: String,
  location: String,
  number: i64,
  offset: i64,
  output_transaction: String,
  sat: Option<i64>,
  timestamp: i64,
  sha256: Option<String>
}

#[derive(Clone, Serialize)]
pub struct Content {
  content: Vec<u8>,
  content_type: Option<String>
}

pub struct InscriptionNumberStatus {
  inscription_number: i64,
  status: String
}

#[derive(Clone)]
pub struct ApiServerConfig {
  pool: sqlx::Pool<sqlx::MySql>
}

impl Vermilion {
  pub(crate) fn run(self, options: Options, index: Arc<Index>, handle: Handle) -> Result {
    println!("Vermilion Indexer Running");
    //1. Normal Server
    let server = server::Server {
      address: self.address,
      acme_domain: self.acme_domain,
      http_port: self.http_port,
      https_port: self.https_port,
      acme_cache: self.acme_cache,
      acme_contact: self.acme_contact,
      http: self.http,
      https: self.https,
      redirect_http_to_https: self.redirect_http_to_https,
    };
    let server_index_clone = index.clone();
    let server_options_clone = options.clone();
    let server_thread = thread::spawn(move || {
      let server_result = server.run(server_options_clone, server_index_clone, handle);
      match server_result {
        Ok(_) => {
          println!("Default server stopped");
        },
        Err(err) => {
          println!("Default server failed to start: {:?}", err);
        }
      }
    });

    //2. Vermilion Indexer
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {      
      println!("Vermilion Indexer Running");
      let clone = index.clone();
      let config = options.load_config().unwrap();
      let url = config.db_connection_string.unwrap();
      let pool = Pool::new(url.as_str())?;
      let n_threads = self.n_threads.unwrap_or(10).into();
      let sem = Arc::new(Semaphore::new(n_threads));
      let status_vector: Arc<Mutex<Vec<InscriptionNumberStatus>>> = Arc::new(Mutex::new(Vec::new()));
      Self::create_metadata_table(&pool).unwrap();
      let start_number = Self::get_start_number(&pool).unwrap();
      let initial = InscriptionNumberStatus {
        inscription_number: start_number,
        status: "UNKNOWN".to_string()
      };
      status_vector.lock().await.push(initial);

      // every iteration fetches 1k inscriptions
      let time = Instant::now();
      print!("Starting @ {:?}", time);
      loop {
        //break if ctrl-c is received
        if SHUTTING_DOWN.load(atomic::Ordering::Relaxed) {
          break;
        }
        let permit = Arc::clone(&sem).acquire_owned().await;
        let cloned_index = clone.clone();
        let cloned_pool = pool.clone();
        let cloned_status_vector = status_vector.clone();
        let cloned_status_vector2 = status_vector.clone();
        let cloned_options = options.clone();
        let fetcher = fetcher::Fetcher::new(&options)?;//Need a new fetcher for each thread
        tokio::task::spawn(async move {
          let _permit = permit;
          let needed_numbers = Self::get_needed_inscription_numbers(cloned_status_vector).await;
          let needed_clone = needed_numbers.clone();
          let needed_clone2 = needed_numbers.clone();
          let mut should_sleep = false;
          println!("Trying Numbers: {:?}-{:?}", &needed_numbers[0], &needed_numbers[&needed_numbers.len()-1]);
          //1. Get ids
          let mut inscription_ids: Vec<InscriptionId> = Vec::new();          
          for j in needed_numbers {
            let inscription_id = cloned_index.get_inscription_id_by_inscription_number(j).unwrap();
            match inscription_id {
              Some(inscription_id) => {
                inscription_ids.push(inscription_id);
              },
              None => {
                println!("No inscription found for inscription number: {}. Marking as not found. Breaking from loop", j);
                let mut status_vector = cloned_status_vector2.lock().await;
                for l in needed_clone2 {
                  let status = status_vector.iter_mut().find(|x| x.inscription_number == l).unwrap();
                  if l >= j {
                    status.status = "NOT_FOUND".to_string();
                  }                
                }
                should_sleep = true;
                break;
              }
            }
          }
          
          //2. Get inscriptions
          let cloned_ids = inscription_ids.clone();
          let cloned_ids2 = inscription_ids.clone();
          let txs = fetcher.get_transactions(inscription_ids.into_iter().map(|x| x.txid).collect()).await;
          let err_txs = match txs {
              Ok(txs) => Some(txs),
              Err(error) => {
                println!("Error getting transactions {}-{}: {:?}", needed_clone[0], needed_clone[needed_clone.len()-1], error);
                let mut status_vector = cloned_status_vector2.lock().await;
                for j in needed_clone {
                  let status = status_vector.iter_mut().find(|x| x.inscription_number == j).unwrap();
                  status.status = "ERROR".to_string();
                }
                println!("error string: {}", error.to_string());
                if  error.to_string().contains("Failed to fetch raw transaction") || 
                    error.to_string().contains("connection closed") || 
                    error.to_string().contains("error trying to connect") || 
                    error.to_string().contains("end of file") {
                  println!("Pausing for 60s & Breaking from loop");
                  std::mem::drop(status_vector); //Drop the mutex so sleep doesn't block
                  tokio::time::sleep(Duration::from_secs(60)).await;
                }
                return;
                None
              }
          };
          let clean_txs = err_txs.unwrap();
          let id_txs: Vec<_> = cloned_ids.into_iter().zip(clean_txs.into_iter()).collect();
          let mut inscriptions: Vec<Inscription> = Vec::new();
          for (inscription_id,tx) in id_txs {
            let inscription = Inscription::from_transaction(&tx)
              .get(inscription_id.index as usize)
              .map(|transaction_inscription| transaction_inscription.inscription.clone())
              .unwrap();
            inscriptions.push(inscription);
          }
          
          //3. Get ordinal metadata
          let id_inscriptions: Vec<_> = cloned_ids2.into_iter().zip(inscriptions.into_iter()).collect();
          let mut status_vector = cloned_status_vector2.lock().await;
          for (inscription_id, inscription) in id_inscriptions.clone() {
            let metadata: Metadata = Self::get_ordinal_metadata(cloned_index.clone(), &cloned_options, inscription_id, inscription.clone()).unwrap();
            let result = Self::insert_metadata(&cloned_pool.clone(), metadata.clone(), inscription);
            let status = status_vector.iter_mut().find(|x| x.inscription_number == metadata.number).unwrap();
            if result.is_err() {
              println!("Error inserting metadata for inscription number: {}. Marking as error", metadata.number);
              status.status = "ERROR".to_string();
            } else {
              status.status = "SUCCESS".to_string();
            }
          }
          let time = Instant::now();
          println!("Finished numbers {} - {} @ {:?}", needed_clone[0], needed_clone[needed_clone.len()-1], time);

          //4. Sleep thread if up to date.
          if should_sleep {
            println!("Sleeping for 60s");
            tokio::time::sleep(Duration::from_secs(60)).await;
          }
        });
        
      }
      Ok(())
    })
  }

  pub(crate) fn run_vermilion_server(self, options: Options) {
    println!("Vermilion Server Running");
    let api_server_options_clone = options.clone();
    let verm_server_thread = thread::spawn(move ||{
      let mut rt = Runtime::new().unwrap();
      rt.block_on(async move {
        let config = api_server_options_clone.load_config().unwrap();
        let url = config.db_connection_string.unwrap();
        let pool = MySqlPoolOptions::new()
          .max_connections(5)
          .connect(url.as_str()).await.unwrap();
        let server_config = ApiServerConfig {
          pool: pool
        };

        let app = Router::new()
          .route("/", get(Self::root))
          .route("/home", get(Self::home))
          .route("/inscription/:inscription_id", get(Self::inscription))
          .route("/inscription_number/:number", get(Self::inscription_number))
          .route("/inscription_metadata/:inscription_id", get(Self::inscription_metadata))
          .route("/inscription_number_metadata/:number", get(Self::inscription_number_metadata))   
          .with_state(server_config);

        let addr = SocketAddr::from(([127, 0, 0, 1], self.api_http_port.unwrap_or(81)));
        //tracing::debug!("listening on {}", addr);
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
      });
    });
  }

  //Helper functions
  pub(crate) fn get_ordinal_metadata(index: Arc<Index>, options: &Options, inscription_id: InscriptionId, inscription: Inscription) -> Result<Metadata> {
    let entry = index
      .get_inscription_entry(inscription_id)
      .unwrap()
      .unwrap();
    let satpoint = index
      .get_inscription_satpoint_by_id(inscription_id)
      .unwrap()
      .unwrap();
    let output = if satpoint.outpoint == unbound_outpoint() {
      None
    } else {
      index
        .get_transaction(satpoint.outpoint.txid)
        .unwrap()
        .unwrap()
        .output
        .into_iter()
        .nth(satpoint.outpoint.vout.try_into().unwrap())
    };
    let address = match options.chain().address_from_script(&output.unwrap().script_pubkey) {
      Ok(address) => Some(address.to_string()),
      Err(_) => None,
    };
    let content_length = match inscription.content_length() {
      Some(content_length) => Some(content_length as i64),
      None => {
        println!("No content length found for inscription: {}, filling with 0", inscription_id);
        Some(0)
      }
    };
    let sat = match entry.sat {
      Some(sat) => Some(sat.n() as i64),
      None => {
        None
      }
    };
    let sha256 = match inscription.body() {
      Some(body) => {
        let hash = digest(body);
        Some(hash)
      },
      None => {
        None
      }
    };
    let metadata = Metadata {
      id: inscription_id.to_string(),
      address: address,
      content_length: content_length,
      content_type: inscription.content_type().map(str::to_string),
      genesis_fee: entry.fee.try_into().unwrap(),
      genesis_height: entry.height.try_into().unwrap(),
      genesis_transaction: inscription_id.txid.to_string(),
      location: satpoint.to_string(),
      number: entry.number,
      offset: satpoint.offset.try_into().unwrap(),
      output_transaction: satpoint.outpoint.to_string(),
      sat: sat,
      timestamp: entry.timestamp.try_into().unwrap(),
      sha256: sha256
    };
    Ok(metadata)
  }

  fn create_metadata_table(pool: &mysql::Pool) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = pool.get_conn()?;
    conn.query_drop(
      r"CREATE TABLE IF NOT EXISTS ordinals (
          id varchar(80) not null primary key,
          address text,
          content mediumblob,
          content_length bigint,
          content_type text,
          genesis_fee bigint,
          genesis_height bigint,
          genesis_transaction text,
          location text,
          number bigint,
          offset bigint,
          output_transaction text,
          sat bigint,
          timestamp bigint,
          sha256 text,
          INDEX index_id (id),
          INDEX index_number (number),
          INDEX index_block (genesis_height)
      )")?;
    Ok(())
  }

  pub(crate) fn insert_metadata(pool: &mysql::Pool, metadata: Metadata, inscription: Inscription) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = pool.get_conn()?;
    let exec = conn.exec_iter(
      r"INSERT INTO ordinals (id, address, content, content_length, content_type, genesis_fee, genesis_height, genesis_transaction, location, number, offset, output_transaction, sat, timestamp, sha256)
        VALUES (:id, :address, :content, :content_length, :content_type, :genesis_fee, :genesis_height, :genesis_transaction, :location, :number, :offset, :output_transaction, :sat, :timestamp, :sha256)",
      params! { "id" => metadata.id,
                "address" => metadata.address,
                "content" => inscription.body(),
                "content_length" => metadata.content_length,
                "content_type" => metadata.content_type,
                "genesis_fee" => metadata.genesis_fee,
                "genesis_height" => metadata.genesis_height,
                "genesis_transaction" => metadata.genesis_transaction,
                "location" => metadata.location,
                "number" => metadata.number,
                "offset" => metadata.offset,
                "output_transaction" => metadata.output_transaction,
                "sat" => metadata.sat,
                "timestamp" => metadata.timestamp,
                "sha256" => metadata.sha256
      }
    );
    match exec {
      Ok(_) => Ok(()),
      Err(error) => {
        println!("Error inserting ordinal metadata: {}", error);
        Err(Box::new(error))
      }
    }
  }

  pub(crate) fn get_start_number(pool: &mysql::Pool) -> Result<i64, Box<dyn std::error::Error>> {
    let mut conn = pool.get_conn()?;
    let mut row = conn.query_iter("select min(previous) from (select number, Lag(number,1) over (order BY number) as previous from ordinals) a where number != previous+1")
      .unwrap()
      .next()
      .unwrap()
      .unwrap();
    let row = mysql::from_row::<Option<i64>>(row);
    let number = match row {
      Some(row) => {
        let number: i64 = row;
        number+1
      },
      None => {
        let row = conn.query_iter("select max(number) from ordinals")
          .unwrap()
          .next()
          .unwrap()
          .unwrap();
        let max = mysql::from_row::<Option<i64>>(row);
        match max {
          Some(max) => {
            let number: i64 = max;
            number+1
          },
          None => {
            0
          }
        }
      }
    };
    println!("Inscription numbers fully populated up to: {:?}, removing any straggler entries after this point.", number);
    let exec = conn.exec_iter(
      r"DELETE FROM ordinals WHERE number>:big_number;",
      params! { "big_number" => number
      }
    )?;

    Ok(number)
  }

  pub(crate) async fn get_needed_inscription_numbers(status_vector: Arc<Mutex<Vec<InscriptionNumberStatus>>>) -> Vec<i64> {
    let mut status_vector = status_vector.lock().await;
    let largest_number_in_vec = status_vector.iter().max_by_key(|status| status.inscription_number).unwrap().inscription_number;
    let mut needed_inscription_numbers: Vec<i64> = Vec::new();
    //Find start of needed numbers
    let mut pending_count=0;
    let mut unknown_count=0;
    let mut error_count=0;
    let mut not_found_count=0;
    let mut success_count=0;
    for status in status_vector.iter() {
      if status.status == "UNKNOWN" || status.status == "ERROR" || status.status == "NOT_FOUND" {
        needed_inscription_numbers.push(status.inscription_number);
      }
      if status.status == "PENDING" {
        pending_count = pending_count + 1;
      }
      if status.status == "UNKNOWN" {
        unknown_count = unknown_count + 1;
      }
      if status.status == "ERROR" {
        error_count = error_count + 1;
      }
      if status.status == "NOT_FOUND" {
        not_found_count = not_found_count + 1;
      }
      if status.status == "SUCCESS" {
        success_count = success_count + 1;
      }      
    }
    println!("Pending: {}, Unknown: {}, Error: {}, Not Found: {}, Success: {}", pending_count, unknown_count, error_count, not_found_count, success_count);
    //Fill in needed numbers
    let mut needed_length = needed_inscription_numbers.len();    
    if needed_length < 1000 {
      let mut i = 0;
      while needed_length < 1000 {        
        i = i + 1;
        needed_inscription_numbers.push(largest_number_in_vec + i);
        needed_length = needed_inscription_numbers.len();
      }
    } else {
      needed_inscription_numbers = needed_inscription_numbers[0..1000].to_vec();
    }
    //Mark as pending
    for number in needed_inscription_numbers.clone() {
      match status_vector.iter_mut().find(|status| status.inscription_number == number) {
        Some(status) => {
          status.status = "PENDING".to_string();
        },
        None => {
          let mut status = InscriptionNumberStatus{
            inscription_number: number,
            status: "PENDING".to_string(),
          };
          status_vector.push(status);
        }
      };
    }
    needed_inscription_numbers
  }

  //Server functions
  async fn root() -> &'static str {    
    "Hello, World!"
  }

  async fn home(State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let content = Self::get_ordinal_content(server_config.pool, "6fb976ab49dcec017f1e201e84395983204ae1a7c2abf7ced0a85d692e442799i0".to_string()).await;
    let bytes = content.content;
    let content_type = content.content_type.unwrap();
    (
        ([(axum::http::header::CONTENT_TYPE, content_type)]),
        bytes,
    )
  }

  async fn inscription(Path(inscription_id): Path<InscriptionId>, State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let content = Self::get_ordinal_content(server_config.pool, inscription_id.to_string()).await;
    let bytes = content.content;
    let content_type = content.content_type.unwrap();
    (
        ([(axum::http::header::CONTENT_TYPE, content_type)]),
        bytes,
    )
  }

  async fn inscription_number(Path(number): Path<i64>, State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let content = Self::get_ordinal_content_by_number(server_config.pool, number).await;
    let bytes = content.content;
    let content_type = content.content_type.unwrap();
    (
        ([(axum::http::header::CONTENT_TYPE, content_type)]),
        bytes,
    )
  }

  async fn inscription_metadata(Path(inscription_id): Path<InscriptionId>, State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let metadata = Self::get_ordinal_metadata_from_db(server_config.pool, inscription_id.to_string()).await;
    (
        ([(axum::http::header::CONTENT_TYPE, "application/json")]),
        Json(metadata),
    )
  }

  async fn inscription_number_metadata(Path(number): Path<i64>, State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let metadata = Self::get_ordinal_metadata_from_db_by_number(server_config.pool, number).await;
    (
        ([(axum::http::header::CONTENT_TYPE, "application/json")]),
        Json(metadata),
    )
  }

  async fn get_ordinal_content(pool: sqlx::Pool<sqlx::MySql>, inscription_id: String) -> Content {
    let content = sqlx::query("SELECT content, content_type FROM ordinals WHERE id=?")
      .bind(inscription_id)
      .map(|row| Content {
          content: row.get("content"),
          content_type: row.get("content_type")
      })
    .fetch_one(&pool).await.unwrap();
    content
  }

  async fn get_ordinal_content_by_number(pool: sqlx::Pool<sqlx::MySql>, number: i64) -> Content {
    let content = sqlx::query("SELECT content, content_type FROM ordinals WHERE number=?")
      .bind(number)
      .map(|row| Content {
          content: row.get("content"),
          content_type: row.get("content_type")
      })
    .fetch_one(&pool).await.unwrap();
    content
  }

  async fn get_ordinal_metadata_from_db(pool: sqlx::Pool<sqlx::MySql>, inscription_id: String) -> Metadata {
    let row = sqlx::query("SELECT * FROM ordinals WHERE id=?")
      .bind(inscription_id)
      .map(|row| Metadata {
          id: row.get("id"),
          address: row.get("address"),
          content_length: row.get("content_length"),
          content_type: row.get("content_type"), 
          genesis_fee: row.get("genesis_fee"),
          genesis_height: row.get("genesis_height"),
          genesis_transaction: row.get("genesis_transaction"),
          location: row.get("location"),
          number: row.get("number"),
          offset: row.get("offset"),
          output_transaction: row.get("output_transaction"),
          sat: row.get("sat"),
          timestamp: row.get("timestamp"),
          sha256: row.get("sha256")
      })
      .fetch_one(&pool).await.unwrap();
    row    
  }

  async fn get_ordinal_metadata_from_db_by_number(pool: sqlx::Pool<sqlx::MySql>, number: i64) -> Metadata {
    let row: Metadata = sqlx::query("SELECT * FROM ordinals WHERE number=?")
      .bind(number)
      .map(|row| Metadata {
          id: row.get("id"),
          address: row.get("address"),
          content_length: row.get("content_length"),
          content_type: row.get("content_type"), 
          genesis_fee: row.get("genesis_fee"),
          genesis_height: row.get("genesis_height"),
          genesis_transaction: row.get("genesis_transaction"),
          location: row.get("location"),
          number: row.get("number"),
          offset: row.get("offset"),
          output_transaction: row.get("output_transaction"),
          sat: row.get("sat"),
          timestamp: row.get("timestamp"),
          sha256: row.get("sha256")
      })
      .fetch_one(&pool).await.unwrap();
    row    
  }

}