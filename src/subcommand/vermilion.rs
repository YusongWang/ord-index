use super::*;
use axum_server::Handle;
use crate::subcommand::server;
use crate::index::fetcher;

use mysql_async::TxOpts;
use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use mysql_async::params;
use tokio::sync::Semaphore;
use tokio::sync::Mutex;
use serde::Serialize;
use sha256::digest;

use s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3 as s3;	
use s3::primitives::ByteStream;	
use s3::error::ProvideErrorMetadata;

use axum::{
  routing::get,
  Json, 
  Router,
  extract::{Path, State},
  body::{Body, BoxBody},
  middleware::map_response
};

use tower_http::trace::TraceLayer;
use tower_http::trace::DefaultMakeSpan;
use tracing::Span;
use http::{Request, Response};
use tracing::Level as TraceLevel;

use std::collections::BTreeSet;
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
    help = "Number of threads to use when uploading content and metadata. [default: 1]."
  )]
  n_threads: Option<u16>,
  #[clap(long, help = "Only run api server, do not run indexer. [default: false].")]
  pub run_api_server_only: bool
}

#[derive(Clone, Serialize)]
pub struct Metadata {
  id: String,
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
  sha256: Option<String>,
  text: Option<String>,
  is_json: bool
}

#[derive(Clone, Serialize)]
pub struct Content {
  content: Vec<u8>,
  content_type: Option<String>
}

#[derive(Clone, Serialize)]
pub struct InscriptionNumberEdition {
  id: String,
  number: i64,
  edition: u64
}

#[derive(Clone, Serialize)]
pub struct InscriptionMetadataForBlock {
  id: String,
  content_length: Option<i64>,
  content_type: Option<String>,
  genesis_fee: i64,
  genesis_height: i64,
  number: i64,
  timestamp: i64
}

pub struct InscriptionNumberStatus {
  inscription_number: i64,
  status: String
}

#[derive(Clone,PartialEq, PartialOrd, Ord, Eq)]
pub struct IndexerTimings {
  inscription_start: i64,
  inscription_end: i64,
  acquire_permit_start: Instant,
  acquire_permit_end: Instant,
  get_numbers_start: Instant,
  get_numbers_end: Instant,
  get_id_start: Instant,
  get_id_end: Instant,
  get_inscription_start: Instant,
  get_inscription_end: Instant,
  upload_content_start: Instant,
  upload_content_end: Instant,
  get_metadata_start: Instant,
  get_metadata_end: Instant,
  retrieval: Duration,
  insertion: Duration,
  locking: Duration
}

#[derive(Clone)]
pub struct ApiServerConfig {
  pool: mysql_async::Pool,
  s3client: s3::Client,
  bucket_name: String
}

impl Vermilion {
  pub(crate) fn run(self, options: Options, index: Arc<Index>, handle: Handle) -> SubcommandResult {
    println!("Ordinals Indexer Starting");
    //1. Ordinals Server
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
          println!("Ordinals server stopped");
        },
        Err(err) => {
          println!("Ordinals server failed to start: {:?}", err);
        }
      }
    });

    //2. Vermilion Indexer
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {      
      println!("Vermilion Indexer Starting");
      let clone = index.clone();
      println!("Index acquired");
      let config = options.load_config().unwrap();
      let url = config.db_connection_string.unwrap();
      let pool = Pool::new(url.as_str());
      let s3_config = aws_config::from_env().load().await;
      let s3client = s3::Client::new(&s3_config);
      let s3_bucket_name = config.s3_bucket_name.unwrap();
      let s3_upload_start_number = config.s3_upload_start_number.unwrap_or(0);
      let s3_head_check = config.s3_head_check.unwrap_or(false);
      let n_threads = self.n_threads.unwrap_or(1).into();
      let sem = Arc::new(Semaphore::new(n_threads));
      let status_vector: Arc<Mutex<Vec<InscriptionNumberStatus>>> = Arc::new(Mutex::new(Vec::new()));
      let timing_vector: Arc<Mutex<Vec<IndexerTimings>>> = Arc::new(Mutex::new(Vec::new()));
      Self::create_metadata_table(&pool).await.unwrap();      
      Self::create_edition_procedure(pool.clone()).await.unwrap();
      let start_number = Self::get_start_number(&pool).await.unwrap();      
      println!("Inscriptions in s3 assumed populated up to: {:?}, will only upload content for {:?} onwards.", std::cmp::max(s3_upload_start_number, start_number)-1, std::cmp::max(s3_upload_start_number, start_number));
      let initial = InscriptionNumberStatus {
        inscription_number: start_number,
        status: "UNKNOWN".to_string()
      };
      status_vector.lock().await.push(initial);

      // every iteration fetches 1k inscriptions
      let time = Instant::now();
      print!("Starting @ {:?}", time);
      loop {
        let t0 = Instant::now();
        //break if ctrl-c is received
        if SHUTTING_DOWN.load(atomic::Ordering::Relaxed) {
          break;
        }
        let permit = Arc::clone(&sem).acquire_owned().await;
        let cloned_index = clone.clone();
        let cloned_pool = pool.clone();        
        let cloned_s3client = s3client.clone();
        let cloned_bucket_name = s3_bucket_name.clone();
        let cloned_status_vector = status_vector.clone();
        let cloned_timing_vector = timing_vector.clone();
        let fetcher = fetcher::Fetcher::new(&options)?;//Need a new fetcher for each thread
        tokio::task::spawn(async move {
          let t1 = Instant::now();
          let _permit = permit;
          let needed_numbers = Self::get_needed_inscription_numbers(cloned_status_vector.clone()).await;
          let mut should_sleep = false;
          println!("Trying Numbers: {:?}-{:?}", &needed_numbers[0], &needed_numbers[&needed_numbers.len()-1]);          

          //1. Get ids
          let t2 = Instant::now();
          let mut inscription_ids: Vec<InscriptionId> = Vec::new();          
          for j in needed_numbers.clone() {
            let inscription_id = cloned_index.get_inscription_id_by_inscription_number(j).unwrap();
            match inscription_id {
              Some(inscription_id) => {
                inscription_ids.push(inscription_id);
              },
              None => {
                println!("No inscription found for inscription number: {}. Marking as not found. Breaking from loop", j);
                let status_vector = cloned_status_vector.clone();
                for l in needed_numbers.clone() {                  
                  let mut locked_status_vector = status_vector.lock().await;
                  let status = locked_status_vector.iter_mut().find(|x| x.inscription_number == l).unwrap();
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
          let t3 = Instant::now();
          let cloned_ids = inscription_ids.clone();
          let txs = fetcher.get_transactions(cloned_ids.into_iter().map(|x| x.txid).collect()).await;
          let err_txs = match txs {
              Ok(txs) => Some(txs),
              Err(error) => {
                println!("Error getting transactions {}-{}: {:?}", &needed_numbers[0], &needed_numbers[&needed_numbers.len()-1], error);
                let status_vector = cloned_status_vector.clone();
                for j in needed_numbers.clone() {                  
                  let mut locked_status_vector = status_vector.lock().await;
                  let status = locked_status_vector.iter_mut().find(|x| x.inscription_number == j).unwrap();
                  status.status = "ERROR".to_string();
                }
                println!("error string: {}", error.to_string());
                if  error.to_string().contains("Failed to fetch raw transaction") || 
                    error.to_string().contains("connection closed") || 
                    error.to_string().contains("error trying to connect") || 
                    error.to_string().contains("end of file") {
                  println!("Pausing for 60s & Breaking from loop");
                  //std::mem::drop(locked_status_vector); //Drop the mutex so sleep doesn't block
                  tokio::time::sleep(Duration::from_secs(60)).await;
                }
                return;
              }
          };
          let clean_txs = err_txs.unwrap();
          let cloned_ids = inscription_ids.clone();
          let id_txs: Vec<_> = cloned_ids.into_iter().zip(clean_txs.into_iter()).collect();
          let mut inscriptions: Vec<Inscription> = Vec::new();
          for (inscription_id, tx) in id_txs {
            let inscription = Inscription::from_transaction(&tx)
              .get(inscription_id.index as usize)
              .map(|transaction_inscription| transaction_inscription.inscription.clone())
              .unwrap();
            inscriptions.push(inscription);
          }

          //3. Upload ordinal content to s3
          let t4 = Instant::now();
          let cloned_ids = inscription_ids.clone();
          let cloned_inscriptions = inscriptions.clone();
          let number_id_inscriptions: Vec<_> = needed_numbers.clone().into_iter()
            .zip(cloned_ids.into_iter())
            .zip(cloned_inscriptions.into_iter())
            .map(|((x, y), z)| (x, y, z))
            .collect();          
          for (number, inscription_id, inscription) in number_id_inscriptions {
            if number < s3_upload_start_number {
                continue;
            }
            Self::upload_ordinal_content(&cloned_s3client, &cloned_bucket_name, inscription_id, inscription, s3_head_check).await;	//TODO: Handle errors
          }
          
          //4. Get ordinal metadata
          let t5 = Instant::now();
          let status_vector = cloned_status_vector.clone();
          let cloned_ids = inscription_ids.clone();
          let cloned_inscriptions = inscriptions.clone();
          
          let id_inscriptions: Vec<_> = cloned_ids.into_iter().zip(cloned_inscriptions.into_iter()).collect();
          let mut retrieval = Duration::from_millis(0);
          let mut metadata_vec: Vec<Metadata> = Vec::new();
          for (inscription_id, inscription) in id_inscriptions {
            let t0 = Instant::now();
            let metadata: Metadata = Self::extract_ordinal_metadata(cloned_index.clone(), inscription_id, inscription.clone()).unwrap();
            metadata_vec.push(metadata.clone());
            let t1 = Instant::now();            
            retrieval += t1.duration_since(t0);
          }
          //4.1 Insert metadata
          let t51 = Instant::now();
          let insert_result = Self::bulk_insert_metadata(&cloned_pool.clone(), metadata_vec).await;
          //4.2 Update status
          let t52 = Instant::now();
          if insert_result.is_err() {
            println!("Error bulk inserting metadata for inscription numbers: {}-{}. Marking as error", &needed_numbers[0], &needed_numbers[&needed_numbers.len()-1]);
            let mut locked_status_vector = status_vector.lock().await;
            for j in needed_numbers.clone() {              
              let status = locked_status_vector.iter_mut().find(|x| x.inscription_number == j).unwrap();
              status.status = "ERROR".to_string();
            }
          } else {
            let mut locked_status_vector = status_vector.lock().await;
            for j in needed_numbers.clone() {              
              let status = locked_status_vector.iter_mut().find(|x| x.inscription_number == j).unwrap();
              if status.status != "NOT_FOUND".to_string() {
                status.status = "SUCCESS".to_string();
              }              
            }
          }
          
          //5. Log timings
          let t6 = Instant::now();
          println!("Finished numbers {} - {} @ {:?}", &needed_numbers[0], &needed_numbers[&needed_numbers.len()-1], t5);
          let timing = IndexerTimings {
            inscription_start: needed_numbers[0],
            inscription_end: needed_numbers[&needed_numbers.len()-1],
            acquire_permit_start: t0,
            acquire_permit_end: t1,
            get_numbers_start: t1,
            get_numbers_end: t2,
            get_id_start: t2,
            get_id_end: t3,
            get_inscription_start: t3,
            get_inscription_end: t4,
            upload_content_start: t4,
            upload_content_end: t5,
            get_metadata_start: t5,
            get_metadata_end: t6,
            retrieval: retrieval,
            insertion: t52.duration_since(t51),
            locking: t6.duration_since(t52)
          };
          cloned_timing_vector.lock().await.push(timing);
          Self::print_index_timings(cloned_timing_vector, n_threads as u32).await;

          //6. Sleep thread if up to date.
          if should_sleep {
            println!("Sleeping for 60s");
            tokio::time::sleep(Duration::from_secs(60)).await;
          }
        });        
        
      }      
      Ok(Box::new(Empty {}) as Box<dyn Output>)
    })
  }

  pub(crate) fn run_vermilion_server(self, options: Options) {
    println!("Vermilion Server Running");
    let api_server_options_clone = options.clone();
    let verm_server_thread = thread::spawn(move ||{
      let rt = Runtime::new().unwrap();
      rt.block_on(async move {
        let config = api_server_options_clone.load_config().unwrap();
        let url = config.db_connection_string.unwrap();
        let pool = mysql_async::Pool::new(url.as_str());
        let bucket_name = config.s3_bucket_name.unwrap();
        let s3_config = aws_config::from_env().load().await;
        let s3client = s3::Client::new(&s3_config);
        
        let server_config = ApiServerConfig {
          pool: pool,
          s3client: s3client,
          bucket_name: bucket_name
        };

        let app = Router::new()
          .route("/", get(Self::root))
          .route("/home", get(Self::home))
          .route("/inscription/:inscription_id", get(Self::inscription))
          .route("/inscription_number/:number", get(Self::inscription_number))
          .route("/inscription_sha256/:sha256", get(Self::inscription_sha256))
          .route("/inscription_metadata/:inscription_id", get(Self::inscription_metadata))
          .route("/inscription_metadata_number/:number", get(Self::inscription_metadata_number))
          .route("/inscription_editions/:inscription_id", get(Self::inscription_editions))
          .route("/inscription_editions_number/:number", get(Self::inscription_editions_number))
          .route("/inscription_editions_sha256/:sha256", get(Self::inscription_editions_sha256))
          .route("/inscriptions_in_block/:block", get(Self::inscriptions_in_block))
          .layer(map_response(Self::set_header))
          .layer(
            TraceLayer::new_for_http()
              .make_span_with(DefaultMakeSpan::new().level(TraceLevel::INFO))
              .on_request(|req: &Request<Body>, _span: &Span| {
                tracing::event!(TraceLevel::INFO, "Started processing request {}", req.uri().path());
              })
              .on_response(|res: &Response<BoxBody>, latency: Duration, _span: &Span| {
                tracing::event!(TraceLevel::INFO, "Finished processing request latency={:?} status={:?}", latency, res.status());
              })
          )
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

  //Indexer Helper functions
  pub(crate) async fn upload_ordinal_content(client: &s3::Client, bucket_name: &str, inscription_id: InscriptionId, inscription: Inscription, head_check: bool) {
    let id = inscription_id.to_string();	
    let key = format!("content/{}", id);
    if head_check {
      let head_status = client	
        .head_object()	
        .bucket(bucket_name)	
        .key(key.clone())	
        .send()	
        .await;
      match head_status {	
        Ok(_) => {	
          log::debug!("Ordinal content already exists in S3: {}", id.clone());	
          return;	
        }	
        Err(error) => {	
          if error.to_string() == "service error" {
            let service_error = error.into_service_error();
            if service_error.to_string() != "NotFound" {
              println!("Error checking if ordinal {} exists in S3: {} - {:?} code: {:?}", id.clone(), service_error, service_error.message(), service_error.code());	
              return;	//error
            } else {
              log::trace!("Ordinal {} not found in S3, uploading", id.clone());
            }
          } else {
            println!("Error checking if ordinal {} exists in S3: {} - {:?}", id.clone(), error, error.message());	
            return; //error
          }
        }
      };
    }
    
    let body = Inscription::body(&inscription);	
    let bytes = match body {	
      Some(body) => body.to_vec(),	
      None => {	
        println!("No body found for inscription: {}, filling with empty body", inscription_id);	
        Vec::new()	
      }	
    };	
    let content_type = match Inscription::content_type(&inscription) {	
      Some(content_type) => content_type,	
      None => {	
        println!("No content type found for inscription: {}, filling with empty content type", inscription_id);	
        ""	
      }	
    };
    let put_status = client	
      .put_object()	
      .bucket(bucket_name)	
      .key(key)	
      .body(ByteStream::from(bytes))	
      .content_type(content_type)	
      .send()	
      .await;

    let _ret = match put_status {	
      Ok(put_status) => {	
        log::debug!("Uploaded ordinal content to S3: {}", id.clone());	
        put_status	
      }	
      Err(error) => {	
        println!("Error uploading ordinal {} to S3: {} - {:?}", id.clone(), error, error.message());	
        return;	
      }	
    };
  }

  pub(crate) fn extract_ordinal_metadata(index: Arc<Index>, inscription_id: InscriptionId, inscription: Inscription) -> Result<Metadata> {
    let entry = index
      .get_inscription_entry(inscription_id)
      .unwrap()
      .unwrap();
    let satpoint = index
      .get_inscription_satpoint_by_id(inscription_id)
      .unwrap()
      .unwrap();
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
    let text = match inscription.body() {
      Some(body) => {
        let text = String::from_utf8(body.to_vec());
        match text {
          Ok(text) => Some(text),
          Err(_) => None
        }
      },
      None => {
        None
      }
    };
    let is_json = match inscription.body() {
      Some(body) => {
        let json = serde_json::from_slice::<serde::de::IgnoredAny>(body);
        match json {
          Ok(_) => true,
          Err(_) => false
        }
      },
      None => {
        false
      }
    };
    let metadata = Metadata {
      id: inscription_id.to_string(),
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
      sha256: sha256,
      text: text,
      is_json: is_json
    };
    Ok(metadata)
  }

  pub(crate) async fn create_metadata_table(pool: &mysql_async::Pool) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = pool.get_conn().await.unwrap();
    conn.query_drop(
      r"CREATE TABLE IF NOT EXISTS ordinals (
          id varchar(80) not null primary key,
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
          sha256 varchar(64),
          text mediumtext,
          is_json boolean,
          INDEX index_id (id),
          INDEX index_number (number),
          INDEX index_block (genesis_height),
          INDEX index_sha256 (sha256)
      )").await.unwrap();
    Ok(())
  }

  pub(crate) async fn bulk_insert_metadata(pool: &mysql_async::Pool, metadata_vec: Vec<Metadata>) -> Result<(), Box<dyn std::error::Error + Send>> {
    let mut conn = pool.get_conn().await.unwrap();
    let mut tx = conn.start_transaction(TxOpts::default()).await.unwrap();
    let _exec = tx.exec_batch(
      r"INSERT IGNORE INTO ordinals (id, content_length, content_type, genesis_fee, genesis_height, genesis_transaction, location, number, offset, output_transaction, sat, timestamp, sha256, text, is_json)
        VALUES (:id, :content_length, :content_type, :genesis_fee, :genesis_height, :genesis_transaction, :location, :number, :offset, :output_transaction, :sat, :timestamp, :sha256, :text, :is_json)",
        metadata_vec.iter().map(|metadata| params! { 
          "id" => &metadata.id,
          "content_length" => &metadata.content_length,
          "content_type" => &metadata.content_type,
          "genesis_fee" => &metadata.genesis_fee,
          "genesis_height" => &metadata.genesis_height,
          "genesis_transaction" => &metadata.genesis_transaction,
          "location" => &metadata.location,
          "number" => &metadata.number,
          "offset" => &metadata.offset,
          "output_transaction" => &metadata.output_transaction,
          "sat" => &metadata.sat,
          "timestamp" => &metadata.timestamp,
          "sha256" => &metadata.sha256,
          "text" => &metadata.text,
          "is_json" => &metadata.is_json
      })
    ).await;
    let result = tx.commit().await;
    match result {
      Ok(_) => Ok(()),
      Err(error) => {
        println!("Error bulk inserting ordinal metadata: {}", error);
        Err(Box::new(error))
      }
    }
  }

  pub(crate) async fn get_start_number(pool: &mysql_async::Pool) -> Result<i64, Box<dyn std::error::Error>> {
    let mut conn = pool.get_conn().await.unwrap();
    let row = conn.query_iter("select min(previous) from (select number, Lag(number,1) over (order BY number) as previous from ordinals) a where number != previous+1")
      .await
      .unwrap()
      .next()
      .await
      .unwrap()
      .unwrap();
    let row = mysql_async::from_row::<Option<i64>>(row);
    let number = match row {
      Some(row) => {
        let number: i64 = row;
        number+1
      },
      None => {
        let row = conn.query_iter("select max(number) from ordinals")
          .await
          .unwrap()
          .next()
          .await
          .unwrap()
          .unwrap();
        let max = mysql_async::from_row::<Option<i64>>(row);
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
    println!("Inscription numbers in db fully populated up to: {:?}, starting metadata upload from {:?}", number-1, number);

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
    log::info!("Pending: {}, Unknown: {}, Error: {}, Not Found: {}, Success: {}", pending_count, unknown_count, error_count, not_found_count, success_count);
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
          let status = InscriptionNumberStatus{
            inscription_number: number,
            status: "PENDING".to_string(),
          };
          status_vector.push(status);
        }
      };
    }
    //Remove successfully processed numbers from vector
    status_vector.retain(|status| status.status != "SUCCESS");
    needed_inscription_numbers
  }

  pub(crate) async fn print_index_timings(timings: Arc<Mutex<Vec<IndexerTimings>>>, n_threads: u32) {
    let mut locked_timings = timings.lock().await;
    locked_timings.sort_by(|a, b| a.inscription_start.cmp(&b.inscription_start));
    //First get the relevant entries
    let mut relevant_timings: Vec<IndexerTimings> = Vec::new();
    let mut last = locked_timings.last().unwrap().inscription_start + 1000;
    for timing in locked_timings.iter().rev() {
      if timing.inscription_start == last - 1000 {
        relevant_timings.push(timing.clone());
        if relevant_timings.len() == n_threads as usize {
          break;
        }
      } else {
        relevant_timings = Vec::new();
        relevant_timings.push(timing.clone());
      }      
      last = timing.inscription_start;
    }
    if relevant_timings.len() < n_threads as usize {
      return;
    }    
    relevant_timings.sort_by(|a, b| a.inscription_start.cmp(&b.inscription_start));    
    let mut queueing_total = Duration::new(0,0);
    let mut acquire_permit_total = Duration::new(0,0);
    let mut get_numbers_total = Duration::new(0,0);
    let mut get_id_total = Duration::new(0,0);
    let mut get_inscription_total = Duration::new(0,0);
    let mut upload_content_total = Duration::new(0,0);
    let mut get_metadata_total = Duration::new(0,0);
    let mut retrieval_total = Duration::new(0,0);
    let mut insertion_total = Duration::new(0,0);
    let mut locking_total = Duration::new(0,0);
    let mut last_start = relevant_timings.first().unwrap().acquire_permit_start;
    for timing in relevant_timings.iter() {
      queueing_total = queueing_total + timing.acquire_permit_start.duration_since(last_start);
      acquire_permit_total = acquire_permit_total + timing.acquire_permit_end.duration_since(timing.acquire_permit_start);
      get_numbers_total = get_numbers_total + timing.get_numbers_end.duration_since(timing.get_numbers_start);
      get_id_total = get_id_total + timing.get_id_end.duration_since(timing.get_id_start);
      get_inscription_total = get_inscription_total + timing.get_inscription_end.duration_since(timing.get_inscription_start);
      upload_content_total = upload_content_total + timing.upload_content_end.duration_since(timing.upload_content_start);
      get_metadata_total = get_metadata_total + timing.get_metadata_end.duration_since(timing.get_metadata_start);
      retrieval_total = retrieval_total + timing.retrieval;
      insertion_total = insertion_total + timing.insertion;
      locking_total = locking_total + timing.locking;
      last_start = timing.acquire_permit_start;
    }
    let count = relevant_timings.last().unwrap().inscription_end - relevant_timings.first().unwrap().inscription_start+1;
    let total_time = relevant_timings.last().unwrap().get_metadata_end.duration_since(relevant_timings.first().unwrap().get_numbers_start);
    println!("Inscriptions {}-{}", relevant_timings.first().unwrap().inscription_start, relevant_timings.last().unwrap().inscription_end);
    println!("Total time: {:?}, avg per inscription: {:?}", total_time, total_time/count as u32);
    println!("Queueing time avg per thread: {:?}", queueing_total/n_threads); //9 because the first one doesn't have a recorded queueing time
    println!("Acquiring Permit time avg per thread: {:?}", acquire_permit_total/n_threads); //should be similar to queueing time
    println!("Get numbers time avg per thread: {:?}", get_numbers_total/n_threads);
    println!("Get id time avg per thread: {:?}", get_id_total/n_threads);
    println!("Get inscription time avg per thread: {:?}", get_inscription_total/n_threads);
    println!("Upload content time avg per thread: {:?}", upload_content_total/n_threads);
    println!("Get metadata time avg per thread: {:?}", get_metadata_total/n_threads);
    println!("--Retrieval time avg per thread: {:?}", retrieval_total/n_threads);
    println!("--Insertion time avg per thread: {:?}", insertion_total/n_threads);
    println!("--Locking time avg per thread: {:?}", locking_total/n_threads);

    //Remove printed timings
    let to_remove = BTreeSet::from_iter(relevant_timings);
    locked_timings.retain(|e| !to_remove.contains(e));

  }

  //Server api functions
  async fn root() -> &'static str {    
"One of the fastest ways to dox yourself as a cryptopleb is to ask \"what's the reason for the Bitcoin pump today.\"

Its path to $1m+ is preordained. On any given day it needs no reasons."
  }

  async fn home(State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let response = Self::get_ordinal_content(&server_config.s3client, &server_config.bucket_name, "6fb976ab49dcec017f1e201e84395983204ae1a7c2abf7ced0a85d692e442799i0".to_string()).await;
    let bytes = response.body.collect().await.unwrap().to_vec();
    let content_type = response.content_type.unwrap();
    (
        ([(axum::http::header::CONTENT_TYPE, content_type)]),
        bytes,
    )
  }

  async fn set_header<B>(mut response: Response<B>) -> Response<B> {
    response.headers_mut().insert("cache-control", "public, max-age=31536000, immutable".parse().unwrap());
    response
  }

  async fn inscription(Path(inscription_id): Path<InscriptionId>, State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let response = Self::get_ordinal_content(&server_config.s3client, &server_config.bucket_name, inscription_id.to_string()).await;
    let bytes = response.body.collect().await.unwrap().to_vec();
    let content_type = response.content_type.unwrap();
    (
        ([(axum::http::header::CONTENT_TYPE, content_type)]),
        bytes,
    )
  }

  async fn inscription_number(Path(number): Path<i64>, State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let response = Self::get_ordinal_content_by_number(server_config.pool, &server_config.s3client, &server_config.bucket_name, number).await;
    let bytes = response.body.collect().await.unwrap().to_vec();
    let content_type = response.content_type.unwrap();
    (
        ([(axum::http::header::CONTENT_TYPE, content_type)]),
        bytes,
    )
  }

  async fn inscription_sha256(Path(sha256): Path<String>, State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let response = Self::get_ordinal_content_by_sha256(server_config.pool, &server_config.s3client, &server_config.bucket_name, sha256).await;
    let bytes = response.body.collect().await.unwrap().to_vec();
    let content_type = response.content_type.unwrap();
    (
        ([(axum::http::header::CONTENT_TYPE, content_type)]),
        bytes,
    )
  }

  async fn inscription_metadata(Path(inscription_id): Path<InscriptionId>, State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let metadata = Self::get_ordinal_metadata(server_config.pool, inscription_id.to_string()).await;
    (
        ([(axum::http::header::CONTENT_TYPE, "application/json")]),
        Json(metadata),
    )
  }

  async fn inscription_metadata_number(Path(number): Path<i64>, State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let metadata = Self::get_ordinal_metadata_by_number(server_config.pool, number).await;
    (
        ([(axum::http::header::CONTENT_TYPE, "application/json")]),
        Json(metadata),
    )
  }

  async fn inscription_editions(Path(inscription_id): Path<InscriptionId>, State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let editions = Self::get_matching_inscriptions(server_config.pool, inscription_id.to_string()).await;
    (
        ([(axum::http::header::CONTENT_TYPE, "application/json")]),
        Json(editions),
    )
  }

  async fn inscription_editions_number(Path(number): Path<i64>, State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let editions = Self::get_matching_inscriptions_by_number(server_config.pool, number).await;
    (
        ([(axum::http::header::CONTENT_TYPE, "application/json")]),
        Json(editions),
    )
  }

  async fn inscription_editions_sha256(Path(sha256): Path<String>, State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let editions = Self::get_matching_inscriptions_by_sha256(server_config.pool, sha256).await;
    (
        ([(axum::http::header::CONTENT_TYPE, "application/json")]),
        Json(editions),
    )
  }

  async fn inscriptions_in_block(Path(block): Path<i64>, State(server_config): State<ApiServerConfig>) -> impl axum::response::IntoResponse {
    let inscriptions = Self::get_inscriptions_within_block(server_config.pool, block).await;
    (
        ([(axum::http::header::CONTENT_TYPE, "application/json")]),
        Json(inscriptions),
    )
  }

  //DB functions
  async fn get_ordinal_content(client: &s3::Client, bucket_name: &str, inscription_id: String) -> GetObjectOutput {
    let key = format!("content/{}", inscription_id);
    let content = client
      .get_object()
      .bucket(bucket_name)
      .key(key)
      .send()
      .await
      .unwrap();
    content
  }

  async fn get_ordinal_content_by_number(pool: mysql_async::Pool, client: &s3::Client, bucket_name: &str, number: i64) -> GetObjectOutput {
    let mut conn = Self::get_conn(pool).await;
    let inscription_id: String = conn.exec_first(
      "SELECT id FROM ordinals WHERE number=:number LIMIT 1", 
      params! {
        "number" => number
      }
    )
    .await
    .unwrap()
    .unwrap();

    let content = Self::get_ordinal_content(client, bucket_name, inscription_id).await;
    content
  }

  async fn get_ordinal_content_by_sha256(pool: mysql_async::Pool, client: &s3::Client, bucket_name: &str, sha256: String) -> GetObjectOutput {
    let mut conn = Self::get_conn(pool).await;
    let inscription_id: String = conn.exec_first(
      "SELECT id FROM ordinals WHERE sha256=:sha256 LIMIT 1", 
      params! {
        "sha256" => sha256
      }
    )
    .await
    .unwrap()
    .unwrap();

    let content = Self::get_ordinal_content(client, bucket_name, inscription_id).await;
    content
  }

  async fn get_ordinal_metadata(pool: mysql_async::Pool, inscription_id: String) -> Metadata {
    let mut conn = Self::get_conn(pool).await;
    let result = conn.exec_map(
      "SELECT * FROM ordinals WHERE id=:id LIMIT 1", 
      params! {
        "id" => inscription_id
      },
      |mut row: mysql_async::Row| Metadata {
        id: row.get("id").unwrap(),
        content_length: row.take("content_length").unwrap(),
        content_type: row.take("content_type").unwrap(), 
        genesis_fee: row.get("genesis_fee").unwrap(),
        genesis_height: row.get("genesis_height").unwrap(),
        genesis_transaction: row.get("genesis_transaction").unwrap(),
        location: row.get("location").unwrap(),
        number: row.get("number").unwrap(),
        offset: row.get("offset").unwrap(),
        output_transaction: row.get("output_transaction").unwrap(),
        sat: row.take("sat").unwrap(),
        timestamp: row.get("timestamp").unwrap(),
        sha256: row.take("sha256").unwrap(),
        text: row.take("text").unwrap(),
        is_json: row.get("is_json").unwrap()
      }
    );
    let result = result.await.unwrap().pop().unwrap();
    result
  }

  async fn get_ordinal_metadata_by_number(pool: mysql_async::Pool, number: i64) -> Metadata {
    let mut conn = Self::get_conn(pool).await;
    let result = conn.exec_map(
      "SELECT * FROM ordinals WHERE number=:number LIMIT 1", 
      params! {
        "number" => number
      },
      |mut row: mysql_async::Row| Metadata {
        id: row.get("id").unwrap(),
        content_length: row.take("content_length").unwrap(),
        content_type: row.take("content_type").unwrap(), 
        genesis_fee: row.get("genesis_fee").unwrap(),
        genesis_height: row.get("genesis_height").unwrap(),
        genesis_transaction: row.get("genesis_transaction").unwrap(),
        location: row.get("location").unwrap(),
        number: row.get("number").unwrap(),
        offset: row.get("offset").unwrap(),
        output_transaction: row.get("output_transaction").unwrap(),
        sat: row.take("sat").unwrap(),
        timestamp: row.get("timestamp").unwrap(),
        sha256: row.take("sha256").unwrap(),
        text: row.take("text").unwrap(),
        is_json: row.get("is_json").unwrap()
      }
    );
    let result = result.await.unwrap().pop().unwrap();
    result    
  }

  async fn get_matching_inscriptions(pool: mysql_async::Pool, inscription_id: String) -> Vec<InscriptionNumberEdition> {
    let mut conn = Self::get_conn(pool).await;
    let editions = conn.exec_map(
      "with a as (select sha256 from editions where id = :id) select id, number, edition from editions,a where editions.sha256=a.sha256;",
      params! {
        "id" => inscription_id
      },
      |row: mysql_async::Row| InscriptionNumberEdition {
        id: row.get("id").unwrap(),
        number: row.get("number").unwrap(),
        edition: row.get("edition").unwrap()
      }
    ).await.unwrap();
    editions
  }

  async fn get_matching_inscriptions_by_number(pool: mysql_async::Pool, number: i64) -> Vec<InscriptionNumberEdition> {
    let mut conn = Self::get_conn(pool).await;
    let editions = conn.exec_map(
      "with a as (select sha256 from editions where number = :number) select id, number, edition from editions,a where editions.sha256=a.sha256;", 
      params! {
        "number" => number
      },
      |row: mysql_async::Row| InscriptionNumberEdition {
        id: row.get("id").unwrap(),
        number: row.get("number").unwrap(),
        edition: row.get("edition").unwrap()
      }
    ).await.unwrap();
    editions
  }

  async fn get_matching_inscriptions_by_sha256(pool: mysql_async::Pool, sha256: String) -> Vec<InscriptionNumberEdition> {
    let mut conn = Self::get_conn(pool).await;
    let editions = conn.exec_map(
      "select id, number, edition from editions where sha256=:sha256;", 
      params! {
        "sha256" => sha256
      },
      |row: mysql_async::Row| InscriptionNumberEdition {
        id: row.get("id").unwrap(),
        number: row.get("number").unwrap(),
        edition: row.get("edition").unwrap()
      }
    ).await.unwrap();
    editions
  }

  async fn get_inscriptions_within_block(pool: mysql_async::Pool, block: i64) -> Vec<InscriptionMetadataForBlock> {
    let mut conn = Self::get_conn(pool).await;
    let inscriptions = conn.exec_map(
      "SELECT id, content_length, content_type, genesis_fee, genesis_height, number, timestamp FROM ordinals WHERE genesis_height=:block", 
      params! {
        "block" => block
      },
      |mut row: mysql_async::Row| InscriptionMetadataForBlock {
        id: row.get("id").unwrap(),
        content_length: row.take("content_length").unwrap(),
        content_type: row.take("content_type").unwrap(), 
        genesis_fee: row.get("genesis_fee").unwrap(),
        genesis_height: row.get("genesis_height").unwrap(),
        number: row.get("number").unwrap(),
        timestamp: row.get("timestamp").unwrap()
      }
    ).await.unwrap();
    inscriptions
  }
  
  async fn get_conn(pool: mysql_async::Pool) -> mysql_async::Conn {
    let conn: mysql_async::Conn = pool.get_conn().await.unwrap();
    conn
  }

  async fn create_edition_procedure(pool: mysql_async::Pool) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Self::get_conn(pool).await;
    let mut tx = conn.start_transaction(TxOpts::default()).await.unwrap();
    tx.query_drop(r"DROP PROCEDURE IF EXISTS update_editions").await.unwrap();
    tx.query_drop(
      r#"CREATE PROCEDURE update_editions()
      BEGIN
      IF "editions" NOT IN (SELECT table_name FROM information_schema.tables) THEN
      CREATE TABLE editions as select id, number, sha256, row_number() OVER(PARTITION BY sha256 ORDER BY number asc) as edition, count(number) OVER(PARTITION BY sha256) as total from ordinals;
      CREATE INDEX idx_id ON editions (id);
      CREATE INDEX idx_number ON editions (number);
      CREATE INDEX idx_sha256 ON editions (sha256);
      ELSE
      DROP TABLE IF EXISTS editions_new;
      CREATE TABLE editions_new as select id, number, sha256, row_number() OVER(PARTITION BY sha256 ORDER BY number asc) as edition, count(number) OVER(PARTITION BY sha256) as total from ordinals;
      CREATE INDEX idx_id ON editions_new (id);
      CREATE INDEX idx_number ON editions_new (number);
      CREATE INDEX idx_sha256 ON editions_new (sha256);
      RENAME TABLE editions to editions_old, editions_new to editions;
      DROP TABLE IF EXISTS editions_old;
      END IF;
      END;"#).await.unwrap();
    tx.query_drop(r"DROP EVENT IF EXISTS editions_event").await.unwrap();
    tx.query_drop(r"CREATE EVENT editions_event ON SCHEDULE EVERY 24 HOUR STARTS FROM_UNIXTIME(CEILING(UNIX_TIMESTAMP(CURTIME())/86400)*86400) DO CALL update_editions()").await.unwrap();
    let result = tx.commit().await;
    match result {
      Ok(_) => Ok(()),
      Err(error) => {
        println!("Error creating editions table stored procedure: {}", error);
        Err(Box::new(error))
      }
    }
  }

  //Deprecated DB functions
  async fn get_ordinal_content_from_db(pool: mysql_async::Pool, inscription_id: String) -> Content {
    let mut conn = Self::get_conn(pool).await;
    let content: Content = conn.exec_map(
      "SELECT content, content_type FROM ordinals WHERE id=:id LIMIT 1", 
      params! {
        "id" => inscription_id
      },
      |mut row: mysql_async::Row| Content {
        content: row.get("content").unwrap(),
        content_type: row.take("content_type").unwrap()
      }
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    content
  }

  async fn get_ordinal_content_by_number_from_db(pool: mysql_async::Pool, number: i64) -> Content {
    let mut conn = Self::get_conn(pool).await;
    let content: Content = conn.exec_map(
      "SELECT content, content_type FROM ordinals WHERE number=:number LIMIT 1", 
      params! {
        "number" => number
      },
      |mut row: mysql_async::Row| Content {
        content: row.get("content").unwrap(),
        content_type: row.take("content_type").unwrap()
      }
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    content
  }

  async fn get_ordinal_content_by_sha256_from_db(pool: mysql_async::Pool, sha256: String) -> Content {
    let mut conn = Self::get_conn(pool).await;
    let content: Content = conn.exec_map(
      "SELECT content, content_type FROM ordinals WHERE sha256=:sha256 LIMIT 1", 
      params! {
        "sha256" => sha256
      },
      |mut row: mysql_async::Row| Content {
        content: row.get("content").unwrap(),
        content_type: row.take("content_type").unwrap()
      }
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    content
  }

}