use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env::var;
use flate2::bufread::GzDecoder;
use nng::{Message, Protocol, Socket};
use rayon::prelude::*;

/* Code to read graph from CSV file
 * and add it into service (using HTTP REST or NNG API).
**/

lazy_static! {
    static ref HTTP_SERVICE_URL: String =
        var("SERVICE_URL")
            .unwrap_or("http://localhost:8000".to_string());

    static ref NNG_SERVICE_URL: String =
        var("RUST_SERVICE_URL")
            .unwrap_or("tcp://127.0.0.1:10234".to_string());
}


#[derive(Debug, Serialize, Deserialize)]
struct Record {
    src: String,
    dest: String,
    weight: f64,
    timestamp: f64
}
/*
#[derive(Serialize)]
struct Edge {
    src: String,
    dest: String,
    weight: f64
}
*/
fn send(req: Record) -> Result<String, Box<dyn std::error::Error>> {
    let url = format!("{}/edge", *HTTP_SERVICE_URL);
    let client = reqwest::blocking::Client::new();
    let body = client.put(url).json(&req).send()?.text()?;
    let json: Value = serde_json::from_str(&body)?;
    let message: String =
        json.get("message")
            .and_then(|v| serde_json::to_string(v).ok())
            .unwrap_or(format!("Warning: cannot decode HTTP reply: {}", body).to_string());
    Ok(message)
}

fn request<T: for<'a> Deserialize<'a>>(
    req: &Vec<u8>,
) -> Result<Vec<T>, Box<dyn std::error::Error + 'static>> {
    let client = Socket::new(Protocol::Req0)?;
    client.dial(&NNG_SERVICE_URL)?;
    client
        .send(Message::from(req.as_slice()))
        .map_err(|(_, err)| err)?;
    let msg: Message = client.recv()?;
    let slice: &[u8] = msg.as_slice();
    rmp_serde::from_slice(slice).or_else(|_| {
        let err: String = rmp_serde::from_slice(slice)?;
        Err(Box::from(format!("Server error: {}", err)))
    })
}

fn mr_edge0(
    src: &str,
    dest: &str,
    weight: f64,
) -> Result<(), Box<dyn std::error::Error>>
{
    let rq = (((src, dest, weight), ), ());
    let req = rmp_serde::to_vec(&rq)?;
    let _: Vec<(String, String, f64)> = request(&req)?;
    Ok(())
}

fn mr_edge(req: Record) -> Result<(), Box<dyn std::error::Error>>
{
    mr_edge0(req.src.as_str(), req.dest.as_str(), req.weight)
}


fn main() {
    // https://snap.stanford.edu/data/soc-sign-bitcoinotc.csv.gz
    let path = "/home/shestero/Downloads/soc-sign-bitcoinotc.csv.gz";

    println!("Reading {path}");
    let file =
        std::fs::File::open(path).expect(format!("file {path} not found!").as_str());
    let gzrdr =
        std::io::BufReader::new(file);
    let ungz =
        GzDecoder::new(gzrdr);//.expect("couldn't decode gzip stream");
    let rdr =
        std::io::BufReader::new(ungz);
    let mut csvrdr =
        csv::ReaderBuilder::new().has_headers(false).from_reader(rdr);
    let arr: Vec<Record> = csvrdr.deserialize().flatten().collect();
    arr.into_par_iter().for_each(|record| {
        println!("{:?}", record);
        // Adding the edge using REST HTTP:
        let r = send(record);
        // Adding the edge using NNG:
        //let r = mr_edge(record);
        match r {
            Err(e) =>
                println!("ERROR={:?}", e),
            Ok(_) => (),
        }
    });
    println!("Bye!")
}
