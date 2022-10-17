use data_quality_report::*;
use std::io::Cursor;
use polars::prelude::*;
use reqwest::blocking::Client;

fn main()
{
  std::env::set_var( "RUST_LOG", "info" );
  pretty_env_logger::init();

  let data = Client::new()
  .get( "https://raw.githubusercontent.com/doordash-oss/DataQualityReport/main/tests/ds_salaries.csv" )
  .send().unwrap()
  .text().unwrap()
  .bytes()
  .collect::< Vec< _ > >();

  let df = CsvReader::new( Cursor::new( data ) )
  .has_header( true )
  .finish().unwrap()
  .lazy()
  .collect().unwrap();

  // let df = df!
  // [
  //   "work_year" =>        [ None,         None,         None, None, None, None, None, None, None, Some( 2020 ) ], 
  //   "experience_level" => [ Some( "SM" ), Some( "XC" ), None, None, None, None, None, None, None, Some( "QW" ) ], 
  // ].unwrap();

  log::info!( "Input DF\n{df}" );

  let dqr = DataQualityReport::new
  (
    df, "some".to_owned(), None, None
  );
  let rep = dqr.wartings_report_str( 3.0 );
  println!( "{}", rep );
}