use data_quality_report::*;
use std::io::Cursor;
use polars::prelude::*;
use reqwest::blocking::Client;

fn main()
{
  // Can be removed if no needed
  std::env::set_var( "RUST_LOG", "info" );
  pretty_env_logger::init();

  // // load data set by url
  // let data = Client::new()
  // .get( "https://raw.githubusercontent.com/doordash-oss/DataQualityReport/main/tests/ds_salaries.csv" )
  // .send().unwrap()
  // .text().unwrap()
  // .bytes()
  // .collect::< Vec< _ > >();

  // // read data to DataFrame
  // let df = CsvReader::new( Cursor::new( data ) )
  // .has_header( true )
  // .finish().unwrap()
  // .lazy()
  // .collect().unwrap();

  let df = df!
  [
    "work_year" =>        [ None,         None,         None,      None,      None,      None, None,      None, None, Some( 2020 ) ], 
    "experience_level" => [ Some( "SM" ), Some( "XC" ), None,      None,      None,      None, None,      None, None, Some( "QW" ) ], 
    "employment_type" =>  Vec::< Option< String > >::from(
                          [ None,         None,         None,      None,      None,      None, None,      None, None, None         ]),
    "negatives" =>        [ Some( -1 ),   Some( -1 ),   Some( -1 ),Some( -1 ),Some( 1 ), None, Some( 1 ), None, None, Some( 1 )    ],
    "most_freq" =>        [ Some( 1 ),    Some( 1 ),    Some( 1 ), Some( 1 ), Some( 1 ), None, Some( 1 ), None, None, Some( 1 )    ],
  ].unwrap();

  log::info!( "Input DF\n{df}" );

  // Initialize data quality report
  let dqr = DataQualityReport::new( df )
  .missing_by( "work_year" )
  .setup();

  // Gets the report into String
  let rep = dqr.wartings_report_str( 3.0 );
  println!( "{}", rep );
}