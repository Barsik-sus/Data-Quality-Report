use std::collections::BTreeMap;

use polars::prelude::*;

mod utils;
use utils::Summary;

#[ derive( Debug ) ]
pub struct DataQualityWarning
{
  level : i32,
  field : String,
  msg : String,
}

#[ derive( Debug, Clone ) ]
pub struct DataQualityRule
{
  pub level : i32,
  pub condition : Expr,
  pub fields : Vec< String >,
  pub msg : Option< String >,
}

lazy_static::lazy_static!
{
  static ref TARGET_RULES : Vec< DataQualityRule > = vec!
  [
    DataQualityRule
    {
      level : 0,
      condition : col( "perc_missing" ).gt( 0 ),
      fields : vec![ "perc_missing".to_owned() ],
      msg : None
    },
  ];

  static ref FEATURE_RULES : Vec< DataQualityRule > = vec!
  [
    DataQualityRule
    {
      level : 0,
      condition : col( "perc_missing" ).gt( 0.95 ),
      fields : vec![ "perc_missing".to_owned() ],
      msg : None
    },
  ];
}


#[ derive( Debug ) ]
pub struct DataQualityReport
{
  df : DataFrame,
  missing_by : String,
  rules : Vec< DataQualityRule >,
  summary_df : DataFrame,
}

impl DataQualityReport
{
  pub fn new
  (
    mut df : DataFrame,
    missing_by : String,
    rules : Option< Vec< DataQualityRule > >,
    max_rows : Option< usize >,
  ) -> Self
  {
    let num_rows = df.height();
    if !max_rows.is_none() && num_rows > max_rows.unwrap()
    {
      println!( "DataFrame has {num_rows} raws, sampling {max_rows} to reduce latency. Specify `max_rows=None` to disable.", max_rows = max_rows.unwrap() );
      df = df.sample_n( max_rows.unwrap(), false, false, None ).unwrap()
    }
    let rules = if rules.is_none()
    { FEATURE_RULES.clone() }
    else
    { rules.unwrap() };

    log::info!( "{:?}", rules );
    let summary_df = df.summarize();
    log::info!( "{:#?}", &summary_df );

    Self{ df, missing_by, rules, summary_df }
  }

  pub fn warnings( &self, min_dq_level : f32 ) -> Vec< DataQualityWarning >
  {
    self.rules.clone().iter()
    .filter_map( | rule |
    {
      if rule.level as f32 <= min_dq_level
      {
        return Some( self.warns_from_rule( rule ) )
      }
      None
    })
    .fold( vec![], | mut acc, mut warns |
    {
      acc.append( &mut warns );
      acc
    })
  }

  fn warns_from_rule( &self, rule : &DataQualityRule ) -> Vec< DataQualityWarning >
  {
    let rows = self.summary_df.clone().lazy()
    .filter( rule.condition.to_owned() )
    .collect().unwrap();

    log::info!( "all :\n{:#?}", &rows );

    rows
    .select_at_idx( 0 ).unwrap().utf8().unwrap().into_iter()
    .filter( | col | col.is_some() ) // ? Need to review
    .map( | row |
    {
      DataQualityWarning
      {
        level : rule.level,
        field : row.unwrap().to_owned(),
        msg : format!
        (
          "{msg}{fields}",
          msg = rule.msg.to_owned().and_then( | s | Some( format!( "{s} " ) ) ).unwrap_or_default(),
          fields = rule.fields.join( ", " ) // ! add values of fields
        )
      }
    }).collect()
  }

  pub fn warnings_summary_str( &self, min_dq_level : f32 ) -> String
  {
    let warns = self.warnings( min_dq_level );
    if warns.is_empty() { return format!( "No Warnings" ) }

    let warns_counter : BTreeMap< i32, usize > = warns.iter()
    .fold( BTreeMap::new(), | mut acc, warn |
    {
      *acc.entry( warn.level ).or_default() += 1;
      acc
    });

    warns_counter.iter()
    .fold( String::new(), | acc, ( level, count ) |
    {
      format!
      (
        "{acc}S{level:?}:{count}, ",
        acc = acc,
        level = level,
        count = count
      )
    })
    .trim_end_matches( ", " ).to_owned()
  }

  pub fn wartings_detail_str( &self, min_dq_level : f32 ) -> String
  {
    let warns = self.warnings( min_dq_level );
    warns.iter()
    .fold( String::new(), | acc, value |
    {
      format!
      (
        "{acc}\n{warn:?}",
        acc = acc,
        warn = value
      )
    })
  }

  pub fn wartings_report_str( &self, min_dq_level : f32 ) -> String
  {
    format!
    (
      "Data Quality Report\n{}\n{}",
      self.warnings_summary_str( min_dq_level ),
      self.wartings_detail_str( min_dq_level )
    )
  }
}
