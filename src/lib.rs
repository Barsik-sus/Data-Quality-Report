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
      condition : col( "perc_missing" ).gt( 0.0 ),
      fields : vec![ "perc_missing".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 0,
      condition : col( "dtype" ).eq( "object" ),
      fields : vec![ "dtype".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 0,
      condition : col( "n_unique" ).eq( 0 ),
      fields : vec![ "n_unique".to_owned(), "mean".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 0,
      condition : col( "num_missing_partitions" ).gt( 0.0 ),
      fields : vec![ "num_missing_partitions".to_owned(), "min_missing_partition".to_owned(), "max_missing_partition".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 1,
      condition : col( "perc_negative" ).gt( 0.0 ).and( col( "perc_negative" ).lt( 0.05 ) ),
      fields : vec![ "perc_negative".to_owned(), "num_negative".to_owned(), "min".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 1,
      condition : col( "n_unique" ).gt( 30 ).or( col( "decimal_col" ) ).and( col( "perc_most_freq" ).gt( 0.4 ) ),
      fields : vec![ "perc_most_freq".to_owned(), "val_most_freq".to_owned() ],
      msg : Some( "High percentage of a single value".to_owned() )
    },
    DataQualityRule
    {
      level : 2,
      condition : col( "perc_zeros" ).gt( 0.5 ),
      fields : vec![ "perc_zeros".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 2,
      condition : col( "num_low_3x_IQR_outliers" ).gt( 0.0 ),
      fields : vec![ "num_low_3x_IQR_outliers".to_owned(), "num_low_10x_IQR_outliers".to_owned(), "min".to_owned(), "p05".to_owned(), "p25".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 2,
      condition : col( "num_high_3x_IQR_outliers " ).gt( 0.0 ),
      fields : vec![ "num_high_3x_IQR_outliers".to_owned(), "num_high_10x_IQR_outliers".to_owned(), "p75".to_owned(), "p95".to_owned(), "max".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 2,
      condition : col( "skew" ).abs().gt( 0.5 ),
      fields : vec![ "skew".to_owned() ],
      msg : Some( "Data is skewed - maybe try a transformation (log?)".to_owned() )
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
    DataQualityRule
    {
      level : 0,
      condition : col( "n_unique" ).eq( 1 ),
      fields : vec![ "n_unique".to_owned(), "mean".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 1,
      condition : col( "perc_distinct" ).gt( 0.99 ).and( col( "perc_distinct" ).lt( 1.0 ) ),
      fields : vec![ "perc_distinct".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 1,
      condition : col( "perc_negative" ).gt( 0.0 ).and( col( "perc_negative" ).lt( 0.05 ) ),
      fields : vec![ "perc_negative".to_owned(), "num_negative".to_owned(), "min".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 2,
      condition : col( "perc_zeros" ).gt( 0.5 ),
      fields : vec![ "perc_zeros".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 2,
      condition : col( "dtype" ).eq( "object" ),
      fields : vec![ "dtype".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 2,
      condition : col( "perc_missing" ).gt( 0.5 ).and( col( "perc_missing" ).lt_eq( 0.95 ) ),
      fields : vec![ "perc_missing".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 2,
      condition : col( "n_unique" ).gt( 30 ).or( col( "decimal_col" ) ).and( col( "perc_most_freq" ).gt( 0.4 ) ),
      fields : vec![ "perc_most_freq".to_owned(), "val_most_freq".to_owned() ],
      msg : Some( "High percentage of a single value".to_owned() )
    },
    DataQualityRule
    {
      level : 3,
      condition : col( "num_low_10x_IQR_outliers" ).gt( 0.0 ),
      fields : vec![ "min".to_owned(), "p05".to_owned(), "p25".to_owned(), "num_low_10x_IQR_outliers".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 3,
      condition : col( "num_high_10x_IQR_outliers" ).gt( 0.0 ),
      fields : vec![ "num_high_10x_IQR_outliers".to_owned(), "p75".to_owned(), "p95".to_owned(), "max".to_owned() ],
      msg : None
    },
    DataQualityRule
    {
      level : 3,
      condition : col( "perc_missing" ).gt( 0.0 ).and( col( "perc_missing" ).lt_eq( 0.5 ) ),
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

#[ derive( Debug, Default ) ]
pub struct DataQualityReportBuilder
{
  df : DataFrame,
  missing_by : Option< String >,
  rules : Option< Vec< DataQualityRule > >,
  max_rows : Option< usize >,
}

impl DataQualityReportBuilder
{
  pub fn new( df : DataFrame ) -> Self { Self { df, ..Default::default() } }
  pub fn missing_by< S >( mut self, missing_by : S ) -> Self
  where
    S : Into< String >
  { self.missing_by = Some( missing_by.into() ); self }
  pub fn rules( mut self, rules : Vec< DataQualityRule > ) -> Self { self.rules = Some( rules ); self }
  pub fn max_rows( mut self, max_rows : usize ) -> Self { self.max_rows = Some( max_rows ); self }
  pub fn setup( mut self ) -> DataQualityReport
  {
    let num_rows = self.df.height();
    if !self.max_rows.is_none() && num_rows > self.max_rows.unwrap()
    {
      println!( "DataFrame has {num_rows} raws, sampling {max_rows} to reduce latency. Specify `max_rows=None` to disable.", max_rows = self.max_rows.unwrap() );
      self.df = self.df.sample_n( self.max_rows.unwrap(), false, false, None ).unwrap()
    }

    let missing_by = self.missing_by.unwrap_or( "active_date".to_owned() );

    log::info!( "==Rules==\n{:?}", self.rules );
    let summary_df = self.df.summarize( missing_by.to_owned() );
    log::info!( "==summarized df==\n{:#?}", &summary_df );

    let file = std::fs::File::create( "./summarized_data.csv" ).unwrap();
    let mut to_write = summary_df.clone();
    CsvWriter::new( file ).finish( &mut to_write ).unwrap();

    DataQualityReport
    {
      df : self.df,
      missing_by,
      rules : self.rules.unwrap_or( FEATURE_RULES.clone() ),
      summary_df,
    }
  }
}

impl DataQualityReport
{
  pub fn new( df : DataFrame ) -> DataQualityReportBuilder
  {
    DataQualityReportBuilder::new( df )
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
    let rows = match self.summary_df.clone().lazy()
    .filter( rule.condition.to_owned() )
    .collect()
    {
      Ok( rows ) => rows,
      _ =>
      {
        log::error!( "Rule : {:?} can not be used.\nMay be some columns needed for this rule is not expected", rule );
        return vec![]
      }
    };
    // could be optimized

    log::info!( "rule : {rule:?}\ndata :\n{rows:#?}" );

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
