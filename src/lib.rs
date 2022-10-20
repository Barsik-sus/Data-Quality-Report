use std::collections::BTreeMap;

use derive_builder::*;
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


#[ derive( Debug, Default, Builder ) ]
#[ builder
(
    custom_constructor,
    create_empty = "empty",
    build_fn( private, name = "fallible_build" )
)]
pub struct DataQualityReport
{
  #[ builder( setter( custom ) ) ]
  df : DataFrame,
  #[ builder( default = "\"active_date\".to_owned()" ) ]
  #[ builder( setter( into ) ) ]
  missing_by : String, // ! Don't work. Look at src/utils.rs:146
  #[ builder( default = "FEATURE_RULES.clone()" ) ]
  rules : Vec< DataQualityRule >,
  #[ builder( setter( custom ) ) ]
  #[ builder( default = "None" ) ]
  max_rows : Option< usize >,
  #[ builder( setter( skip ), default = "self.summary()" ) ]
  summary_df : DataFrame,
}

impl DataQualityReportBuilder
{
  pub fn new( df : DataFrame ) -> DataQualityReportBuilder
  {
    DataQualityReportBuilder
    {
      df : Some( df ),
      ..DataQualityReportBuilder::empty()
    }
  }

  pub fn max_rows( &mut self, max_rows : usize ) -> Self
  {
    let num_rows = self.df.as_ref().unwrap().height();
    if num_rows > max_rows
    {
      self.max_rows = Some( Some( max_rows ) );
      println!
      (
        "DataFrame has {num_rows} raws, sampling {max_rows} to reduce latency. Specify `max_rows=None` to disable.",
        max_rows = max_rows
      );
      self.df = self.df.as_ref().map( | df | df.sample_n( max_rows, false, false, None ).unwrap() );
      log::info!( "==sampled df==\n{:#?}", &self.df );
    }
    self.to_owned()
  }

  fn summary( &self ) -> DataFrame
  {
    self.df.as_ref().unwrap().summarize( self.missing_by.as_ref().unwrap().to_owned() )
  }

  pub fn build( &mut self ) -> DataQualityReport
  {
    self.fallible_build()
    .expect( "Can not build DataQualityReport" )
  }
}

impl DataQualityReport
{
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

    let fields = rows.select_at_idx( 0 ).unwrap().utf8().expect( "Something wrong with field names" ).into_iter();

    fields
    .map( | row |
    {
      let row = row.unwrap();
      // collect field names into vec
      let rule_fields = rule.fields.iter().map( | rule | col( rule ) ).collect::< Vec< _ > >();
      // select values
      let data_row = rows.clone().lazy()
      .filter( col( "Fields" ).eq( lit( row ) ) )
      .select( rule_fields ).collect().unwrap();
      // collect fields names with values
      let fields_values = data_row.get_row( 0 ).0
      .iter().zip( rule.fields.iter() )
      .map( |( value, name )| format!( "{name}: {value}" )).collect::< Vec< _ > >();
      DataQualityWarning
      {
        level : rule.level,
        field : row.to_owned(),
        msg : format!
        (
          "{msg}{fields}",
          msg = rule.msg.to_owned().map( | s | format!( "{s} " ) ).unwrap_or_default(),
          fields = fields_values.join( ", " ),
        )
      }
    }).collect()
  }

  pub fn warnings_summary_str( &self, min_dq_level : f32 ) -> String
  {
    let warns = self.warnings( min_dq_level );
    if warns.is_empty() { return "No Warnings".to_string() }

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

  pub fn warnings_detail_str( &self, min_dq_level : f32 ) -> String
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

  pub fn warnings_report_str( &self, min_dq_level : f32 ) -> String
  {
    format!
    (
      "Data Quality Report\n{}\n{}",
      self.warnings_summary_str( min_dq_level ),
      self.warnings_detail_str( min_dq_level )
    )
  }
}
