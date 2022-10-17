use polars ::prelude::*;
use std::collections::HashMap;

pub trait Summary
{
  fn summarize( &self ) -> Self;
}

impl Summary for DataFrame
{
  fn summarize( &self ) -> Self
  {
    fn describe_cast( df: &DataFrame ) -> DataFrame
    {
      let mut columns: Vec< Series > = vec![];

      for s in df.get_columns().iter()
      {
        columns.push( s.cast( &DataType::Float64 ).expect( "cast to float failed" ) );
      }

      DataFrame::new( columns ).unwrap()
    }

    fn count( df: &DataFrame ) -> DataFrame
    {
      df.clone().lazy().select([ all().count() ]).collect().unwrap()
    }

    fn perc_missing( df : &DataFrame ) -> DataFrame
    {
      let nuls = df.null_count();
      let all = df.height();
      
      nuls.iter().map( | nul | nul.cast( &DataType::Float64 ).unwrap() / ( all as f64 ) ).collect()
    }

    fn perc_zeros( df : &DataFrame ) -> DataFrame
    {
      num_zeros( df ).iter().zip( count( df ).iter() )
      .map( |( zero, count )|
      {
        zero.cast( &DataType::Float64 ).unwrap() / count.cast( &DataType::Float64 ).unwrap()
      }).collect()
    }

    fn num_zeros( df : &DataFrame ) -> DataFrame
    {
      df.clone().lazy().select([ all().to_float().eq( 0 ) ]).collect().unwrap().sum()
    }

    fn num_negative( df : &DataFrame ) -> DataFrame
    {
      df.clone().lazy().select([ all().to_float().lt( 0 ) ]).collect().unwrap().sum()
    }

    // let percentiles = percentiles.unwrap_or(&[0.25, 0.5, 0.75]);

    let data = HashMap::from(
    [
      ( "perc_missing", perc_missing( self ) ),
      ( "perc_zeros", perc_zeros( self ) ),
      ( "num_negative", num_negative( self ) ),
      ( "num_zeros", num_zeros( self ) ),
      ( "count", count( self ) ),
      ( "min", self.min() ),
      ( "median", self.median() ),
      ( "mean", self.mean() ),
      ( "max", self.max() ),
    ]);

    let headers = data.keys().collect::< Vec< _ > >();

    // let __headers = vec!
    // [
    //   "perc_missing",
    //   "perc_zeros",
    //   "num_negative",
    //   "num_zeros",
    //   "perc_negative",
    //   "perc_distinct",
    //   "num_low_3x_IQR_outliers",
    //   "num_high_3x_IQR_outliers",
    //   "num_low_10x_IQR_outliers",
    //   "num_high_10x_IQR_outliers",
    //   "count",
    //   "n_unique",
    //   "decimal_col",
    //   "perc_most_freq",
    //   "val_most_freq",
    //   "min",
    //   "p05",
    //   "p25",
    //   "median",
    //   "mean",
    //   "p75",
    //   "p95",
    //   "max",
    //   "dtype",
    //   "skew",
    // ];

    let tmp = data.values().into_iter()
    .map( | value | describe_cast( value ).lazy() )
    .collect::< Vec< _ > >();

    // for p in percentiles {
    //     tmp.push(describe_cast(
    //         &self
    //             .quantile(*p, QuantileInterpolOptions::Linear)
    //             .expect("quantile failed"),
    //     ));
    //     headers.push(format!("{}%", *p * 100.0));
    // }

    let col_names = self.get_column_names();
    let summary = concat( &tmp, true, true ).unwrap();

    let mut summary = summary
    .collect().unwrap()
    .transpose().unwrap();

    summary.set_column_names( &headers )
    .expect( "insert of header failed" );
    
    summary
    .insert_at_idx( 0, Series::new( "Fields", col_names ) ).unwrap()
    .to_owned()
  }
}
