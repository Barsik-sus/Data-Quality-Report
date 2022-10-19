use polars ::prelude::*;

pub trait Summary
{
  fn summarize( &self, missing_by : String ) -> Self;
}

impl Summary for DataFrame
{
  fn summarize( &self, missing_by : String ) -> Self
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

    fn perc_negative( df : &DataFrame ) -> DataFrame
    {
      num_negative( df ).iter().zip( count( df ).iter() )
      .map( |( negative, count )|
      {
        negative.cast( &DataType::Float64 ).unwrap() / count.cast( &DataType::Float64 ).unwrap()
      }).collect()
    }

    fn n_unique( df : &DataFrame ) -> DataFrame
    {
      df.clone().lazy().select([ all().n_unique() ]).collect().unwrap()
    }

    fn mode_value( df : &DataFrame ) -> DataFrame
    {
      df.clone().lazy().select([ all().mode() ]).collect().unwrap()
    }

    fn perc_mode_value( df : &DataFrame ) -> DataFrame
    {
      df.clone().lazy().select([ all().filter( all().eq( all().mode() ) ).sum() ]).collect().unwrap()
      .iter().zip( count( df ).iter() )
      .map( |( unique, count )|
      {
        unique.cast( &DataType::Float64 ).unwrap() / count.cast( &DataType::Float64 ).unwrap()
      }).collect()
    }

    fn perc_distinct( df : &DataFrame ) -> DataFrame
    {
      n_unique( df ).iter().zip( count( df ).iter() )
      .map( |( unique, count )|
      {
        unique.cast( &DataType::Float64 ).unwrap() / count.cast( &DataType::Float64 ).unwrap()
      }).collect()
    }

    fn quantile( df : &DataFrame, p : f64 ) -> DataFrame
    {
      df
      .quantile( p, QuantileInterpolOptions::Linear )
      .expect( "quantile failed" )
    }

    fn skew( df : &DataFrame ) -> DataFrame
    {
      df.clone().lazy().select([ all().skew( true )]).collect().unwrap()
    }

    let data = 
    [
      ( "perc_missing", perc_missing( self ) ),
      ( "perc_zeros", perc_zeros( self ) ),
      ( "num_negative", num_negative( self ) ),
      ( "num_zeros", num_zeros( self ) ),
      ( "perc_negative", perc_negative( self ) ),
      ( "perc_distinct", perc_distinct( self ) ),
      // ( "num_low_3x_IQR_outliers", tmp( self ) ),
      // ( "num_high_3x_IQR_outliers", tmp( self ) ),
      // ( "num_low_10x_IQR_outliers", tmp( self ) ),
      // ( "num_high_10x_IQR_outliers", tmp( self ) ),
      ( "count", count( self ) ),
      ( "n_unique", n_unique( self ) ),
    //   ( "decimal_col", tmp( self ) ),
      // ( "perc_most_freq", perc_mode_value( self ) ), // ! filter * not allowed
      ( "val_most_freq", mode_value( self ) ),
      ( "min", self.min() ),
      ( "p05", quantile( self, 0.05 ) ),
      ( "p25", quantile( self, 0.25 ) ),
      ( "median", self.median() ),
      ( "p75", quantile( self, 0.75 ) ),
      ( "p95", quantile( self, 0.95 ) ),
      ( "mean", self.mean() ),
      ( "max", self.max() ),
    //   ( "dtype", tmp( self ) ),
      ( "skew", skew( self ) ),
    ];

    let ( headers, tmp ) = data.iter()
    .fold( ( vec![], vec![] ),
    | ( mut headers, mut frames ), ( header, frame ) |
    {
      headers.push( header );
      frames.push( describe_cast( frame ).lazy() );
      ( headers, frames )
    });

    let col_names = self.get_column_names();
    let summary = concat( &tmp, true, true ).unwrap();
    // let missing_ds = if col_names.contains( &missing_by.as_str() )
    // {
    //   summary.clone().groupby([ col( missing_by.as_str() ) ])
    //   .agg(
    //   [
    //     col( &missing_by ).min().suffix( "_min" ),
    //     col( &missing_by ).max().suffix( "_max" ),
    //     col( &missing_by ).sum().suffix( "_sum" ),
    //   ])
    // }
    // else
    // {
    //   LazyFrame::default()
    // }.collect().unwrap();
    // log::info!( "==missing data set==\n{missing_ds:#?}" );
    //? This is written with Python
    //? if self.missing_by is not None and self.missing_by in self.df.columns:
    //?          missing_ds = self.df.groupby(self.missing_by).count() == 0
    //?      else:
    //?          missing_ds = pd.DataFrame()
    //?      missing_df_summary = pd.DataFrame(
    //?          {
    //?              "min_missing_partition": missing_ds.aggregate(lambda x: x.index[x].min()),
    //?              "max_missing_partition": missing_ds.aggregate(lambda x: x.index[x].max()),
    //?              "num_missing_partitions": missing_ds.sum(),
    //?          }
    //?      )
    //?     self._summary_df = pd.concat([summary_df_no_missing, missing_df_summary], axis=1)

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
