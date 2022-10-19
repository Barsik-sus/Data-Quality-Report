# Questions
- jobtitle and employee_residence missing values [ [calls from][notebook] | [dataset][dataset] ]
- active date exists in [summary][notebook] but not in [dataset][dataset]
- active date into [summary][notebook] is nan in last row

# Add to summary DF
- [ ] impl num_*_IQR_outliers [ ~20h-60h ]
  - [ ] num_low_3x_IQR_outliers
  - [ ] num_hegh_3x_IQR_outliers
  - [ ] num_low_10x_IQR_outliers
  - [ ] num_high_10x_IQR_outliers

- [ ] decimal_col [ ~4h-10h ] - in [summary][notebook] all has False
- [ ] dtype [ ~4h-10h ]
- [ ] perc_most_freq [ ~10h-30h ] - need to find count of most frequency values for all columns and get the percentage in one query

# Implement
- [ ] [missing_by](https://github.com/doordash-oss/DataQualityReport/blob/main/dataqualityreport/dataqualityreport.py#L127) [ ~15h-40h ] - column in DataFrame to compute missing rates over

[notebook]: https://github.com/doordash-oss/DataQualityReport/blob/main/tutorial.ipynb
[dataset]: https://github.com/doordash-oss/DataQualityReport/blob/main/tests/ds_salaries.csv