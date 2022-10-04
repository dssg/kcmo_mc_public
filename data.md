# The Data

## Data Sources

We received data from two systems:

- **court data:** Extracted from REJIS, the state's criminal justice data
  contractor. These data were extracted via Crystal Reports and sent as csvs.
  Data from parking ticket cases were excluded from the extract.
- **probabtion case management records:** Exctracted from Community CareLink
  (CCL), these contain data entered by probation officers for people on SES
  supervised probation or in specialty courts. These data cover fewer than 1000
  people and do not cover individuals on SIS probation and therefore were
  deprioritized for use in the machine learning pipeline.

## Data Loading

Raw data are uploaded to the database using
[load_data.py](infrastructure/load_data.py). This file can be run by calling:

```bash
python -m infratructure.load_data <type> <schema>
```

For the `type`, pass `court` or `ccl`. All of the files in the relevant folder
(configured in [src/utils/constants.py](src/utils/constants.py) (and
subdirectories for court data) will be loaded as tables into the given schema.
The remainder of the pipeline uses `raw_court` and `raw_ccl`, so test runs
should send data to an alternate schema and runs intended to load the data into
the pipeline should load into one of those schemas.

The following data files have badly formatted lines:

- DSSGCode_Bnd_Type.csv
- Assoc_Data_fixed.csv
- Assoc_Data6222022_fixed.csv
- DSSGAssoc_data_noprob_a_fixed.csv

As of 2022-08-11, these lines are skipped when uploading the files.

## Court Data

The court data extracted through Crystal Reports includes multiple files per
source table for a few different reasons:

- Many tables were sent separately for cases with probation assigned and those
  without probation. Often, the first version of a file contains probations
  cases only and the second version of the file has `NoProb` in the name,
  indicating that the file contains records only for cases without probations.
  However, some files have the reverse pattern, indicating probations data with
  `Prob` and having no signal for cases without probations.
- The no probations data were often too large for Crystal Reports to export as a
  single file and therefore are broken down into two or more files, typically
  indicated as `A`, `B`, and (in one case) `C`. There is at least one example
  of these being `part_1` and `part_2`.
- Often, files were missing information like foreign keys and updated versions
  were sent. Typivally, updated data have a date in MDDYYYY format, for example,
  `6222022`.

There are also several files with the suffix `_fixed`, indicating that these
are files we processed upon receipt because the originally shared version was
exported in a way that column headers were repeated on each row, rather than as
the first line of the file. Where we recognized this upon receipt of the data,
only the fixed version was uploaded to the server, but some files were fixed
only after initially loading them to the database and for those, you may see
both the fixed and original versions on the server.

You will find files with any combination of the above (probations + no
probations, multiple parts, multiple versions, and unfixed and fixed versions)
for the same source table.

Our primary source tables are for the following dimensions:

- Case Records
- Names (demographics)
- Charges
- Charge Dispositions
- Ordinance Violations

## Case Records

Case records data contain properties of the cases and are recorded in two files
that are loaded to two tables in the `raw_court` schema:

| file name           | table name      |
| ------------------- | --------------- |
| caserec_fixed.csv   | caserec_fixed   |
| caserec_b_fixed.csv | caserec_b_fixed |

These files contain probation and non-probation cases mixed together.

## Names

The names dimension includes details of the defendants such as name and
demographic information. It comes from 3 files that are loaded to three tables
in the `raw_court` schema.

| file name            | table name       |
| -------------------- | ---------------  |
| DSSGName.csv         | dssgname         |
| DSSGNameNoProb A.csv | dssgnamenoprob_a |
| DSSGNameNoProb B.csv | dssgnamenoprob_b |

## Charge Dispositions

We received 3 files containing charge disposition data from the court that are
loaded to 3 tables in the `raw_court` schema.

| file name                      | table name                 |
| ------------------------------ | -------------------------- |
| DSSGChargeDispNoProb_fixed.csv | dssgchargedisp_fixed       |
| DSSGChargeDisp_fixed.csv       | dssgchargedispnoprob_fixed |

## Charges

We received 3 files containing charge data from the court that are loaded to 3
tables in the `raw_court` schema.

| file name                          | table name                     |
| ---------------------------------- | ------------------------------ |
| DSSGCharge8052022_part_1_fixed.csv | dssgcharge8052022_part_1_fixed |
| DSSGCharge8052022_part_2_fixed.csv | dssgcharge8052022_part_2_fixed |
| DSSGCharge6222022.csv              | dssgcharge6222022              |
