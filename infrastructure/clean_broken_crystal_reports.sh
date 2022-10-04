#!/bin/bash

# This script fixes files that have on each row the name of the columns as well as their values. The goal is to
# create a clean csv file with first row with column name (as headers) and then all the rows with just values
# for those columns

#files to fix, you will need to have a specific directory with the files to fix and MUST have not spaces in its names
dir_files=$1

for file in $(ls "$dir_files")
do
  # shellcheck disable=SC2086
  echo "working on: $file"

  # file with complete path
  file_w_path="$dir_files/$file"
  # get original name of file without extension
  filename_part1=$(echo $file | cut -d"." -f 1)
  # creates name of file w/ fixed data
  filename_part2="_fixed.csv"
  filename="$dir_files/$filename_part1$filename_part2"

  # some files have amounts with commas that we need to remove
  ## amounts with xxx,xxx,xxx
  sed -E -i 's/("[0-9]+),([0-9]+),([0-9]+")/\1\2\3/g' "$file_w_path"
  ## amounts with xxx,xxx
  sed -E -i 's/("[0-9]+),([0-9]+")/\1\2/g' "$file_w_path"

  # Since each row have colum names and values it must have an even number of fields (since for each row you have
  # a value), that's why in order to get from which field to which field do we have column names and from which field
  # do we have values we dived the total number of fields by 2.
  # get number of real columns for this file
  num_cols="$(head -1 "$file_w_path" | awk -F, '{print NF/2}')"
  # get field where data begins
  from_field=$(expr "$num_cols" + 1)
  # get field where data ends
  to_field=$(expr "$num_cols" + "$num_cols")

  # start wiht header
  echo " fixing data..."
  head -1 "$file_w_path" | cut -d "," -f 1-"$num_cols" > "$filename"
  cut -d"," -f "$from_field"-"$to_field" "$file_w_path" >> "$filename"
  echo " done fixind data :)"
  original_num_lines=$(wc -l "$file_w_path" | cut -d" " -f 1)
  new_num_lines=$(wc -l "$filename" | cut -d" " -f 1)
  echo " checking number of lines in files, original file: $original_num_lines, new fixed file: $new_num_lines"
done