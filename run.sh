#!/bin/bash

file_dir=$(dirname "${BASH_SOURCE[0]}")

tmp_path="${file_dir}/tmp"
source_result_path="${tmp_path}/source/lvr_landcsv.zip"
unzip_result_folder="${tmp_path}/unzip"

cd "${file_dir}"
cleanup() {
    rm -r $tmp_path
}
trap "cleanup" exit

# Init tmp folder
mkdir -p $(dirname "$source_result_path")
mkdir -p $unzip_result_folder

# Get source file
python3 crawler.py $source_result_path

# Unzip
unzip $source_result_path -d $unzip_result_folder

# remove first line (Chinese field name)
for file in ${unzip_result_folder}/*;
do
    sed -i '' 1d $file
done

# Start pyspark-notebook container
echo Starting pyspark-notebook container
cid=$(docker run -d --name pyspark_notebook -p 8888:8888 -v $(pwd):/home/jovyan jupyter/pyspark-notebook)

# Run preprocessing script
docker exec $cid python3 preprocessing.py

# Remove container
echo All Process completed, closing container
docker rm -f $cid