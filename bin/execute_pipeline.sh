#!/bin/bash
###Getting keys:
keys_file=gcp.keys; #swap it for dummy.keys
declare -a info=$(< $keys_file);
delimiter=':';
declare -A dict;
for element in ${info[@]};
do
 dict[$(echo $element | cut -d $delimiter -f 1)]=$(echo $element | cut --complement -d $delimiter -f 1);
done

###Processing options:
target=datasets/;
args=$(getopt -a -o t:n:f:p: --long target:,name:,file:,pipeline: -- "$@");

if [[ $? -ne 0 ]]; then
 echo "Invalid option";
 exit 2;
fi

eval set -- "$args"
while :
do
    case $1 in
     -f | --file) file_path=$2; shift 2;;
     -n | --name) name=$2; shift 2;;
     -p | --pipeline) pipeline=$2; shift 2;;
     -t | --target) target=$2; shift 2;;
     --) shift; break;;
     *) echo "Invalid option $1"; exit 2;;
    esac
done

#Files and directories manipulation:
if [[ ! -e $file_path ]]; then
 echo "$file_path does not exist";
 exit 2;
fi

if [[ ! -d "$target$name" ]]; 
then
 mkdir -p "$target$name/original";
 mkdir -p "$target$name/cleaned";
 mkdir -p "$target$name/schema";
fi

cp $file_path "$target$name/original/";

#Clean data:
declare -a csvs=$(python bin/datacleaner.py \
                  -f "$file_path" -d "$target$name/cleaned/" \
                  -p "${pipeline:-$name}" -s "$target$name/schema/" );

#Upload to cloud storage:
for csv in ${csvs[@]};
do
 gsutil cp $csv "${dict[parent-bucket]}$name/";
done

#Upload to bigquery
declare -a uris=$(gsutil ls "${dict[parent-bucket]}$name/");

python bin/bqm.py -a commit -d "$name" -s "$target$name/schema" \
                  -p "${dict[project-id]}" -l "${dict[location]}" ${uris[@]};
