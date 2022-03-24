# !/bin/bash
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
args=$(getopt -a -o t:n: --long target:,name: -- "$@");

if [[ $? -ne 0 ]]; then
 echo "Invalid option";
 exit 2;
fi

eval set -- "$args"
while :
do
    case $1 in
     -n | --name) name=$2; shift 2;;
     -t | --target) target=$2; shift 2;;
     --) shift; break;;
     *) echo "Invalid option $1"; exit 2;;
    esac
done
#move files back to unprocessed folder
find "$target$name/original" -type f -exec mv -f {} "$target/unprocessed/" \;
#remove BQ dataset
python bin/bqm.py -a rollback -d "$name" -s "$target$name/schema" \
                  -p "${dict[project-id]}" -l "${dict[location]}" "nothing";
#remove bucket
gsutil rm -r "${dict[parent-bucket]}$name/";
#rm local directory
rm -r "$target/$name";