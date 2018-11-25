#!/bin/bash
echo "[" > "submission-result.json"
files=(./submision-result/*.json)
for result in "${files[@]::${#files[@]}-1}" ; do
    cat "$result" >>  "submission-result.json"
	echo "," >> "submission-result.json"
done
cat "${files[@]: -1:1}" >>  "submission-result.json"
echo "]" >> "submission-result.json"