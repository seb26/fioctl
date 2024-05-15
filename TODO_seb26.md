* Finished rewriting to work with pathlib AND to gather items as a queue first
* Current challenge - They are uploading one by one - alter the exec_stream() so that it is handling more files concurrently

BUGS

* `--exclude-files='.DS_Store'` does not actually filter out those files - filters broken?