#!/usr/bin/env bash

# '980:BOOK | 960:21 -980:DELETED -980:PROCEEDINGS -960:42 -960:43'

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -m|--mode)
    MODE="$2"
    shift
    shift
    ;;
    -c|--chunk_size)
    CHUNK_SIZE="$2"
    shift
    shift
    ;;
    -f|--file_prefix)
    FILE_PREFIX="$2"
    shift
    shift
    ;;
    -h|--help)
    echo "Options:\n"
    echo "-m|--mode [dump | load]             \t mode for performing dump from legacy [required]"
    echo "-u|--username username              \t username for performing the dump (not required in read mode)"
    echo "-h|--help                           \t prints this message"
    echo "-q|--query 'legacy query'           \t query to select records on legacy. [Required] in dump mode"
    echo "-f|--file_prefix test_books         \t file name prefix for dumps. [Required] in dump mode"
    echo "-c|--chunk_size 100                 \t records per file, default is 100"
    echo "-d|--from-date '1970-01-01 00:00:00'\t optional"
    echo "-l|--limit                          \t number of records to dump"

    echo "EXAMPLE query: 980:BOOK | 960:21 -980:DELETED -980:PROCEEDINGS -960:42 -960:43"
    shift
    exit
    ;;
    -u|--username)
    USERNAME="$2"
    shift
    shift
    ;;
    -q|--query)
    QUERY="$2"
    shift
    shift
    ;;
    -d|--from-date)
    FROM_DATE='--from date '"$2"
    shift
    shift
    ;;
    -l|--limit)
    LIMIT='--limit '"$2"
    shift
    shift
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift
    ;;
  esac
done

echo $MODE
echo $CHUNK_SIZE
echo $FILE_PREFIX
echo $USERNAME
echo $QUERY
echo $FROM_DATE
echo $LIMIT

if [ "$MODE" == "dump" ] || [ "$MODE" == "load" ]
then
    if [ "$MODE" == "dump" ] && [ "$FILE_PREFIX" != "" ] && [ "$USERNAME" != "" ] && [ "$QUERY" != "" ]; then

        if [ "$CHUNK_SIZE" != "" ]; then
            echo 'Performing '$QUERY' dump on the legacy.... ('$CHUNK_SIZE' records)'
            ssh $USERNAME@cds-wn-04 "cd /eos/media/cds/test/books/migration/records/tmp/; /usr/bin/inveniomigrator dump records -q '$QUERY' --with-collections [--latest-only]" --file-prefix $FILE_PREFIX --chunk-size $CHUNK_SIZE $FROM_DATE $LIMIT
        else
            echo 'Performing '$QUERY' dump on the legacy.... (100 records)'
            ssh $USERNAME@cds-wn-04 "cd /eos/media/cds/test/books/migration/records/tmp/; /usr/bin/inveniomigrator dump records -q '$QUERY' --with-collections [--latest-only]" --file-prefix $FILE_PREFIX --chunk-size 100 $FROM_DATE $LIMIT
        fi
    elif [ "$MODE" == "load" ]; then
        echo 'Record analysis on books-migrator-dev...'
        STATUS=$(oc status 2>&1)
        if [[ $STATUS =~ .*Forbidden.* ]]
        then
            oc login openshift.cern.ch
        fi
        pod_name=`oc get pods --no-headers=true -o name | grep --invert-match build`
        pod_name=${pod_name#*/}
        echo 'Files dumped: \n'
        oc exec -c migrator-app $pod_name ls /eos/media/cds/test/books/migration/records/tmp/
        echo 'Which of the listed files would you like to choose for cleaning?'
        read file_name
        fpath='/eos/media/cds/test/books/migration/records/tmp/'$file_name
        echo $fpath
        oc exec -c migrator-app $pod_name migrator report dryrun $fpath
    else
        echo "MISSING REQUIRED PARAMS. Please use sh migrate.sh -h to see the manual"

    fi
else
    echo "Please specify the mode"
fi
