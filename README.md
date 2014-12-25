# elasticsearch-push
==================

It is a simple Python 3 based push script powered by Amazon SNS and ElasticSearch

## How to Use It

First, you need to register an valid SNS either for Google Play or for Apple IOS

Second, install python3 and AWS boto package, as well as the elasticsearch library.

1. sudo apt-get install python3 python3-pip

2. sudo pip3 install boto

3. sudo pip3 install elasticsearch


## How to query ElasticSearch

First, we query all users who registered yesterday

curl -XGET 'http://<hostname>:9200/<index>/_search?pretty=true' -d '
    {
        "filter": {
            "not": {
                "term": {
                    "tags": "_grokparsefailure"
                }
            }
        },
        "query": {
            "match": {
                "name": "x"
                }
        }
    }
'
