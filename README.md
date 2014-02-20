Sampling Users from Tweets and following their Timelines
=======================================

For anyone not on the team, the only script of interest is likely `fetch_timelines.py`, which just takes a generic list of twitter userids and fetches their timelines (and keeps them updated).

## Sampling Twitter Users from Tweets

The `pickusers.py` script takes files with a carefully formatted filename and format (the JSONStore files from Bill's Search scraper), that contains json tweets. It outputs unique users on the format `userid,location` for unique users.

Noticed that this script currently is hardcoded to read the location from the filename of the input file! Files are expected to be on the format.

The script is dependent on `streamusers.sh` to pick random JSON tweets from a tweet collection. 

Example usage:

    python pickusers.py ../JSONstore/2014-02-* > users.csv

## Picking Users Distributed Fairly Across UK

The `sample_subset_users.py` script takes a file with a list of twitter users (output of pickusers.py) as argument 1, and emits a subsample of users proportionate to the population of uk cities (all hardcoded, see the file).

Example usage:
	
    python sample_subset_users.py users.csv > uk_sampled_users.csv

Note: usersample.csv must be avilable in the same folder!

## Fetching Timelines

The `fetch_timelines.py` takes a file of user ids (one per line), and fetches their timelines. 

Rerun the script to update the timelines (fetch newer tweets). 

Will skip users if the expected amount of new tweets is low, so remember that you never get a "completely updated" list of tweets. Keep running the script to keep the tweets collection reasonably updated.

The script uses app auth, and will automatically wait before continuing when hitting the rate limit.

Example usage:

    python fetch_timelines.py uk_sampled_users.csv APPID APPSECRET ACCESSTOKEN TOKENSECRET