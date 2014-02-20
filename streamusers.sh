#!/usr/bin/env bash

#send stderr to dev null to avoid it complaining about head breaking the pipe
lzop -dcf $1 2> /dev/null | shuf 2> /dev/null | head -n 1000