#!/usr/bin/env bash
## run this script to connect locally to the
## you will need a wikitech account, and need to copy the replica.my.cnf file locally
ssh -N maximilianklein@tools-dev.wmflabs.org -L 3310:enwiki.analytics.db.svc.eqiad.wmflabs:3306
