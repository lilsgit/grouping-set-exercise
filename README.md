# python-practice

## This repo contains simple coding excercise for the following questions: 

Please do same exercise using two different framework.

Framework 1. panda

Framework 2. apache beam python https://beam.apache.org/documentation/sdks/python/

Framework 3. pyspark


using two input files dataset1 and dataset2

join dataset1 with dataset2 and get tier

generate below output file:

legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), 
sum(value where status=ACCR)

Also create new record to add total for each of legal entity, counterparty & tier.
