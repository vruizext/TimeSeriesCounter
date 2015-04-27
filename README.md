## Redis Time Series Counter

This is a pet project I've built for didactic purpose, i.e. to learn how to use redis client for Python.
```RedisTSCounter``` allows to track counts of things along time, splitting time in buckets of a given size and get the
ranking of counts for the top N things in the list. When aggregating data from the different buckets, the default behaviour
is to assign same weight to all buckets. An exponential decay weighting can be used instead, to give more value to the
most recent events over the less recent. To see some usage examples, see ```TestCounter.py```
