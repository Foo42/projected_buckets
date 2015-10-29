ProjectedBuckets
================

## Purpose
To provide way of creating processes for managing associative data structures which expose streams of changes. In addition these "buckets" can have views plugged in, which will asynchronously update projected versions of the bucket contents by running value changes through an (optional) supplied mapping function, and the resultant dictionary through an (optional) supplied reduce.

### Processes
