Calcite does a good job of breaking down the query into multiple stages,
so the aggregation stage is really just aggregating.
For example, Calcite converts the query:

```
SELECT a+1, sum(b+1)*2
FROM t
```

into

```
SELECT $1*2
FROM (SELECT $1, sum($2)
      FROM (SELECT a+1, b+1 FROM t))
```

No matter what strategy we use to do the aggregation, we always have an incomplete input program:

```
for (row in input)
  ?
```

and an incomplete output program:

```
doSomething(?)
```

There are 3 basic strategies for implementing an aggregation:

# Hash-aggregate

We build a table of aggregated rows, then flush them to the output program:

```
table = {}

for (row in input)
  table[row.groupBy] += row.sum

for (key in table)
  doSomething(table[key])
```

This strategy only requires a single step, but it loads the entire output of the aggregate into memory.

# Streaming-aggregate

Assuming the data is sorted, there is a streaming approach to aggregation:

```
first = false
groupBy = null
sum = null

for (row in input)
  if (first)
    groupBy = row.groupBy
    sum = row.sum
  else if (groupBy != row.groupBy)
    doSomething(sum)
    groupBy = row.groupBy
    sum = row.sum
  else
    sum += row.sum

doSomething(sum)
```

There are other reasons to [sort the data during the shuffle step](http://blog.cloudera.com/blog/2015/01/improving-sort-performance-in-apache-spark-its-a-double/).
The first / last logic is a bit convoluted, and duplicates the output stage `doSomething(sum)`,
which might be a very large expression.
If the input were a "pull" iterator rather than a "push" for-loop, this would be easy:

```
input.next()

while (true)
  groupBy = input.value.groupBy
  sum = input.value.sum

  while (groupBy == input.value.groupBy)
    sum += input.value.sum
    if (!input.next())
      return

  doSomething(sum)
```

It's possible to convert a "push" process into a "pull" iterator by using two threads and backpressure:

```
iterator = newIterator()
for (row in input)
  iterator.row = row
  iterator.wait()
...
```

All these wait() / notify() calls might be costly though.