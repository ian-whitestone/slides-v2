## In the trenches with Spark

<img class="stretch" src="imgs/in-the-trenches-with-spark/cover.png" height="400px">

<!-- <p style="font-size:1px;"> -->
<br>
<br>
<p style="font-size:30px;">
  2021-11-02
  <br>
  Slides 👉 <a href='https://ianwhitestone.work/slides-v2/in-the-trenches-with-spark.html'>ianwhitestone.work/talks</a>
<p>
<!-- <p> -->

note: example speakr notes!


### Motivation

<li>Most of the time, Spark jobs just work </li>
<li class="fragment"> But when they don't, most people struggle to understand why and reason about a proper solution</li> 
<li class="fragment"> To fix or optimize a job, you need to understand how Spark works under the hood so you can think from first principles </li> 



### Agenda

<ul style="display: list-item">
  <li>Intro to Spark
    <ul>
      <li>Architecture overview</li>
      <li>Common terminology</li>
    </ul>
  </li>
  <li>Example Jobs
    <ul>
      <li>Aggregating transactions</li>
      <li>Enriching user event logs</li>
    </ul>
  </li>
  <li>Spark Web UI Overview</li>
</ul>


### Agenda (cont'd)

<ul style="display: list-item">
  <li>Spark join strategies
    <ul>
      <li>Broadcasting</li>
      <li>Salting</li>
      <li>Partial broadcast join</li>
    </ul>
  </li>
</ul>


### Agenda (cont'd)

<ul style="display: list-item">
  <li>Spark in the wild
    <ul>
      <li>Stack traces & broadcast timeouts</li>
      <li>Chunk that data: partitioning gotchas & strategies</li>
      <li>The curse of skew</li>
      <li>Avoiding the shuffle</li>
      <li>Other fun errors</li>
    </ul>
  </li>
</ul>

---

# Intro to Spark


## Spark?

* Open-source (Apache) framework for large-scale data analytics
* Available in multiple languages: Scala, Java, Python, R, SparkSQL
* Came after Hadoop/MapReduce, offering much faster performance since data is retained in memory
* Batch and stream based processing, machine learning library, graph data processing
  * We will only focus on **Batch processing** today


## Architecture overview

<img loading="lazy" src="imgs/in-the-trenches-with-spark/cluster-overview-1.png">


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/cluster-overview-2.png">


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/cluster-overview-3.png">


# Common terminology


* **Driver:** Responsible for orchestration of the job. *Generally* not interacting with the data
  * Converts the users program into tasks that run on the **executors**

* **Executor:** Think of it as a "single computer" (conceptually) with a single Java VM running Spark
  * Runs tasks across multiple threads (cores)
  * Called **container** internally in Starscream


* **Partition:** small chunk of a large distributed dataset

* **Task:** Unit of work that is run on a single partition, on a single executor

* **Transformation:** Anything that creates a new dataset (filter, map, sort, group by, join)

* **Action:** Anything that triggers the execution (count, collect, write, top, take)


#### Action vs. Transformation Example

<pre> 
  <code class="language-python"> 
  event_logs_df
  .filter(F.col('event_at') >= F.lit('2020-01-01'))
  .join(event_dimension_df, on='event_id')
  .select(['user_id', 'event_at', 'event_type'])
  .collect()
  </code>
</pre>

<p style="text-align: center">👇</p>

<pre> 
  <code class="language-python fragment"> 
  event_logs_df
  .filter( # transformation
    F.col('event_at') >= F.lit('2020-01-01')
  )
  .join( # transformation
    event_dimension_df, on='event_id'
  )
  .select( # transformation
    ['user_id', 'event_at', 'event_type']
  )  
  .collect() # action
  </code>
</pre>


* **Stage:** Collection of **transformations**. 
  * New stage is created whenever there is a **shuffle**
* **Job:** Collection of stages
* **Shuffle:** Mechanism for redistributing data so that it’s grouped differently across partitions
  * Required by sort-merge join, sort, groupBy, and distinct operators
  * Typically involves copying data across executors in a cluster
  * Complex & costly operation


# Putting it all together


<ul style="display: list-item">
  <li>Every Spark <b>“action”</b> triggers a <b>“job”</b>
    <ul>
      <li>transformation: creating new datasets → filter, map, sort, group by, join</li>
      <li>action: count, collect, top, take, write, etc.. triggers the execution</li>
    </ul>
  </li>
</ul>

<ul style="display: list-item" class="fragment">
  <li>Every job contains multiple <b>"stages"</b>
    <ul>
      <li>New <b>"stages"</b> are created when a <b>"shuffle" operation</b> is required (join, sort, groupby)</li>
    </ul>
  </li>
</ul>

<ul style="display: list-item" class="fragment">
  <li>A <b>"stage"</b> is a collection of <b>"transformations"</b></li>
</ul>

<ul style="display: list-item" class="fragment">
  <li>A <b>"stage"</b> divides the work into a number of <b>"tasks"</b>”</li>
</ul>

<ul style="display: list-item" class="fragment">
  <li><b>Tasks</b> run in parallel on <b>"executors"</b></li>
</ul>

---

## Example 1
### Aggregating Transactions by App


#### Aggregating Transactions by App (SQL)

<pre>
  <code class="language-sql stretch">
  WITH
  trxns_cleaned AS (
    SELECT
      CASE
        WHEN api_client_id=123 THEN 'A'
        WHEN api_client_id IN (456, 789) THEN 'B'
        ELSE 'C'
      END AS app_grouping,
      amount
    FROM
      transactions
    WHERE
      created_at >= TIMESTAMP'2020-01-01'
  )
  SELECT
      app_grouping,
      SUM(amount) AS amount_processed
  FROM
      trxns_cl
  GROUP BY 1
  </code>
</pre>


#### Aggregating Transactions by App (PySpark)

<pre>
  <code class="language-python stretch">
  trxns_cleaned = (
      df
      .filter(F.col('created_at') >= F.lit('2020-01-01'))
      .withColumn(
          'app_grouping', 
          F.when(F.col('api_client_id') == F.lit(123), 'A')
          .when(F.col('api_client_id').isin([456, 789]), 'B')
          .otherwise('C')
      )
  )

  output = (
      trxns_cleaned
      .groupBy('app_grouping')
      .agg(
          F.sum('amount').alias('amount_processed')
      )
      .select(['app_grouping', 'amount_processed'])
  )
  </code>
</pre>


<img loading="lazy" src="imgs/in-the-trenches-with-spark/example-1-full-split.png">


<img loading="lazy" src="imgs/in-the-trenches-with-spark/example-1-part-1.png">


<img loading="lazy" src="imgs/in-the-trenches-with-spark/example-1-part-1-w-data.png">


<img loading="lazy" src="imgs/in-the-trenches-with-spark/example-1-part-2.png">


<img loading="lazy" src="imgs/in-the-trenches-with-spark/example-1-part-2-w-executors.png">


<img loading="lazy" src="imgs/in-the-trenches-with-spark/example-1-part-2-w-data.png">


<img loading="lazy" src="imgs/in-the-trenches-with-spark/example-1-part-3.png">

---

## Example 2
### Get all user events in a timeframe


#### Get all user events in a timeframe (SQL)

<pre>
  <code class="language-sql stretch">
  WITH
  cleaned_logs AS (
    SELECT
      user_id,
      event_id,
      event_at
    FROM
      user_event_logs
    WHERE
      event_at >= TIMESTAMP'2020-01-01'
  )
  SELECT
      user_id,
      event_at,
      event_type
  FROM
      cleaned_logs
      INNER JOIN user_event_dimension
        ON cleaned_logs.event_id=event_dimension.event_id
  </code>
</pre>



#### Get all user events in a timeframe (PySpark)

<pre>
  <code class="language-sql python">
  output = (
      user_event_logs_df
      .filter(F.col('event_at') >= F.lit('2020-01-01'))
      .join(user_event_dimension_df, on='event_id')
      .select(['user_id', 'event_at', 'event_type'])
      .collect()
  )
  </code>
</pre>


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-2-no-broadcast.png">


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-2-no-broadcast-w-data.png">


#### Switching to a broadcast join

<pre>
  <code class="language-sql python" data-line-numbers="6">
  output = (
      user_event_logs_df
      .filter(F.col('event_at') >= F.lit('2020-01-01'))
      .join(
        F.broadcast(user_event_dimension_df), 
        on='event_id'
      )
      .select(['user_id', 'event_at', 'event_type'])
      .collect()
  )
  </code>
</pre>


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-2-with-broadcast.png">

---

# Spark Web UI

<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/spark-web-ui-0.png">


## Sample Data

Transactions

<table style="font-size:50%; margin-left: 5%; margin-right: 5%">
<thead>
  <tr>
    <th>transaction_id</th>
    <th>shop_id</th>
    <th>created_at</th>
    <th>currency_code</th>
    <th>amount</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>1</td>
    <td>123</td>
    <td>2021-01-01 12:55:01</td>
    <td>USD</td>
    <td>25.99</td>
  </tr>
  <tr>
    <td>2</td>
    <td>123</td>
    <td>2021-01-01 17:22:05</td>
    <td>USD</td>
    <td>13.45</td>
  </tr>
  <tr>
    <td>3</td>
    <td>456</td>
    <td>2021-01-01 19:04:59</td>
    <td>CAD</td>
    <td>10.22</td>
  </tr>
</tbody>
</table>

Shop Dimension

<table style="font-size:50%; margin-left: 5%; margin-right: 5%">
<thead>
  <tr>
    <th>shop_id</th>
    <th>shop_country_name</th>
    <th>shop_country_code</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>123</td>
    <td>Canada</td>
    <td>CA</td>
  </tr>
  <tr>
    <td>456</td>
    <td>United States</td>
    <td>US</td>
  </tr>
</tbody>
</table>


## Simulating skewness

<pre> 
  <code class="language-python"> 
  ids = np.round(
    1 + np.random.chisquare(0.35, size=10000)*100000
  )
  plt.hist(ids, bins=100);
  </code>
</pre>

<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-trxns-skew.png" >


<pre> 
  <code class="language-bash"> 
  >>> pd.Series(ids).describe()
  count    1.000000e+04
  mean     3.370065e+04
  std      8.156347e+04
  min      1.000000e+00
  25%      4.600000e+01
  50%      2.676000e+03
  75%      2.833600e+04
  max      1.702097e+06

  >>> 100.0*ids[ids == 1].shape[0] / ids.shape[0] 
  11.19
  </code>
</pre>


<pre class="stretch"> 
  <code class="language-python stretch"> 
  N = 6500000 # 6.5 million rows

  currencies = ['USD', 'CAD', 'EUR', 'GBP', 'DKK', 'HKD']
  currency_probas = [0.8, 0.02, 0.1, 0.05, 0.015, 0.015]

  df = pd.DataFrame({
    'transaction_id': np.arange(1, N + 1),
    'shop_id': np.round(
      1 + np.random.chisquare(0.35, size=N)*100000
    ),
    '_days_since_base': np.random.randint(0, 10, size=N),
    'currency_code': np.random.choice(
      currencies, size=N, p=currency_probas
    ),
    'amount': np.random.exponential(50, size=N)
  })

  df['base_date'] = datetime(2016, 1, 1)
  days = pd.TimedeltaIndex(df['_days_since_base'], unit='D')
  df['created_at_date'] = df.base_date + days
  </code>
</pre>


## Example job (SQL)

<pre class="stretch"> 
  <code class="language-sql stretch"> 
  SELECT
    sd.shop_country_code,
    trxns.created_at_date,
    MAX(amount) AS max_transaction_value
  FROM
    transactions AS trxns
    INNER JOIN shop_dimension AS sd
      ON trxns.shop_id=sd.shop_id
  GROUP BY 1,2
  </code>
</pre>


## Example job (PySpark)

<pre class="stretch"> 
  <code class="language-python stretch"> 
  output = (
      trxns_skewed_df
      .join(shop_df, on='shop_id')
      .groupBy('shop_country_code', 'created_at_date')
      .agg(
          F.max('amount').alias('max_transaction_value')
      )
  )

  result = output.collect()
  </code>
</pre>

**Pop quiz:** how many stages will there be?


<img loading="lazy" src="imgs/in-the-trenches-with-spark/spark-web-ui-3.png">


<img loading="lazy" src="imgs/in-the-trenches-with-spark/spark-web-ui-4.png">


<img loading="lazy" src="imgs/in-the-trenches-with-spark/spark-web-ui-5-1.png">


<img loading="lazy" src="imgs/in-the-trenches-with-spark/spark-web-ui-5-2.png">


<img loading="lazy" src="imgs/in-the-trenches-with-spark/spark-web-ui-5-3.png">


<img loading="lazy" src="imgs/in-the-trenches-with-spark/spark-web-ui-6.png">


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/spark-web-ui-7-1.png">


### Hover over to see more info

<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/spark-web-ui-7-2.gif">


<img loading="lazy" src="imgs/in-the-trenches-with-spark/spark-web-ui-8.png">


### Inspecting the plan

<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/spark-physical-plan-1.png">


### Inspecting the plan
<li>See how the Spark optimizer has planned to execute your job</li>
<li>Understand what join strategies are being used</li>
<ul>
      <li>did Spark decide to broadcast something?</li>
</ul>
<li>See what filters have been pushed down to Parquet (more efficient)</li>



### Alternatively, you can get the physical plan by calling `.explain()`

```python
  output = (
      trxns_skewed_df
      .join(shop_df, on='shop_id')
      ...
  )
  output.explain()
```

<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/spark-physical-plan-2.png">

---

## Spark Join Strategies

<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/spark-web-ui-1.png">


## Background

* Whenever we join, there is a shuffle
* Shuffle recap:
  * Data copied across the network to other executors
  * Involves data serialization, memorconsumption, network I/O
  * Shuffle are therefore **complex** & **costly**
* Most issues I've seen are a result of bad joins


## Background (cont'd)

* When you join, all records with the *same joining key* are written to the *same partition*
* Once this is complete, Spark can issue tasks to work on each parititon and perform the join without having to request data from other partitions
* All data for a particular join key must fit in a single partition
  * Max partition size = 2GB


### All strategies boil down to one thing..

Stop (or reduce) the shuffle!

<img loading="lazy" src="imgs/in-the-trenches-with-spark/shuffle.gif" height="500">
<img loading="lazy" src="imgs/in-the-trenches-with-spark/stop_it.gif" width="300">


### Normal Join

<pre class="stretch"> 
  <code class="language-sql stretch"> 
  SELECT
      transactions.user_id,
      transactions.amount
      users.signup_country_code,
      users.signup_source
  FROM
      transactions
      INNER JOIN users
          ON transactions.user_id=users.user_id
  </code>
</pre>


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-3-normal-join.png">


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-3-user-graph.png">


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-3-normal-join-w-data.png">


### Broadcast join

* A copy of the smaller dataset is sent to each executor
* No shuffle required!
* Will almost always be the best performing join strategy (if it works)


### Broadcast join (cont'd)

* Spark will automatically try and broadcast small datasets
  * One of the tables must be smaller than 10MB
    * `spark.sql.autoBroadcastJoinThreshold`
  * Must take <10 min
    * `spark.sql.broadcastTimeout`
  * Disable auto-broadcasting with `spark.sql.autoBroadcastJoinThreshold: -1`
* Manually mark datasets for broadcasting with `F.broadcast(df)`


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-3-broadcast-join.png">


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-3-broadcast-join-w-data.png">


### Salted Join

* Salting inserts a random element into the partitioning
* Example with 5 "salt partitions"
  * Add a random number between 1 and 5 to each row in the large (skewed dataset)
  * Your largest partition will be divided in 5
  * Smaller dataset is copied 5 times
* Join key becomes (original_join_key, salt_key)


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-3-salt-join-1.png">


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-3-salt-join-2.png">


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-3-salt-join-3.png">


### Partial Broadcast Join

* Identify high frequency (HF) join keys (i.e. `shop_id`s with the most transactions)
* Divide datasets into two: one with HF keys and another with all others key
* Broadcast join for HF datasets
* Regular join for LF datasets


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-3-partial-broadcast-join.png">


<img loading="lazy" class="stretch" src="imgs/in-the-trenches-with-spark/example-3-partial-broadcast-join-dag.png">


### Filter prior to joining

* Shrink your dataset so there is less to shuffle
* Spark query optimizer will sometimes automatically do this for you
  * Need to check the physical plan to be sure


### Adjust your partioning

* Default number of partitions will be equal to number of files
* Anytime there is a shuffle (i.e. join), spark will default to 200 partitions
  * Can control this setting with `spark.sql.shuffle.partitions: 4000`


### Adjust your partioning (cont'd)
* Be careful:
  * Too few and you lose out on parallelism (executors sit idle) 
  * Too many may result in task scheduling time > actual execution time
* Easy rule of thumb: aim for 128MB per partition
  * `[largest_dataset_size_Mb] / 128Mb`
  * Grab `largest_dataset_size` from one of the Exchange operations in SQL tab 


### Adjust your partioning (cont'd)

*"There is no replacement for simply increasing the number of partitions until performance stops improving.”*

**- Holden Karau - High Performance Spark**
