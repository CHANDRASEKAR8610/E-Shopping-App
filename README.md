# E-Shopping-App
A Submission for Astraakathon by CHANDRASEKAR B.
Hi ! I'm going to demonstrate my ` E-Shopping-App ` for Astraakathon Submission. This application lets you create an account, add or buy a product for sale. Follow the instructions given below for running this application in your Local System.

# Content

* [Run this application](https://github.com/CHANDRASEKAR8610/E-Shopping-App/blob/master/README.md#1-run-this-application)
* [Data Model for ` E-Shopping-App `](https://github.com/CHANDRASEKAR8610/E-Shopping-App#2-data-model-for-e-shopping-app)
* [Reference & Resources](https://github.com/CHANDRASEKAR8610/E-Shopping-App/blob/master/README.md#3-reference--resources)

## 1. Run this application

### 1.1 Prerequisite

you must have following installed in your local system.
1. Python 3.6 or later
2. Docker Desktop latest

### 1.2 Start Cassandra using Docker
To start cassandra on docker, jump into the specified directory and start ` CMD ` in the current directory.
```
cd E-shopping-app/materials
docker-compose up
```
Expected Output: (for single node cluster)
```
Creating network "materials_dc1ring" with driver "bridge"
Creating materials_cassandra-node1_1 ... done
Attaching to materials_cassandra-node1_1
cassandra-node1_1  | CompilerOracle: dontinline org/apache/cassandra/db/Columns$Serializer.deserializeLargeSubset (Lorg/apache/cassandra/io/util/DataInputPlus;Lorg/apache/cassandra/db/Columns;I)Lorg/apache/cassandra/db/Columns;
cassandra-node1_1  | CompilerOracle: dontinline org/apache/cassandra/db/Columns$Serializer.serializeLargeSubset (Ljava/util/Collection;ILorg/apache/cassandra/db/Columns;ILorg/apache/cassandra/io/util/DataOutputPlus;)V
cassandra-node1_1  | CompilerOracle: dontinline org/apache/cassandra/db/Columns$Serializer.serializeLargeSubsetSize (Ljava/util/Collection;ILorg/apache/cassandra/db/Columns;I)I
cassandra-node1_1  | CompilerOracle: dontinline org/apache/cassandra/db/commitlog/AbstractCommitLogSegmentManager.advanceAllocatingFrom (Lorg/apache/cassandra/db/commitlog/CommitLogSegment;)V
.............................
.............................
.............................
cassandra-node1_1  | INFO  [main] 2020-08-19 09:17:39,474 CassandraDaemon.java:556 - Not starting RPC server as requested. Use JMX (StorageService->startRPCServer()) or nodetool (enablethrift) to start it
```

### 1.3 Create Schema Tables
Start the ` cassandra-node ` on docker dashboard and open `CLI ` for the node. Now run the following to open Cql shell for querrying.
```
# cqlsh
```
Expected Output:
```
Connected to spring_demo_cluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.11.6 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
cqlsh>
```

Now copy the following querries to create a keyspace and some of its tables.

```
CREATE KEYSPACE killrvideo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

use killrvideo;

CREATE TYPE killrvideo.acc_details (
    acc_no text,
    pin_no text
);

CREATE TYPE killrvideo.address (
    doorid text,
    street text,
    city text,
    country text,
    pincode int
);

CREATE TABLE killrvideo.add_product (
    userid uuid,
    added_at timestamp,
    title text,
    productid timeuuid,
    cost float,
    description text,
    discount float,
    isavailable boolean,
    stockleft int,
    tags set<text>,
    PRIMARY KEY (userid, added_at, title, productid)
) WITH CLUSTERING ORDER BY (added_at DESC, title ASC, productid ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE killrvideo.account_type (
    acc_type text,
    transaction_at timestamp,
    userid uuid,
    acc_details frozen<acc_details>,
    transactionid timeuuid,
    PRIMARY KEY (acc_type, transaction_at, userid)
) WITH CLUSTERING ORDER BY (transaction_at DESC, userid ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE killrvideo.user_details (
    userid uuid PRIMARY KEY,
    address frozen<address>,
    email text,
    firstname text,
    lastname text,
    products_purchased set<text>
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE killrvideo.product_details (
    productid timeuuid PRIMARY KEY,
    added_at timestamp,
    added_by uuid,
    cost float,
    description text,
    discount float,
    tags set<text>,
    title text,
    isavailable boolean,
    stockleft int
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE killrvideo.latest_products (
    year int,
    added_at timestamp,
    productid timeuuid,
    title text,
    userid uuid,
    PRIMARY KEY (year, added_at, productid)
) WITH CLUSTERING ORDER BY (added_at DESC, productid ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE killrvideo.users_credentials (
    userid uuid PRIMARY KEY,
    password text
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE killrvideo.users_register (
    emailid text PRIMARY KEY,
    password text,
    userid uuid
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

CREATE TABLE killrvideo.buy_product (
    productid timeuuid,
    transaction_at timestamp,
    userid uuid,
    cost float,
    stocks int,
    title text,
    transactionid timeuuid,
    PRIMARY KEY (productid, transaction_at, userid)
) WITH CLUSTERING ORDER BY (transaction_at ASC, userid ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
```

### 1.4 Run python file

` Note: Ensure that you have already installed cassandra-python-driver or please do install to run this application successfuly. `

Now run ` cluster.py ` from your ` CMD ` on your current directory.
```
python cluster.py
```

#### 1.4.1 Register an account
To register an account in this appplication, Make sure you do like the following one.
```
'**********************Electronics Shopping plaza*********************'
Click 1 to view to view Latest Products
Else, Click 0 to Register or Authenticate.
Enter your choice = 0

To Register Click  2
Else to Authenticate Click 3
Enter your choice = 2

**************User Registration*****************
Enter your Email= india@datastax.com
Enter your password= indian

15f408e8-e0a6-11ea-87d0-0242ac130003 has been registered for the email id india@datastax.com

Authenticating your Account.....
Successfully Authenticated :)
```

Copy this ` UUID ` for the future navigation.

#### 1.4.2 Authenticate an account
If you have already registered, you may have to do authentication like the following one.
```
Click 1 to view to view Latest Products
Else, Click 0 to Register or Authenticate.
Enter your choice = 0

To Register Click  2
Else to Authenticate Click 3
Enter your choice = 3

Enter your user Id = 15f408e8-e0a6-11ea-87d0-0242ac130003

Authenticating your Account.....
Successfully Authenticated :)
```

#### 1.4.3 Add a product
` Note: you must be registered & authenticated to add a product. `
To add a product to this appplication for sale, do like the following one.

```
Dear user, to Navigate to pages follow the below instructions carefully.
* To view Latest products, Click 4
* To view Product Details, Click 5
* To Update your Profile, Click 6
* To view your Profile, Click 7
* To Add Product, Click 8
* To Update your Product, Click 9
* To Buy your Product, Click 10
* To Delete your Account, Click 11
* To Quit Application, Click 12

Enter your Choice = 8

Enter the Product name = Raspberry PI 3 Model B+
Enter the Product Description = A low cost pocket sized microcomputer for Mini projects
Enter the Cost of the product = 3200      //in ruppes
Enter the Discount of the product = 30    // in percentage
Enter the year when product added = 2020
Is the product available? Reply True or False.. True
Enter Number of Stocks Left = 10
Do you want to add tag the product? Enter 1 for yes & 0 for No..
1
Add a Tag below !!
Raspberry
Do you want to add tags to the product? Enter 1 for yes & 0 for No..
1
Add a Tag below !!
PI 3
Do you want to add tags to the product? Enter 1 for yes & 0 for No..
0
Your product "Raspberry PI 3 Model B+" has been successfully added for the sale !!
15f6ty78-e0a6-11ea-87d0-0242ac190903 is your productid.
```

#### 1.4.4 View Latest Products
You can view Latest Products either without registering in this application or by registering in this application.
Let's say if I have been registered, now to view Latest Products do like the following

```
Dear user, to Navigate to pages follow the below instructions carefully.
* To view Latest products, Click 4
* To view Product Details, Click 5
* To Update your Profile, Click 6
* To view your Profile, Click 7
* To Add Product, Click 8
* To Update your Product, Click 9
* To Buy your Product, Click 10
* To Delete your Account, Click 11
* To Quit Application, Click 12

Enter your Choice = 4

PRODUCTID                                        TITLE                          ADDED_BY                                ADDED_AT                        YEAR
15f6ty78-e0a6-11ea-87d0-0242ac190903        Raspberry PI 3 Model B+     15f408e8-e0a6-11ea-87d0-0242ac130003           2020-08-19                       2020
```
Now I see 1 Record because we have added only one product. Keep Adding more Products !!!!!

#### 1.4.5 View Specific Product Details
` Note: you must be registered & authenticated to View Specific Product Details. `

Do like the following
```
Dear user, to Navigate to pages follow the below instructions carefully.
* To view Latest products, Click 4
* To view Product Details, Click 5
* To Update your Profile, Click 6
* To view your Profile, Click 7
* To Add Product, Click 8
* To Update your Product, Click 9
* To Buy your Product, Click 10
* To Delete your Account, Click 11
* To Quit Application, Click 12

Enter your Choice = 5

Enter productid = 15f6ty78-e0a6-11ea-87d0-0242ac190903

*****PRODUCT DETAILS******

Productid = 15f6ty78-e0a6-11ea-87d0-0242ac190903
Product Title = Raspberry PI 3 Model B+
Product Description = 
Product Cost = 3200         //in rupees
Product Discount = 30       // in percentage
Product added at = 2020-08-19
Product added by = 15f408e8-e0a6-11ea-87d0-0242ac130003
Tags = Sortedset{'Raspberry','PI 3'}
Is the Product Still available? True
Number of stocks left= 10
```

#### 1.4.6 Buy a Product
` Note: you must be registered & authenticated to buy a Product. `

To buy a product, do like the following
```
Dear user, to Navigate to pages follow the below instructions carefully.
* To view Latest products, Click 4
* To view Product Details, Click 5
* To Update your Profile, Click 6
* To view your Profile, Click 7
* To Add Product, Click 8
* To Update your Product, Click 9
* To Buy your Product, Click 10
* To Delete your Account, Click 11
* To Quit Application, Click 12

Enter your Choice = 10

Enter productid to buy the product = 15f6ty78-e0a6-11ea-87d0-0242ac190903
Enter number of stocks needed = 5
Enter yout Account type {CC ~ Credit Card / DC ~ Debit Card / IB ~ Internet Banking} = CC
Enter your Card / Bank Account number = XXXX XXXX XXXX XXXX
Enter your Pin number = ####
Linking to your Bank Account....
Transaction in Progress......
Howzaat ! your purchase is successful and the produuct is on the way !!
```
#### 1.4.7 Other Features

In addition to the sales, we also offer the users to update their personal details and update their added products.
Do like the above scenarios for all the other features and explore this application more to view the perfectness of Data Modelling.

## 2. Data Model for E-Shopping-App
At First, let have a look at our Conceptual Data Model !!!
![Conceptual Data Model](https://github.com/CHANDRASEKAR8610/E-Shopping-App/blob/master/images/DM.jpg)

# 3. Reference & Resources
1. [Datastax Python Driver Docs](https://docs.datastax.com/en/developer/python-driver/3.24/)
2. [Cassandra-Python Driver](https://pypi.org/project/cassandra-driver/)
3. [UUID and TIMEUUID Functions](https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/timeuuid_functions_r.html)
4. [How to insert UDT?](https://community.datastax.com/questions/7764/how-udt-values-are-inserted-into-c-table-using-dat.html?childToView=7795#answer-7795)
5. [Docker-compose Docs](https://docs.docker.com/compose/gettingstarted/)


