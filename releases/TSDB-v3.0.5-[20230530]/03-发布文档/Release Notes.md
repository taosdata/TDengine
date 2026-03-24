# Release Notes

**Dear community members,**
 
The past six years have been a long but exciting journey for TDengine, seeing the evolution of our product from a basic TSDB to a full-featured cloud-native solution for time-series data, and the growth of our team from a five-person startup to the 80 members of today. 
Throughout our journey, our constant goal has been to provide our community with first-class products and services. Today, on TDengine’s sixth anniversary, it is with much pride that we look back on the evolution of TDengine from 1.0 to 2.0 and 3.0, from the first line of code written by Jeff himself to the hundreds of thousands of lines that now comprise our product, indicating the continuous growth of our ecosystem, the increasing maturity of features such as stream processing and data subscription, the launch of our fully managed cloud service – and, of course, the resolution of many issues brought up by community members like yourself.
And indeed, what gives our team the most satisfaction is the growth of our open-source community over the years, exemplified by our over 21,000 stars on GitHub and more than 250,000 instances installed worldwide. We are delighted that an ever-increasing number of users and developers have chosen to become members of our community and accompany us on our journey, and we would like to take this opportunity to express our sincere gratitude for your support – our greatest motivation to continue our work and accomplish our goals.
Going forward, we intend to continue innovating in the time-series database field and enabling digital transformation across more industries. It is only through your support that we will be able to deepen our understanding of the scenarios and use cases of various industries and to maintain the growth of our product in a way that aligns with the needs of our community. Once again, thank you for being there with us over the past six years, and we hope that we can count on your continuing support in the future.
 
**The TDengine Team, 2023/Jun/6**

**New Features and Improvements in TDengine 3.0.5.0**
1. System stability & performance
   - Improved system stability under high stress data writing
   - Optimized system performance in some query scenarios
   - Altering database replicas doesn't block writing by introducing RAFT Learner
   - Write driven cache for last() and last_row() to improve the query performance
   - Optimized time cost of creating/dropping database
   - Log long queries by default for easy debugging
   - Controlled meta data cache in taosc library
   - dnode can be restored after its data is totally lost (Enterprise only)
2. System security
   - Privilege control at table level (Enterprise only)
   - License key can be updated using SQL command by "root" (Enterprise only)
3. Stream processing
   - Significantly reduced disk I/O and memory usage
   - Stream can be paused/resumed
4. Data Subscription
   - Consuming progress can be queried
   - Consumers can perform seek operation
   - Consumers can subscribe supertable with tag filtering
   - Consumers can retrieve meta data based on a topic name
   - Improved performance
5. Others
   - Maximum row length is increased to 64KB
   - interp() can be used for super table
   - Python UDF can support multiple versions with "REPLACE" command
   - Partition by and window clause can be followed by "Having" clause
