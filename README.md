# Market data processing app

##### Fulfills the following requirements 

- Ensure that the number of calls of publishAggregatedMarketData method for publishing messages does not exceed 100 times per second, where this period is a sliding window.
- Ensure that each symbol does not update more than once per sliding window.
- Ensure that each symbol always has the latest market data published.
- Ensure the latest market data on each symbol will be published.

##### Assumptions, edge cases, and other considerations

- The application is latency sensitive, so the data should be sent as soon as it arrives. If the slight delay is acceptable, we can increase the throughput by adding a buffer. The code for the same has been added in the comments but not used.
- The Messages can arrive **out of order** from the source. So, it cannot be assumed that message that arrives later would have the latest market data.
- The sliding window means that rate limit applies to **any** 1-second window over the entire period. For example, if there are 80 messages sent in the first second (between 0.5 and 1 second), and 40 messages sent in the 2nd second (between 1 and 1.5 second), it will still be considered over the limit as there are more than 100 messages in 0.5 to 1.5 second window.
- If the rate limit is reached, subsequent messages are to be sent at the next **earliest** possible opportunity.
    - It is possible that multiple messages arrive from the source before the cool-off period. To ensure fairness, a queue has been implemented.
    - For handling concurrency in scheduling, a single-threaded model has been used.
- As an optimization, it is acceptable to skip the intermediate/old data while waiting in a queue for the restriction period to be over. 
- Processing order between different symbols need not be maintained. That is, if symbol A arrives before symbol B, it is acceptable to process symbol B first.
- Unit tests have been written to cover many of the above cases. They manipulate time programmatically instead of using Thread.Sleep().