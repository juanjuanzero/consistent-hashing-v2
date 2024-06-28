# Consistent Hashing V2

This is my own implementation of consistent hashing but ill only be distributing this in one application rather than a multiple machines

- [x] add implement a basic hash ring
- [] implement quorum reads
  - it will be the hash rings responsibility to close out any changes done by the quorum.
  - as a point of design any a primary node or any of its replicas can process a read and update event, this will mean that a set of nodes can disagree about who has the right information, this is where vector clocks will come in handy
  - the Hash Ring will be the main coordinator and be responsible for making sure that the reads are up to date.
  - When the Hash Ring calls the get method, it will retrieve all of the data that has been saved in the replicas and primary.
  - Any node can process a put request meaning replicas can also have the most up to date information...
