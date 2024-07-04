# Consistent Hashing V2

What? This is a distributed (sort of) key value store that stores key value pairs in-memory . The data will be distributed across Nodes and spread using a hash of the key. On insert when a request comes in the system chooses either the primary node or any of the replica nodes to handle and store the insert. No replication is implemented here. I am merely simulating a scenario where an update to a key may not always reside in the same place as the intended place for the key. The application will also randomly pick who can handle writes in this case. This naturally creates a problem when the system needs to handle updates and retrievals appropriately since two versions of the data from the same key can arise.

How do we solve this? Using vector clocks we can determine a partial order between two events. So we can differentiate between a key that was written to, and an update on that key even if they don't reside in the same Node.

What did I learn?

- Clocks are just counters at the end of the day. The time you have right now reading this is also a counter just counting from a specific reference point.
- Vector Clocks are used to determine partial order
- Lamport Diagrams can also be called spacetime diagrams, it sounds cooler.
- I somehow forgot that slices are pointers to arrays and was wondering why the vector clocks in the test were also updated.

Why? I wanted to create a system that could take in multiple messages in a distributed way and learn more about vector clocks and the problems they solve.

Resources:
This [video series on distributed systems](https://www.youtube.com/@lindseykuperwithasharpie) was interesting and really helpful
