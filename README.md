# csv2python
Rust sample: read graph from gzipped csv and write to HTTP or NNG API. 
Simple parallelize with rayon.

Swith API here:
```
        // Adding the edge using REST HTTP:
        let r = send(record);
        // Adding the edge using NNG:
        //let r = mr_edge(record);
```

To be used with data:
https://snap.stanford.edu/data/soc-sign-bitcoinotc.csv.gz
