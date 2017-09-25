# Agni: An experimental LTS for Prometheus

This is an experimental LTS for Prometheus with blocks stored in S3. We push 
blocks as Prometheus produces them to S3 and then we have a reading service 
which serves queries off the blocks in S3.

The focus is operational simplicity rather than speed. The plan is to try and 
upstream as much of Agni as possible, for example the shipper will be 
upstreamed as the remote-bulk write end-point.
