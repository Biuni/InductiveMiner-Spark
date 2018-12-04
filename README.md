# Inductive Miner ⛏️ on Apache Spark 💥
An Implementation of Inductive Miner plugin with Apache Spark.

This's an initial release of IM. Will should be implemented:
- Concurrent Cut
- Loop Cut
- Concurrent Split
- Loop Split
- Falltrough

### Install
```bash
$ git clone https://github.com/Biuni/InductiveMiner-Spark.git
$ cd InductiveMiner-Spark
$ sbt
```

### Execute
```bash
> run "/home/user/Desktop/log.txt" IM
```
or
```bash
> run "/home/user/Desktop/log.txt" IMf 0.5
```

### Performance
![IM Performance](https://i.postimg.cc/65k731X2/performance.png)

Testing performed on a virtual machine with **Ubuntu 18.04** ( *Intel i5-8250U @ 1.60GHz x 4 core | 4.8 GB di RAM | 64 bit* )

*PS: In the logs used as a test, each track consists of 19 activities.*


### Authors
*Gianluca Bonifazi*  
*Gianpio Sozzo*
