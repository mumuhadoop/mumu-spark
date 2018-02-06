# mumu-spark 基于内存的快速计算系统

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/mumuhadoop/mumu-spark/blob/master/LICENSE)
[![Build Status](https://travis-ci.org/mumuhadoop/mumu-spark.svg?branch=master)](https://travis-ci.org/mumuhadoop/mumu-spark)
[![codecov](https://codecov.io/gh/mumuhadoop/mumu-spark/branch/master/graph/badge.svg)](https://codecov.io/gh/mumuhadoop/mumu-spark)

mumu-spark是一个学习项目，主要通过这个项目来了解和学习spark的基本使用方式和工作原理。mumu-spark主要包括弹性数据集rdd、spark sql、机器学习语言mlib、实时工作流streaming、图形数据库graphx。通过这些模块的学习，初步掌握spark的使用方式。

## spark描述
mumu-spark是一个多功能的快速计算系统，使用spark可以快速的计算大量的数据，比hadoop的mapreduce计算框架快100倍。apache spark是将计算的数据存储到内存中，极大
的提高了计算速度，并且spark可以以master-slave方式部署在集群总，极大的提高了spark的弹性计算。apache spark目前已经发展成集多功能于一体的框架，提供了spark sql、spark streaming、spark mlib、spark graphx等。

## spark 安装方式

## spark rdd

## spark sql

### parquet存储格式
Apache	Parquet	is	a	columnar	data	storage	format,	specifically	designed	for	big	data	storage	and
processing.	It	is	based	on	record	shredding	and	the	assembly	algorithm	from	the	Google	Dremel	paper.	In
Parquet,data in	a	single	column	is	stored	contiguously.	The	columnar	format	gives	Parquet	some	unique
benefits.For example,	if	you	have	a	table	with	100	columns	and	you	mostly	access	10	columns	in	a	rowbased	format,	you	will have to load all	the	100	columns,	as	the	granularity	level	is	at	the	row	level.	But,
in	Parquet,you	will	only	need	to	load	10	columns.	Another	benefit	is	that	since	all	of	the	data	in	a	given column is	of	the	same	datatype	(by	definition),	compression	is	much	more	efficient。

## spark streaming

## spark mlib

## spark graphx


## 相关阅读

[hadoop官网文档](http://hadoop.apache.org)

[Apache spark 官网](http://spark.apache.org/)

## 联系方式

以上观点纯属个人看法，如有不同，欢迎指正。

email:<babymm@aliyun.com>

github:[https://github.com/babymm](https://github.com/babymm)

