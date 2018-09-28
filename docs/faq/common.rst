常见问题
========

jdk版本低问题
-------------

spark2.2.0版本依赖于java1.8，如果执行spark程序的时候jdk版本低于1.8的时候会包版本错误问题。

hadoop的文件系统异常问题
------------------------

*当项目中同时依赖hadoop-commons包和hadoop-hdfs包的时候，打包项目的时候会存在一个问题，那就是hadoop-hdfs(hadoop2.8.1之后是hadoop-client包)包下
的MATA-INF/service/org.apache.hadoop.fs.FileSystem文件会被覆盖，导致打包程序无法使用org.apache.hadoop.hdfs.DistributedFileSystem文件系统无法
使用。*

hadoop-common包

.. code :: python

    org.apache.hadoop.fs.LocalFileSystem
    org.apache.hadoop.fs.viewfs.ViewFileSystem
    org.apache.hadoop.fs.ftp.FTPFileSystem
    org.apache.hadoop.fs.HarFileSystem


hadoop-hdfs或者hadoop-client包

.. code :: python

    org.apache.hadoop.hdfs.DistributedFileSystem
    org.apache.hadoop.hdfs.web.WebHdfsFileSystem
    org.apache.hadoop.hdfs.web.SWebHdfsFileSystem
    org.apache.hadoop.hdfs.web.HftpFileSystem
    org.apache.hadoop.hdfs.web.HsftpFileSystem