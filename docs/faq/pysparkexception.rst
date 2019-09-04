执行pyspark时候报错
===================

报错异常信息
>>>>>>>>>>>>

使用pyspark执行python脚本的时候报错
::

    18/06/22 11:58:39 ERROR SparkUncaughtExceptionHandler: Uncaught exception in thread Thread[main,5,main]
    java.util.NoSuchElementException: key not found: _PYSPARK_DRIVER_CALLBACK_HOST
        ...
    Exception: Java gateway process exited before sending its port number


解决办法
>>>>>>>>

将spark环境的python包添加到PYTHONPATH环境变量中

::

    export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
    export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH
