有向无环图
==========

spark提交job之后会把job分成多个stage，多个stage之间是有依赖关系的，正如前面所看到的的，stage0依赖于stage1，因此构成了有向无环图DAG。而且stage中
的依赖分为窄依赖和宽依赖，窄依赖是一个worker节点的数据只能被一个worker使用，而宽依赖是一个worker节点的数据被多个worker使用。一般讲多个窄依赖归
类为一个stage，方便与pipeline管道执行，而将以宽依赖分为一个stage。

spark正是使用dag有向无环图来实现容错机制，当节点的数据丢失的时候，我们可以通过dag向父节点重新计算数据，这种容错机制成为lineage。