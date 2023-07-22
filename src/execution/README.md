##  Block Nested Loop Join

**块嵌套循环连接算法**

需要的额外参数：bpm(用于缓存tuple)

算法：

```
foreach B - 2 pages pR ∈ R:
	foreach page pS ∈ S:
		foreach tuple r ∈ B - 2 pages:
			foreach tuple s ∈ ps:
				emit, if r and s match
```

其中，B是可用的BufferSize。

