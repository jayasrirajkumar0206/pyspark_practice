from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.MEMORY_AND_DISK)
df.persist(StorageLevel.DISK_ONLY)
df.persist(StorageLevel.MEMORY_ONLY_SER)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
df.persist(StorageLevel.OFF_HEAP)


#cache
df.cache()
df.unpersist()

