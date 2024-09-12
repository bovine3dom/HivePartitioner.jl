Quick and dirty script for reading/writing Hive-style partitioned arrow files in Julia, see `readhivedir` and `writehivedir`

```
col1=yes/
    col2=lemons/
        file.arrow
col1=no/
    col2=oops/
        another_file.arrow
```

```
a = DataFrame(a=[1,2,3], b=["a","b","a"])
writehivedir("arrowtest", a, [:b])
readhivedir("arrowtest")

writehivedir("csvtest", a, [:b]; writer=CSV.write)
readhivedir("csvtest"; reader=f->CSV.read(f, DataFrame))
```
