Quick and dirty script for reading/writing Hive-style partitioned files in Julia, see `readhivedir` and `writehivedir`

```
col1=yes/
    col2=lemons/
        file.arrow
col1=no/
    col2=oops/
        another_file.arrow
```

```
using DataFrame
a = DataFrame(a=[1,2,3], b=["a","b","a"])

using CSV
writehivedir(CSV.write, "csvtest", a, [:b]; filename="part0.csv")
readhivedir(f->CSV.read(f, DataFrame), "csvtest")

using Arrow
writehivedir((path, table)->Arrow.write(path, table; compress=:zstd), "arrowtest", a, [:b]; filename="part0.arrow")
readhivedir(path->DataFrame(Arrow.Table(path)), "arrowtest") # preserves mmap
```
