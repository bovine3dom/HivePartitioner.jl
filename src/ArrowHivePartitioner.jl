#!/bin/julia

module ArrowHivePartitioner
export writehivedir, readhivedir

using Arrow, DataFrames # really should only depend on Tables.jl
import Tables, TableOperations

function writehivedir(outdir, df, groupkeys=[]; filename="part0.arrow", writer=(path, table)->Arrow.write(path, table; compress=:zstd))
    g = groupby(df, groupkeys)
    for t in keys(g)
        !all(v -> >:(AbstractString, typeof(v)), values(t)) && throw("All grouped column values must be strings") # TODO: support other types?
        path = join(["$k=$v" for (k,v) in zip(keys(t), values(t))], "/")
        mkpath(joinpath(outdir,path))
        writer(joinpath(outdir,path,filename), g[t][!, Not(keys(t))])
    end
end

function readhivedir(hivedir; reader=path->DataFrame(Arrow.Table(path)))
    # two regressions: only deal with one file per folder
    #                  explode if non-readable file found
    Tables.partitioner(x -> begin
        (root, _, files) = x
        file = files[1]
        colsvals = filter(x-> length(x) == 2,split(root,'/') .|> x -> split(x, '='))
        df = reader(joinpath(root, file))
        for (col, val) in colsvals
            df[!, col] .= val
        end
        df
    end, Iterators.filter(x->length(x[3]) > 0, walkdir(hivedir))) |> TableOperations.joinpartitions |> DataFrame
end
end
