#!/bin/julia

module ArrowHivePartitioner
export writehivedir, readhivedir

using Arrow, DataFrames # really should only depend on Tables.jl
import Tables, TableOperations

function writehivedir(outdir, df, groupkeys=[]; filename="part0.arrow")
    g = groupby(df, groupkeys)
    for t in keys(g)
        !all(v -> >:(AbstractString, typeof(v)), values(t)) && throw("All grouped column values must be strings") # TODO: support other types?
        path = join(["$k=$v" for (k,v) in zip(keys(t), values(t))], "/")
        mkpath(joinpath(outdir,path))
        Arrow.write(joinpath(outdir,path,filename), g[t][!, Not(keys(t))]; compress=:zstd)
    end
end

function readhivedir(hivedir)
    # two regressions: only deal with one file per folder
    #                  explode if non-arrow file found
    Tables.partitioner(x -> begin
        (root, _, files) = x
        file = files[1]
        colsvals = filter(x-> length(x) == 2,split(root,'/') .|> x -> split(x, '='))
        df = Arrow.Table(joinpath(root, file)) |> DataFrame
        for (col, val) in colsvals
            df[!, col] .= val
        end
        df
    end, Iterators.filter(x->length(x[3]) > 0, walkdir(hivedir))) |> TableOperations.joinpartitions |> DataFrame
end
end
