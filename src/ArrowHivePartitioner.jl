#!/bin/julia

module ArrowHivePartitioner
export writehivedir, readhivedir

using Arrow, DataFrames # really should only depend on Tables.jl

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
    mapreduce(x -> begin
        (root, _, files) = x
        mapreduce(file -> begin
            colsvals = filter(x-> length(x) == 2,split(root,'/') .|> x -> split(x, '='))
            df = Arrow.Table(joinpath(root, file)) |> DataFrame
            for (col, val) in colsvals
                df[!, col] .= val
            end
            df
        end, vcat, Iterators.filter(f -> splitext(f)[end] == ".arrow", files))
    end, vcat, Iterators.filter(x->length(x[3]) > 0, walkdir(hivedir)))
end
end
