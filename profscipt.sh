go test -benchmem -run=^$ -bench ^BenchmarkFlatFields_NewPaospPackableComputBench2$ \
-cpuprofile=cpu.prof -memprofile=mem.prof \
github.com/BranchAndLink/paosp/access
