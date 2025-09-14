import oxyroot

if __name__ == '__main__':
    import time

    file_name = "ntuples.root"
    tree_name = 'mu_mc'

    oxy_start_time = time.time()
    oxy_tree = oxyroot.open(file_name)[tree_name]
    oxy_tree.to_parquet("ntuples.pq", overwrite=True, compression="zstd")
    oxy_end_time = time.time()

    print("\n Total time")
    print(f"Oxyroot took: {oxy_end_time - oxy_start_time:.3}s")
