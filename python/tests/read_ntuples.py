import oxyroot

if __name__ == '__main__':
    import uproot
    import numpy as np
    import time

    file_name = "ntuples.root"
    tree_name = 'mu_mc'

    up_start_time = time.time()
    up_tree = uproot.open(file_name)[tree_name]
    for branch in up_tree:
        # print(branch, branch.typename)
        if branch.typename != "std::string": # Uproot cannot read strings?
            up_values = branch.array(library="np")
            print(f"Uproot read {branch.name} into a {type(up_values)} and it has a mean of {np.nanmean(up_values):.2f}") 
    up_end_time = time.time()
    
    print("\n")

    oxy_start_time = time.time()
    oxy_tree = oxyroot.open(file_name)[tree_name]
    for branch in oxy_tree:
        # print(branch, branch.typename)
        # if branch.typename != "string":
        oxy_values = branch.array().to_numpy()
        if type(oxy_values) is np.ndarray:
            print(f"Oxyroot read {branch.name} into a {type(oxy_values)} and it has a mean of {np.nanmean(oxy_values):.2f}")
        else:
            print(f"Oxyroot read {branch.name} into a {type(oxy_values)} and it has a length of {len(oxy_values)}")
    oxy_end_time = time.time()

    print("\n Total time")
    print(f"Uproot took: {up_end_time - up_start_time:.3}s")
    print(f"Oxyroot took: {oxy_end_time - oxy_start_time:.3}s")
