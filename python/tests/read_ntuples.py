import oxyroot

if __name__ == '__main__':
    import uproot
    import numpy as np
    import time

    file_name = "ntuples.root"
    tree_name = 'mu_mc'

    up_start_time = time.time()
    up_tree = uproot.open(file_name)[tree_name]
    for branch in up_tree.keys():
        # print(branch, up_tree[branch].typename)
        if up_tree[branch].typename != "std::string":
            up_values = up_tree[branch].array(library="np")
            print(f"Uproot read {branch} into a {type(up_values)} and it has a mean of {np.nanmean(up_values):.2f}") 
    up_end_time = time.time()
    
    print("\n")

    oxy_start_time = time.time()
    oxy_branches = oxyroot.read_root(file_name, tree_name=tree_name)
    for branch in oxy_branches:
        oxyroot.read_root(file_name, tree_name=tree_name, branch=branch)
        oxy_values = oxyroot.read_root(file_name, tree_name=tree_name, branch=branch)
        if type(oxy_values) is np.ndarray:
            print(f"Oxyroot read {branch} into a {type(oxy_values)} and it has a mean of {np.nanmean(oxy_values):.2f}")
        else:
            print(f"Oxyroot read {branch} into a {type(oxy_values)} and it has a length of {len(oxy_values)}")
    oxy_end_time = time.time()

    print("\n Total time")
    print(f"Uproot took: {up_end_time - up_start_time:.3}s")
    print(f"Oxyroot took: {oxy_end_time - oxy_start_time:.3}s")
