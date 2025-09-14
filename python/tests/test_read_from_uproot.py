import oxyroot
import uproot
import numpy as np
import os

print(oxyroot.__version__)

def test_read_from_uproot():
    # Create a dummy ROOT file for testing

    input = np.array([4.1, 5.2, 6.3])
    file_name = "test.root"

    with uproot.recreate(file_name) as f:
        f.mktree("tree1", {"branch1": np.float64})
        f["tree1"].extend({"branch1": input})
        

    output = oxyroot.open(file_name)["tree1"]["branch1"].array()
    assert(type(output) is np.ndarray)
    assert(np.array_equal(input, output))

    os.remove(file_name)
