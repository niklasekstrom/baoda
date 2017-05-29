# The Branching Append-Only Dynamic Array (BAODA) Data Type
This is a description of the branching append-only dynamic array data type. It describes the data type, specifies its correctness conditions, presents an algorithm that implements it, and a proof of correctness.

The BAODA is similar to an append-only dynamic array (AODA), but instead of concurrently appending elements to an append-only dynamic array, processes that uses the BAODA creates private branches and appends elements to those branch. The head of an array is however not committed immediately, but is initially in a tentaive. The BAODA guarantees that all committed array states lie on the same path, and that each state on a path is an extension of its parents.
