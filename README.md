# Two Phase MapReduce:Sparse Matrix Multiplication Using Hadoop
This code implements a Hadoop MapReduce program, TwoPhase.java, that computes the multiplication of two given matrices using the two-phase approach.

The input matrices A and B are stored in two separate directories, e.g., mat-A and mat-B. Each directory contains a single text file, each line of which is a tuple (row-index, column-index, value) that specifies a non-zero value in the matrix.

For example, the following is the content of mat-A/values.txt which stores the entries for the matrix A shown above.

           0,0,2
           0,1,2
           0,2,1
           1,0,2
           1,1,1
           2,0,1
           2,2,2
           
Invocation of the program is as follows:

> bin/hadoop jar TwoPhase_2p.jar TwoPhase mat-A mat-B output

Your output directory (the file part-r-00000) should contain the entries of matrix C (= A * B) in the following format (tab-separated). For example,

          1,1 3
          1,2 7
          2,1 2
          2,2 4
          3,1 3
          3,2 3
