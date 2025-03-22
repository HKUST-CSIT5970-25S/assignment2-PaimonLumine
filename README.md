[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/vR2_GQCZ)
# csit5970 Assignment-2 (10 marks)

### Deadline: April, 1st, 23:59 (Tuesday)

---

## MapReduce Programming

In this assignment, you will write MapReduce code that counts the bigrams appeared in a text file, using the two design patterns taught in class. Please follow these instructions carefully!

### Environment Setup

**Note: Please prepare the Hadoop environment according to the lab's instructions in order to run the subsequent commands; you are free to use a Docker container or a VM. If you want to use EC2, please make sure that the instance have sufficient memory(>4G) and vCPU>=2. We recommend t2.large for this exercise**

#### EC2 Setup

For EC2 Users, you are recommended to use the web shell provided by EC2 to get much better stability. You can get it by 1. select the instance and click Connect on the EC2 panel. 2. Choose EC2 Instance Connect and click on connect button.

Before cloning the repo, you will need to install gh.

```
$ sudo apt update
$ sudo apt install gh
```

Then, login to your GitHub account.

```
$ gh auth login
```

After logging in, you will be able to clone the repo. You are recommended to clone the repo using gh repo clone.


After cloning, cd into the repo and run the following scripts.


The `install_docker.sh` and `start_hadoop.sh` scripts will help you install docker on EC2 instance, and start a hadoop container with home directory mounted to `/host`. If you are using EC2, simply run

```
$ sudo bash install_docker.sh
```

```
$ sudo bash start_hadoop.sh
```


For text editing on EC2 instance, please refer to lab-0 or watch the following recording: https://hkust.zoom.us/rec/share/FRAflAjNKRR1TA2AaUKoibycxNtRkK4TTN8STd-7ngkQLhsVxogq_t31oVPFRNuJ.Pm7E6y9r1spxJ4Vg

Notice that the hadoop filesystem and the maven are installed inside the container, so you need to enter the container by `docker exec -w /host -it quickstart bash` and then run the compilation and hadoop commands.

This assignment repo is generated using [Apache Maven][Maven]. You'll find a `pom.xml` file in the root directory (which tells [Maven][Maven] how to build the code) and a source folder `src/`. Inside `src/`, there are two sub-directories, one for the main source code (`main/`) and another for the test code (`test/`). The test code is nothing but a placeholder and will not be used in this assignment. The real code locates in the source folder, where you'll find the following Java class files: 

* Sample code: `WordCount.java`, `BigramCountPairs.java`, `BigramCountStripes.java`
* **Skeleton code to be completed in this assignment:**  `BigramFrequencyPairs.java`, `BigramFrequencyStripes.java`, `CORPairs.java`, `CORStripes.java`
* Useful data structures: `PairOfStrings.java`, `HashMapStringIntWritable.java`
* Result checking programs: `AnalyzeWordCount.java`, `AnalyzeBigramCount.java`, `AnalyzeBigramFrequency.java`

### Warming up 1: WordCount

The assignment includes a sample WordCount code for you to get started. To try it out, let's `cd` into the `assignment-2-<yourGitHubHandle>` folder under which `pom.xml` is located. We can build the entire project by typing

```
$ mvn clean package --settings settings.xml
```

The build should succeed without error. Before we run the WordCount example, we need to copy a text file, say, `1400-8.txt` (Project Gutenberg EBook of Great Expectations, by Charles Dickens), to HDFS:

```
$ hadoop fs -put 1400-8.txt .
```

We can now run the WordCount example to count the occurrences of each word in the file we've just copied:

```
$ hadoop jar target/assignment-2-1.0-SNAPSHOT.jar hk.ust.csit5970.WordCount -input 1400-8.txt -output wc -numReducers 2
```

> Hadoop will print out a wealth of runtime information to track the job execution. Examining this information will give you a clear idea about what's going on there.

In the command above, we have specified the input file (`1400-8.txt`), the output directory (`wc`), and the number of reduce tasks in the Hadoop job. Since we have launched 2 reduce tasks, they respectively produced two splits in the `wc` directory, namely, `part-r-00000` and `part-r-00001`:

```
$ hadoop fs -ls wc
```

Along with the two splits, there is an empty file called `_SUCCESS`, which indicates that the Hadoop job has completed *successfully*.

We have provided a handy tool (`AnalyzeWordCount.java`) to analyze the output:

```
$ hadoop jar target/assignment-2-1.0-SNAPSHOT.jar hk.ust.csit5970.AnalyzeWordCount -input wc
```

The tool scans the output in `wc` and generates the following statistical summary:

```
total number of unique words: 22183
total number of words: 187462
number of words that appear only once: 13167

ten most frequent words:
the	7885
and	6509
I	5670
to	5018
of	4446
a	3961
in	2847
was	2685
that	2648
had	2056
```

Alternatively, we can examine the details by copying the results to the local disk. For example:

```
$ hadoop fs -get wc .
$ head wc/part-r-00000
```

```
"'Eat	1
"'God	1
"'She	1
"'What	1
"(I'm	1
"--At	1
"--By	1
"--Had	1
"--Invest	1
"--That	1
```

Now it's time to examine the `WordCount` code in detail.

### Warming up 2: Count the Bigrams

[Bigrams](https://en.wikipedia.org/wiki/Bigram) are simply sequences of two consecutive words. For example, the following famous quotes from Charles Dickens' "A Tale of Two Cities", `It was the best of times` and `It was the worst of times` contain 7 bigrams: "It was", "was the", "the best", "best of", "of times", "the worst", and "worst of".

**The sample code is to count the occurrences of each bigram.** In particular, it builds two versions of the program:

1. *A "pairs" implementation.* The implementation uses combiners and is built atop the skeleton code in `BigramCountPairs.java`. It uses the data type provided in `PairOfStrings.java`.
2. *A "stripes" implementation.* The implementation uses combiners and is built atop the skeleton code in `BigramCountStripes.java`. It uses the data type provided in `HashMapStringIntWritable.java`.

**Output format:** Each line contains a bigram and its count, delimited by tab ([`"\t"`](https://en.wikipedia.org/wiki/Tab_key#:~:text=default%20mostly%20four\).-,Tab%20characters,character%20into%20a%20string%20literal.)). For example, in the previous quotes, bigram "It was" appears twice, and the corresponding output line should be:

```
It was  2
```

You can build the programs by `mvn clean package --settings settings.xml`, and run them:

```
$ hadoop jar target/assignment-2-1.0-SNAPSHOT.jar hk.ust.csit5970.BigramCountPairs -input 1400-8.txt -output bc -numReducers 2
```

To debug, you can either examine the output manually or use the analysis tool provided in `AnalyzeBigramCount.java`:

```
$ hadoop jar target/assignment-2-1.0-SNAPSHOT.jar hk.ust.csit5970.AnalyzeBigramCount -input bc
```

As a reference, you should see something like this:

```
total number of unique bigrams: 89925
total number of bigrams: 171207
number of bigrams that appear only once: 72438

ten most frequent bigrams: 
of	the	820
in	the	724
I	had	579
that	I	503
I	was	452
and I	398
to  the 393
to	be	390
on	the	374
at	the	374
```

Note that the 9th and the 10th most frequent bigrams in your output might be different, but their occurrences should be 374. You may try other input text and debug yourself.

Now it's time to examine the `BigramCountPairs` and `BigramCountStripes` code in detail.

## Assignment

### Part 1 (4 marks): From Bigram Counts to Relative Frequencies

This is a simple extension of the "Count the Bigrams". In the first part of the exercise, you'll count bigrams, and in the second part of the exercise, you'll compute bigram relative frequencies.

Extend your program to compute bigram relative frequencies, i.e., how likely you are to observe a word given the preceding word. The output of the code should be a table of values for P(W<sub>n</sub>|W<sub>n-1</sub>), where W<sub>n</sub> is the n-th word in the documents. For example, in the previous quotes from "A Tale of Two Cities", it is easy to verify that P("was"|"It") = 1 and P("best"|"the") = P ("worst"|"the") = 0.5.

Similar to Warming up 2, you will build two versions of the program:

1. (**2 marks**) *A "pairs" implementation.* The implementation must use combiners and partitioners. You will start with the skeleton code in `BigramFrequencyPairs.java`. You will be using the data type provided in `PairOfStrings.java`.
2. (**2 marks**) *A "stripes" implementation.* The implementation must use combiners. You will start with the skeleton code in `BigramFrequencyStripes.java`. You will be using the data type provided in `HashMapStringIntWritable.java`.

> **Hint:** 
> To compute P(B|A), count up the number of occurrences of the bigram "A B", and then divide by the number of occurrences of all the bigrams that start with "A".

**Output format:** For each word "A", the program first outputs its total count, followed by a list of relative frequencies of its succeeding words, one record per line. For example, the bigram relative frequencies of the quotes from "A Tale of Two Cities" should look something like this, where all items are separated by tab (`"\t"`):

```
It		2.0
It	was	1.0
best		1.0
best	of	1.0
of		2.0
of	times	1.0
the		2.0
the	best	0.5
the	worst	0.5
was		2.0
was	the	1.0
worst		1.0
worst	of	1.0
```

We can tell from the output above that "It" has 2 occurrences, both succeeded by "was", making P("was"|"It") = 1. On the other hand, "the" has 2 occurrences, one succeeded by "best" and another by "worst". As a result, we have P("best"|"the") = P("worst"|"the") = 0.5.

**Note: It is important to follow the output format strictly as your program will be marked by an autograder!**

Once you are done coding, you can build your programs by `mvn clean package --settings settings.xml`, and run the following command:

```
$ hadoop jar target/assignment-2-1.0-SNAPSHOT.jar hk.ust.csit5970.BigramFrequencyPairs -input 1400-8.txt -output bc -numReducers 2
```

In the command above, we have specified the input file, the output path, and the number of reducers. You are free to try something different.

To debug, we have provided a handy tool (`AnalyzeBigramFrequency.java`) which you can use to produce a statistical summary of your output. For example, if you are interested in bigrams starting with "the", you can extract the ten most frequent occurrences by

```
$ hadoop jar target/assignment-2-1.0-SNAPSHOT.jar hk.ust.csit5970.AnalyzeBigramFrequency -input bc -word the
```

```
Ten most frequent bigrams starting with the:
the		7216.0
the	same	0.011086474
the	old	0.009284923
the	first	0.007899113
the	best	0.006236142
the	two	0.00595898
the	other	0.00595898
the	whole	0.0054046563
the	time	0.0052660755
the	way	0.0051274947
the	man	0.0051274947
```

You might see different output for the last two terms, but their relative frequencies should be the same as the reference output.

### Part 2 (6 marks): Collect the Correlation Coefficients of Bigrams

We define the correlation coefficient of two words A and B as COR(A, B):

COR(A, B) = Freq(A, B) / (Freq(A) * Freq(B))

Freq(A, B) denotes the number of lines that contains the word pair (A, B).

Please be noted that each occurrence of a word pair (A, B) & (B, A) will be only counted for once.

For example, if the input data only contains one line "A A B C", then we have **Freq(A, B) = 1, not 2 or 4**. 

But if the input data has multiple lines, and there are three lines containing word pairs (A, B) or (B, A), **Freq(A, B) = 3**. An example is Freq("to", "which") = 3, which can be found in `cor-demo-sample.txt`. 

Freq(A) denotes the frequency of the word A in the text. Note that if there are multiple occurance of "A" in the same line, they will all be counted. In above example "A A B C", Freq(A) = 2, Freq(B) = 1.

To help you better understand this problem, we have provided a demo Python program `demo/cor-demo.py`. 

> **We will use the Python demo program to check if your program's output is correct. You can use this demo program to double-check your solution before submission.  Please ignore the differences in output order. Any order of correct output will be regarded as full marks.**

Similar to Warming up 2 and Part 1, you will build two versions of the program:

1. (**3 marks**) *A "pairs" implementation.* The implementation must use combiners and partitioners. You will start with the skeleton code in `CORPairs.java`. You will be using the data type provided in `PairOfStrings.java`.
2. (**3 marks**) *A "stripes" implementation.* The implementation must use combiners. You will start with the skeleton code in `CORStripes.java`. You will be using the data type provided in `PairOfStrings.java`.

Once you are done coding, you can build your programs by `mvn clean package --settings settings.xml`, and run them:
```
$ hadoop jar target/assignment-2-1.0-SNAPSHOT.jar hk.ust.csit5970.CORStripes -input cor-demo-sample.txt -output cor -numReducers 2
```
In the command above, we have specified the input file, the output path, and the number of reducers. You are free to try something different.

> **Hint 1:**
> In this problem, you are required to do MapReduce twice.
> In the first pass, MapReduce produces a middle result that contains the frequency of each word (i.e., Freq(A)).
> In the second pass, MapReduce firstly reads the middle result, then produces Freq(A, B) for each word pair (A, B) and calculates COR(A, B).
>
> **Hint 2:**
> The fisrt-pass MapReduce code of CORPairs and CORStripes should be same.

**Output format:** For each word pair "A"-"B", the program outputs the two words "A" and "B" ("A" is alphabetically smaller than "B"), then outputs COR(A, B). The three items are seperated by tab (`"\t"`).

For the sample input `cor-demo-sample.txt`, we have provided the sample output `cor-demo-output.txt` and the sample middle file generated by the first-pass MapReduce `cor-demo-mid.txt`. You can use these files to debug your program. (Note: the sample output is generated with option `numReducers=1`)

### Grading

We will clone your repo, go into your `assignment-2-<yourGitHubHandle>/` directory, and build your Maven artifact. We will run the **four** programs (i.e., `BigramFrequencyPairs.java`, `BigramFrequencyStripes.java`, `CORPairs.java`, and `CORStripes.java`) with input test cases (different from `1400-8.txt` and `cor-demo-sample.txt`) and compare the results with the standard output.

[Maven]: https://maven.apache.org
