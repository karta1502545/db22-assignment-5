# Assignment 5
In this assignment, you are asked to implement the conservative concurrency manager.

## Steps
To complete this assignment, you need to

1. Fork the Assignment 5 project
2. Trace the code in `org.vanilladb.core.storage.tx` and `org.vanilladb.core.storage.tx.concurrency` in `core-patch`
3. Create your own `ConservativeConcurrencyMgr`
4. Modify the stored procedure API and stored procedures of the micro-benchmark to accommodate read-/write-sets and use your concurrency manager
5. Remember to modify `vanilladb.properties` and set the concurrency managers to your new one
6. Run experiments with `bench`
7. Write a report

## Conservative Locking

In the conservative locking approach, we lock the read/write sets of a given transaction at once (atomically) **before execution**. Since the locks are retained in the beginning, there are no interleaving for conflicting txs and can thus prevent deadlocks. It performs well only if there are no/very few long txs. To find out which objects to lock before execution, we need to collect the read/write sets in the stored procedures.

In this assignment, you are asked to implement the conservative concurrency manager that extends `ConcurrencyMgr`. You can trace the other concurrency managers like `SerializableConcurrencyMgr` as a reference to implement your own. Note that, you should not modify the existing `ConcurrencyMgr` interface. You can simply override and leave some methods blank if you think they are no longer necessary in the conservative implementation. You can also add some methods if needed.

The workload we provide only access the data with their primary keys instead of predicates(e.g. you don't have to take care of the query such as "find all users with balance > 100.0$"). 

After creating your own ConcurrencyMgr, you may have to modify the `StoredProcedure` API and the `MicroTxnProc` so that you can collect the read/write objects before executing a transaction.

Note that we also provide the TPC-C benchmark, but due to the characteristics of the TPC-C, it is hard to implement conservative locking for the TPC-C. Therefore, **you only have to implement conservative locking for the micro-benchmark**.

## What you should do

1. Implement the conservative concurrency manager that extends `ConcurrencyMgr`.
2. Make sure transactions with lower `txNum`s acquire locks **before** transactions with higher `txNum`s to ensure deterministic execution.
3. Modify the `StoredProcedure` API and the `MicroTxnProc` so that you can collect the read/write objects before executing a transaction.
4. You only need to make sure that **conservative locking** works on **micro benchmark**.
5. However, you need to explain the challenges of implementing conservative locking for the TPC-C benchmark in phase 1 report.

## Phase 1 Report

- How you implement
  - API changes and/or new classes
- Tell us what is the challenge of implementing conservative locking for the TPC-C benchmark
- Experiments
  - Your experiment environment including (a list of hardware components, the operating system)
    - e.g. Intel Core i5-3470 CPU @ 3.2GHz, 16 GB RAM, 128 GB SSD, CentOS 7
  - Compare the throughputs before and after your modification using the given benchmark & loader (You should use more RTE to demonstrate the concurrent execution behaviour)
    - The benchmarks and parameters you use for your experiments
    - Analyze and explain the result of your experiments
  - Observe and discuss the impact of buffer pool size to your new system

Note: There is no strict limitation to the length of your report. Generally, a 2~3 pages report with some figures and tables is fine. **Remember to include all the group members' student IDs in your report.**

## Phase 2 Report

- List all the differences between your work and the solution. For each difference:
  - Describe the difference
  - Which implementation is better?
  - Point out the strengths of the implementation you think is better.

There is also no strict limitation to the length of your report. It's good to have 1~2 pages.

## Submission
The procedure of submission is as following:

1. Fork our Assignment 5 on GitLab
2. Clone the repository you forked
3. Finish your work and write the report
4. Commit your work, push your work to GitLab.
    - Name your report as `[Team Number]_assignment5_report1.pdf`
        - E.g. team1_assignment5_report1.pdf
5. Open a merge request to the original repository.
    - Source branch: Your working branch.
    - Target branch: The branch with your team number. (e.g. `team-1`)
    - Title: `Team-X Submission` (e.g. `Team-1 Submission`).


The procedure of **phase 2** is as follows:

1. Write a report about the differences between your work and the solution.
2. Commit your work, push to GitLab and then open a merge request to submit your report.The repository should contain
    - *[Team Number]*_assignment5_report2.pdf (e.g. team1_assignment5_report2.pdf)

Note: Each team only needs one submission.

**Important: We do not accept late submission.**

## Plagiarism will not be tolerated

If we find you copy someone’s code, you will get 0 point for this assignment.

## Hint

- To acquire the read/write set atomically, you may want to let this process execute serially (e.g. in a critical section)
- You will need a special object to represent the **Key** of the record, which can uniquely identify a record, for locking. You can create one by yourselves or use our PrimaryKey. Following is the template class of a PrimaryKey :

```Java
public class PrimaryKey  {

    private String tableName;
    private Map<String, Constant> keyEntryMap;

    public PrimaryKey(String tableName, Map<String, Constant> keyEntryMap) {
      // TODO: Figure it out yourself
    }

    public String getTableName() {
      // TODO: Figure it out yourself
    }

    public Constant getKeyVal(String fld) {
      // TODO: Figure it out yourself
    }

    @Override
    public boolean equals(Object obj) {
      // TODO: Figure it out yourself
    }

    @Override
    public int hashCode() {
      // TODO: Figure it out yourself
    }

}

```
## Deadline

### Phase 1
Submit your work before **2022/05/11 (Wed.) 23:59:59**.

### Phase 2
Submit your work before **2022/05/18 (Wed.) 23:59:59**.

Late submission will NOT be accepted.
