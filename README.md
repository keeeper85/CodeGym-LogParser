# CodeGym-LogParser
Project at the end of level 37. A program which reads logs from files and filters the results. Developing own query commands.

You've implemented a log parser that reads from different files.

In addition to the parser, you also implemented your own query language. We need it in order to minimize the number of methods. A line in our log file contained a total of 5 parameters plus one optional parameter.
If a query has two parameters, then there are 25 possible combinations. This means that choosing any two of them would require us to implement 25 methods. Now imagine that the lines of the log file have not 5 parameters, but 10. And that the number of query parameters is not 2, but 3. In this case, we would need 10 * 10 * 10 = 1000 methods.
The more complex the log, the more time a developer can save.

One of the possible improvements you could make would be to implement support for a query with the number of parameters, e.g. 3:
get field1 for field2 = "value1" and field3 = "value2" and date between "after" and "before"

Architecturally speaking, it would be appropriate to improve the program by using the command pattern (to get the field values in a consistent manner). Implement it if you haven't done so already.
