# Ikkat - A Distributed File System
the name is just random, I already had another repo named distributed system, so

## Question

need to process petabytes of numerical data to discover large prime numbers for next-generation encryption systems

two components - distributed file system, distributed prime number finding application

will need a 2-page document at the end, so better to document decisions well
### Part 1 - The distributed file system

think AFS because AFS code is open and easily available

tasks:
 - basic client-server setup with RPC, basic RPC versions of
	 - open()
	 - create()
	 - read()
	 - write()
	 - close()
- whole file caching on client side
	- reads and writes local while file open
	- flushed when file closed
	- on next access, client will send TestAuth to check if file has been changed
	- if not, use local cached copy
- fault tolerance
- at least three replica servers
	- choose primary based on consensus algorithms?
	- trade-offs, explanations
## Choice of Environment

which of these three should we choose? why?

- VM
- WSL
- Docker
## References


[notes doc](https://docs.google.com/document/d/1RqWAw4xXuOwCf-Y8gUuitbKD9yG5_Jj3v4r1IxojRr4/edit?usp=sharing) to keep track of changes and to contain explanations of edits we make
