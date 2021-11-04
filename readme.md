## IO csv reader sample
### version 0.0.2

### Goals:  
main:
read huge file, hex substring, write to new file.  
repo has 2 version.

----------------------------------------------------------------
### How to run

### v1
> install go
> go run cmd/v1/main.go 1kk.csv 1kk_ex.csv 1

### v2 version is a spical verion for customer.
#### cmd/v2 version can read and write use ony one file.  
if need to read file and create new use this:
> go run cmd/v2/main.go csv/test.csv n  
if need to read and to write with the same file use: 
> go run cmd/v2/main.go csv/test.csv o  

v2 version has hardcode with position for phone number which I need to hash (string to hex). It's snapshot version. 
todo list:  
1) position with yaml 
2) find position and ask an user about it

### v3 insert filename and position (phone number leght is const)
> go run cmd/v3/main.go csv/lt.csv 2

### file generator. insert amount of field and filename.
>  go run ./gen/main.go 100000 t.csv
----------------------------------------------------------------

Useful:
1. [reading cvs](https://ankurraina.medium.com/reading-a-simple-csv-in-go-36d7a269cecd)
2. [reading with gorutines (RUS)](https://golangify.com/writing-file-multiple-goroutines)
3. [go and csv](https://zetcode.com/golang/csv/)
4. [workers](https://goinbigdata.com/golang-wait-for-all-goroutines-to-finish/) 
5. [random](https://gobyexample.com/random-numbers)   
6. [gorutines](https://www.golangprograms.com/goroutines.html)
7. [fast reader with goroutines](https://medium.com/swlh/processing-16gb-file-in-seconds-go-lang-3982c235dfa2)
----------------------------------------------------------------
