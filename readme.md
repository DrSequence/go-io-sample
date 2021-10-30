## IO csv reader sample
### version 0.0.1-SNAPSHOT

### Goals:  
main:
read huge file, hex substring, write to new file.  

cmd/v2 version can read and write use ony one file.


special version:  
v2 version has hardcode with position for phone number which I need to hash (string to hex). It's snapshot version. 
todo list:  
1) position with yaml 
2) find position and ask an user about it


----------------------------------------------------------------
### How to run
> install go
> go run main.go 1kk.csv 1kk_ex.csv 1


// TODO:
> install go 
> open yaml.file and modify if you need it  
> (optional) move your file here  
> go run .  


### file generator
>  go run ./gen/main.go 
----------------------------------------------------------------

Useful:
1. [reading cvs](https://ankurraina.medium.com/reading-a-simple-csv-in-go-36d7a269cecd)
2. [reading with gorutines (RUS)](https://golangify.com/writing-file-multiple-goroutines)
3. [go and csv](https://zetcode.com/golang/csv/)
4. [workers](https://goinbigdata.com/golang-wait-for-all-goroutines-to-finish/) 
5. [random](https://gobyexample.com/random-numbers)   
6. [gorutines](https://www.golangprograms.com/goroutines.html)
----------------------------------------------------------------
