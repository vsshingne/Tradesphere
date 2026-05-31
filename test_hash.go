package main
import "fmt"
import "golang.org/x/crypto/bcrypt"
func main() {
    hash, _ := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
    fmt.Println(string(hash))
}
