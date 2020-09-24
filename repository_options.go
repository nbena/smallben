package smallben

import "fmt"

type RepositoryOptions struct {
	Host     string
	DbName   string
	Port     int
	User     string
	Password string
}

func (o *RepositoryOptions) PostgresString() string {
	return fmt.Sprintf("host=%s dbname=%s port=%d user=%s password=%s", o.Host, o.DbName, o.Port, o.User, o.Password)
}
