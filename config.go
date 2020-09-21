package smallben

import (
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/spf13/viper"
	"log"
)

const (
	KeyDbHostname = "DB_HOSTNAME"
	KeyDbName     = "DB_NAME"
	KeyDbPort     = "DB_PORT"
	KeyDbUserName = "DB_USERNAME"
	KeyDbPassword = "DB_PASSWORD"
	KeyDbURL      = "DB_CONNECTION_URL"
)

type RepositoryOptions struct {
	Host     string
	DbName   string
	Port     int
	User     string
	Password string
}

func PgRepositoryOptions(connString string) (*pgxpool.Config, error) {
	return pgxpool.ParseConfig(connString)
}

func PgRepositoryOptionsFromEnv() (*pgxpool.Config, error) {
	return PgRepositoryOptions(viper.GetString(KeyDbURL))
}

func NewRepositoryOptions() RepositoryOptions {
	return RepositoryOptions{
		Host:     viper.GetString(KeyDbHostname),
		Port:     viper.GetInt(KeyDbPort),
		DbName:   viper.GetString(KeyDbName),
		User:     viper.GetString(KeyDbUserName),
		Password: viper.GetString(KeyDbPassword),
	}
}

func (o *RepositoryOptions) String() string {
	return fmt.Sprintf("host=%s dbname=%s port=%d user=%s password=%s", o.Host, o.DbName, o.Port, o.User, o.Password)
}

func init() {
	vipers := []string{
		KeyDbHostname,
		KeyDbName,
		KeyDbPort,
		KeyDbUserName,
		KeyDbPassword,
		KeyDbURL,
	}

	for _, key := range vipers {
		registerViperString(key)
	}
}

func registerViperString(key string) {
	// the two parameters are: key name, name of the environment variable
	// in this case they are the same
	if err := viper.BindEnv(key, key); err != nil {
		log.Fatalf("Fail to bind env %s. Error: %s", key, err.Error())
	}
}
