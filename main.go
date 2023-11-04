package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gocql/gocql"
	"github.com/urfave/cli/v2"
)

type Config struct {
	Host     string
	Port     string
	Keyspace string
}

func parseLine(line string) (key, value string) {
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return "", ""
	}
	key = strings.TrimSpace(parts[0])
	value = strings.TrimSpace(parts[1])
	return key, value
}

func readConfig(filename string) (Config, error) {
	var cfg Config
	file, err := os.Open(filename)
	if err != nil {
		return cfg, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, value := parseLine(scanner.Text())
		switch key {
		case "host":
			cfg.Host = value
		case "port":
			cfg.Port = value
		case "keyspace":
			cfg.Keyspace = value
		}
	}

	if err := scanner.Err(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func comma(value int) string {
	sign := ""
	if value < 0 {
		sign = "-"
		value = -value
	}

	str := fmt.Sprintf("%d", value)
	if len(str) <= 3 {
		return sign + str
	}

	var result []string
	for len(str) > 3 {
		result = append([]string{str[len(str)-3:]}, result...)
		str = str[:len(str)-3]
	}
	if len(str) > 0 {
		result = append([]string{str}, result...)
	}

	return sign + strings.Join(result, ",")
}

func InitCassandraConfig() {
	var cassandra Config

	fmt.Println("Make sure to have your test Keyspace ready :)\n")

	fmt.Print("Enter cassandra host: ")
	_, err := fmt.Scanln(&cassandra.Host)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print("Enter cassandra port: ")
	_, err = fmt.Scanln(&cassandra.Port)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print("Enter cassandra keyspace: ")
	_, err = fmt.Scanln(&cassandra.Keyspace)
	if err != nil {
		log.Fatal(err)
	}

	content := fmt.Sprintf("host: %s\nport: %s\nkeyspace: %s\n", cassandra.Host, cassandra.Port, cassandra.Keyspace)

	dirPath := filepath.Join(os.Getenv("HOME"), ".cassandra-loader")
	err = os.MkdirAll(dirPath, 0755)
	if err != nil {
		log.Fatal(err)
	}

	err = os.WriteFile(filepath.Join(dirPath, "config"), []byte(content), 0644)
	if err != nil {
		log.Fatal(err)
	}
}

func InitCassandra(h, k string) *gocql.Session {
	cluster := gocql.NewCluster(h)
	cluster.Keyspace = k
	cluster.Consistency = gocql.One

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal("Error connecting to Cassandra:", err)
	}

	return session
}

func insertData(session *gocql.Session, j int, wg *sync.WaitGroup) {
	defer wg.Done()

	uuid := gocql.MustRandomUUID()

	queries := []string{
		`INSERT INTO cassandradd (id, random) VALUES (?, ?)`,
	}

	for _, query := range queries {
		if err := session.Query(query, uuid, "cassandradd").Exec(); err != nil {
			fmt.Printf("Error inserting data into Cassandra table: %v\n", err)
			log.Fatal(err)
		}
	}
}

func main() {
	app := &cli.App{
		Name:  "cassandra-loader",
		Usage: "Inserts dummy data to a Cassandra database.",
		Action: func(*cli.Context) error {
			fmt.Println("Run cassandra-loader init to initialize cassandra connection configurations.")
			return nil
		},
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:    "concurrency",
				Aliases: []string{"c"},
				Value:   10,
				Usage:   "Maximum number of concurrent requests.",
			},
			&cli.IntFlag{
				Name:    "batch",
				Aliases: []string{"b"},
				Value:   1,
				Usage:   "Number of batches.",
			},
			&cli.IntFlag{
				Name:    "size",
				Aliases: []string{"s"},
				Value:   1,
				Usage:   "Number of writes per batch.",
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "init",
				Usage: "Initialize cassandra connection configurations.",
				Action: func(cCtx *cli.Context) error {
					InitCassandraConfig()
					fmt.Println("\nInitialized cassandra connection configurations.")

					return nil
				},
			},
			{
				Name:  "run",
				Usage: "Insert dummy data to Cassandra.",
				Action: func(cCtx *cli.Context) error {
					configFile := filepath.Join(os.Getenv("HOME"), ".cassandra-loader", "config")

					config, err := readConfig(configFile)
					if err != nil {
						log.Fatal(err)
					}

					session := InitCassandra(config.Host, config.Keyspace)
					defer session.Close()

					if err := session.Query(`
						CREATE TABLE IF NOT EXISTS cassandradd (
							id UUID PRIMARY KEY,
							random TEXT
						)`).Exec(); err != nil {
						log.Fatal("Failed to create table:", err)
					}

					var wg sync.WaitGroup

					fmt.Println("Inserting dummy data...")
					semaphore := make(chan struct{}, cCtx.Int("concurrency"))

					for x := 0; x < cCtx.Int("batch"); x++ {
						for y := 0; y < cCtx.Int("size"); y++ {
							wg.Add(1)
							semaphore <- struct{}{}
							go func(y int) {
								insertData(session, y, &wg)
								<-semaphore
							}(y)
						}
						wg.Wait()
						fmt.Printf("Batch no.%d completed.\n", x+1)
					}
					fmt.Printf("Successfully written %s rows to Cassandra table.\n", comma(cCtx.Int("batch")*cCtx.Int("size")))

					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
