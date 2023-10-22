package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func main() {
	diff := compareTables()

	if len(diff) == 0 {
		fmt.Println("The tables are identical.")
	} else {
		for _, d := range diff {
			fmt.Println(d)
		}
	}

}

func compareTables() []string {
	connStr1 := "user=paarijat password=loki dbname=test sslmode=disable"
	connStr2 := "user=paarijat password=loki dbname=test2 sslmode=disable"

	db1, err := sql.Open("postgres", connStr1)

	if err != nil {
		log.Fatal((err))

	}
	defer db1.Close()

	db2, err := sql.Open("postgres", connStr2)

	if err != nil {
		log.Fatal((err))

	}
	defer db2.Close()

	rows1, err := db1.Query("SELECT id, data FROM sample_table ORDER BY id")

	if err != nil {
		log.Fatal(err)
	}

	defer rows1.Close()

	rows2, err := db2.Query("SELECT id, data FROM sample_table ORDER BY id")

	if err != nil {
		log.Fatal(err)

	}
	defer rows2.Close()

	var diff []string

	for rows1.Next() && rows2.Next() {
		var id1, id2 int
		var data1, data2 string

		err = rows1.Scan(&id1, &data1)

		if err != nil {
			log.Fatal(err)
		}
		err = rows2.Scan(&id2, &data2)
		if err != nil {
			log.Fatal(err)
		}

		if id1 != id2 || data1 != data2 {
			diff = append(diff, fmt.Sprintf("Difference found: DB1(ID: %d, Data: %s) vs DB2(ID: %d, Data: %s)", id1, data1, id2, data2))
		}

	}

	return diff
}
