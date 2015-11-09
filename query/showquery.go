package main

import (
	"flag"
	"fmt"
	"io/ioutil"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
)

var (
	dataset = flag.String("dataset", "", "")
	project = flag.String("project", "", "")
)

const (
	pemPath    = "g.pem"
	pageSize   = 100
	bqEndpoint = "https://www.googleapis.com/auth/bigquery"
	queryStr   = "select * from publicdata:samples.shakespeare limit 100;"
)

func showQuery() error {
	// generate auth token and create service object
	pemKeyBytes, err := ioutil.ReadFile(pemPath)
	if err != nil {
		return fmt.Errorf("failed to read pem file: %s, error: %v", pemPath, err)
	}

	conf, err := google.JWTConfigFromJSON(pemKeyBytes, bqEndpoint)

	client := conf.Client(oauth2.NoContext)

	service, err := bigquery.New(client)
	if err != nil {
		return fmt.Errorf("failed to init bigquery: %v", err)
	}

	datasetRef := &bigquery.DatasetReference{
		DatasetId: *dataset,
		ProjectId: *project,
	}

	query := &bigquery.QueryRequest{
		DefaultDataset: datasetRef,
		MaxResults:     int64(pageSize),
		Kind:           "json",
		Query:          queryStr,
	}

	qr, err := service.Jobs.Query(*project, query).Do()

	if err != nil {
		return fmt.Errorf("failed to do Query: %v", err)
	}

	l := len(qr.Schema.Fields)
	fields := qr.Schema.Fields
	headers := make([]string, l)
	rows := make([][]interface{}, len(qr.Rows))
	for i, f := range fields {
		headers[i] = f.Name
	}
	for i, tableRow := range qr.Rows {
		row := make([]interface{}, l)
		for j, tableCell := range tableRow.F {
			row[j] = tableCell.V
		}
		rows[i] = row
	}
	fmt.Printf("%v\n", headers)
	fmt.Printf("%v\n", rows)
	return nil
}

// go run showquery.go --dataset=<dataset> --project=<project>
func main() {
	flag.Parse()
	if err := showQuery(); err != nil {
		fmt.Println(err)
	}
}
