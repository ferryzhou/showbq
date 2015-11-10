package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
)

var (
	dataset = flag.String("dataset", "", "")
	project = flag.String("project", "", "")
	table   = flag.String("table", "", "")
	query   = flag.String("query", "", "")
	flatten = flag.Bool("flatten", false, "")
)

const (
	pemPath    = "g.pem"
	bqEndpoint = "https://www.googleapis.com/auth/bigquery"
)

func getJob() (*bigquery.Job, error) {
	dstTableRef := &bigquery.TableReference{
		ProjectId: *project,
		DatasetId: *dataset,
		TableId:   *table,
	}
	defaultDatasetRef := &bigquery.DatasetReference{
		ProjectId: *project,
		DatasetId: *dataset,
	}
	qConf := &bigquery.JobConfigurationQuery{
		Query:             *query,
		DestinationTable:  dstTableRef,
		DefaultDataset:    defaultDatasetRef,
		AllowLargeResults: true,
		WriteDisposition:  "WRITE_TRUNCATE",
		CreateDisposition: "CREATE_IF_NEEDED",
		FlattenResults:    flatten,
	}
	conf := &bigquery.JobConfiguration{
		Query: qConf,
	}

	return &bigquery.Job{
		Configuration: conf,
	}, nil
}

func getBigqueryService() (*bigquery.Service, error) {
	// generate auth token and create service object
	pemKeyBytes, err := ioutil.ReadFile(pemPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read pem file: %s, error: %v", pemPath, err)
	}

	conf, err := google.JWTConfigFromJSON(pemKeyBytes, bqEndpoint)

	client := conf.Client(oauth2.NoContext)

	return bigquery.New(client)
}

func showQuery() error {

	service, err := getBigqueryService()
	if err != nil {
		return fmt.Errorf("failed to init bigquery: %v", err)
	}

	job, err := getJob()
	if err != nil {
		return fmt.Errorf("failed to create a job: %v", err)
	}

	job, err = service.Jobs.Insert(*project, job).Do()
	if err != nil {
		return fmt.Errorf("failed to do Query: %v", err)
	}

	for i := 0; i < 20; i++ {
		time.Sleep(2 * time.Second)
		j, err := service.Jobs.Get(*project, job.JobReference.JobId).Do()
		if err != nil {
			fmt.Printf("failed to get job: %v\n", err)
			continue
		}
		fmt.Printf("status: %s\n", j.Status.State)
		if j.Status.State != "DONE" {
			fmt.Println("not done yet")
			continue
		}
		fmt.Println("Done")
		if err := j.Status.ErrorResult; err != nil {
			fmt.Fprintf(os.Stderr, "error results: %v\n", err)
		}
		break
	}

	return nil
}

// go run showequerytotable.go --dataset=<dataset> --project=<project> \
//   --table=<destination_table> --query=<query>
func main() {
	flag.Parse()
	if err := showQuery(); err != nil {
		fmt.Println(err)
	}
}
